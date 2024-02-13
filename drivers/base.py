from abc import ABC, abstractmethod
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
from google.cloud.exceptions import NotFound, Conflict
import logging
import pandas as pd
from utils.coinAPI_util import CoinAPI
import numpy as np
import seaborn as sns


logger = logging.getLogger(__name__)
load_dotenv()

"""
For ease of use, let's name the main datetime field as startTime
"""


class DataDriver(ABC):
    def __init__(self, dataset_id: str, table_name: str, timeframe: str):
        self.PROJECT_ID = os.environ.get("project_id")

        self.DATASET_ID = dataset_id
        self.TABLE_NAME = table_name
        self.TABLE_ID = f"{self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_NAME}"

        self.unified_timestamp_name = "startTime"
        self.unified_market_name = "ticker"

        self.timeframe = timeframe

        # we favor small but frequent uploads
        self.max_upload_size_mb = 0.2

        info = {
            "type": os.getenv("type"),
            "project_id": os.getenv("project_id"),
            "private_key_id": os.getenv("private_key_id"),
            "private_key": os.getenv("private_key"),
            "client_email": os.getenv("client_email"),
            "client_id": os.getenv("client_id"),
            "auth_uri": os.getenv("auth_uri"),
            "token_uri": os.getenv("token_uri"),
            "auth_provider_x509_cert_url": os.getenv("auth_provider_x509_cert_url"),
            "client_x509_cert_url": os.getenv("client_x509_cert_url"),
        }

        bigquery_credentials = service_account.Credentials.from_service_account_info(
            info
        )

        self.BQ_client = bigquery.Client(
            project=self.PROJECT_ID, credentials=bigquery_credentials
        )

        self.CoinApi = CoinAPI()

        self.create_dataset()
        self.create_table()

    @property
    @abstractmethod
    def schema(self):
        pass

    @property
    @abstractmethod
    def possible_resolutions(self):
        pass

    @property
    @abstractmethod
    def period_to_pandas(self) -> dict:
        """frequency needs to be mapped to these offset aliases: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases"""
        pass

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """
        for each item if it's already in the DB then fetch from the latest date, other from begining of time
        :return:
        """
        pass

    def check_dataset_exists(self):
        try:
            self.BQ_client.get_dataset(self.DATASET_ID)  # Make an API request.
            print(f"Dataset {self.DATASET_ID} already exists")
            return True
        except NotFound:
            print(f"Dataset {self.DATASET_ID} is not found")
            return False

    def create_table(self):

        table = bigquery.Table(self.TABLE_ID, schema=self.schema)

        # if hasattr(self.__class__, "time_partitioning"):
        #     table = self.time_partitioning(table)

        try:
            table = self.BQ_client.create_table(table)  # Make an API request.
            logger.info(
                f"Created table {table.project}.{table.dataset_id}.{table.table_id}"
            )
        except Conflict as conf:
            logger.info(f"{conf}")
        except Exception as e:
            logger.fatal(e)
            raise Exception("Something bad & unexpected happened")

    def create_dataset(self):

        try:
            table = self.BQ_client.create_dataset(
                self.DATASET_ID
            )  # Make an API request.
            logger.info(f"Created table {table.project}.{table.dataset_id}")
        except Conflict as conf:
            logger.info(f"{conf}")
        except Exception as e:
            logger.fatal(e)
            raise Exception("Something bad & unexpected happened")

    def get_latest_date(self) -> datetime:

        query_job = self.BQ_client.query(
            f"""SELECT
                max({self.unified_timestamp_name}) as maxStartTime,
                min({self.unified_timestamp_name}) as minStartTime,
                count({self.unified_timestamp_name}) as countStartTime,
                {self.unified_market_name}
                FROM {self.TABLE_ID}
                GROUP BY {self.unified_market_name}
                ORDER BY maxStartTime DESC"""
        )

        df = query_job.to_dataframe()

        if df.empty:
            logger.warning(
                f"Table is either empty or doesn't contain time field named `startTime`."
            )

        return df

    def validate_df(self, df: pd.DataFrame, unique_col: str, savefig_path: str = None):

        if not self.unified_timestamp_name in df.columns:
            raise Exception(
                f"{self.unified_timestamp_name} needs to be a column in the DataFrame"
            )

        if not self.unified_market_name in df.columns:
            raise Exception(
                f"{self.unified_market_name} needs to be a column in the DataFrame"
            )

        clean_df = pd.DataFrame()
        stats = pd.DataFrame(columns=["duplicated", "missing"])

        for ticker in df[self.unified_market_name].unique():

            logger.info(f"Processing: {ticker}")

            ticker_df = df[df[self.unified_market_name] == ticker]
            ticker_df = ticker_df.set_index("startTime")
            ticker_df = ticker_df.sort_index()
            index_time = pd.Series(ticker_df.index)

            # check seconds for all periods
            if not index_time.apply(lambda x: x.second == 0).all():
                logger.warning(f"Not all seconds are zero")
                ticker_df.index = ticker_df.index.floor("min")

            # check seconds for all hourly and above
            if self.timeframe.upper() in ["1H", "8H"]:
                if not index_time.apply(lambda x: x.minute == 0).all():
                    logger.warning(f"Not all minutes are zero")
                    ticker_df.index = ticker_df.index.floor("h")

            # check for duplicates
            duplicated = ticker_df.index.duplicated()
            if duplicated.any():
                logger.warning(
                    f"Duplicated {len(duplicated)} samples ({(duplicated.sum() / len(ticker_df) - 1)*100:.2f}%)"
                )
                ticker_df = ticker_df[~duplicated]

            # check for gaps in timeseries
            t_start = ticker_df.index[0]
            t_end = ticker_df.index[-1]

            all = pd.Series(
                data=pd.date_range(start=t_start, end=t_end, freq=self.timeframe)
            )
            mask = all.isin(ticker_df.index)
            missing_timestamp = all[~mask]

            if len(missing_timestamp) != 0:
                logger.warning(
                    f"Missing {len(missing_timestamp)} samples ({(len(missing_timestamp) / len(ticker_df) - 1)*100:.2f}%)"
                )
                ticker_df = ticker_df.resample(self.timeframe).asfreq()
                ticker_df[self.unified_market_name] = ticker_df[
                    self.unified_market_name
                ].ffill()

            stats.loc[ticker, "missing"] = len(missing_timestamp)
            stats.loc[ticker, "duplicated"] = duplicated.sum()

            clean_df = pd.concat([clean_df, ticker_df])

            logger.info("\n")

        offset_alias = self.period_to_pandas[self.timeframe]

        # Ensure that all times are as we expect
        unique_times = np.unique(clean_df.index.time)
        expected_times = pd.date_range("00:00", "23:59", freq=offset_alias).time
        unexpected_times = unique_times[~np.isin(unique_times, expected_times)]

        if len(unexpected_times) > 0:
            logger.info(f"Got UNEXPECTED time(s): {unexpected_times}")
            clean_df = clean_df[~np.isin(clean_df.index.time, unexpected_times)]

        logger.info(f"Unique times: {unique_times}")
        logger.info(f"Total missing rows: {(len(clean_df) / len(df) - 1) * 100:.2f}%")

        if savefig_path:
            clean_df["is_valid"] = np.where(clean_df[unique_col].isna(), 0, 1)
            ax = sns.relplot(
                data=clean_df.reset_index(),
                x="startTime",
                y="is_valid",
                row="ticker",
                kind="line",
                height=0.8,
                aspect=8,
                facet_kws=dict(sharey=False),
            )
            for i, row in enumerate(ax.figure.axes):
                row.set_ylabel(None)
                row.set_title(ax.row_names[i])
            try:
                ax.figure.savefig(savefig_path)
            except Exception as e:
                logger.info(e)
            clean_df.drop(columns=["is_valid"], inplace=True)

        return clean_df.sort_index().reset_index()

    def load_from_dataframe(self, df: pd.DataFrame, unique_col: str):

        df = self.validate_df(df, unique_col=unique_col)

        table = self.BQ_client.get_table(self.TABLE_ID)

        try:
            resp = self.BQ_client.load_table_from_dataframe(df, table)
        except Conflict as conf:
            logger.info(f"{conf}")
        except Exception as e:
            raise e
        else:
            logger.info(
                f"Updated table: {table.project}.{table.dataset_id}.{table.table_id}"
            )
            if resp.error_result is not None:
                logger.error(f"Found errors uploading job: {resp.error_result}")
