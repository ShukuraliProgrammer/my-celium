from abc import ABC, abstractmethod
from datetime import datetime
from utils.coinAPI_util import CoinAPI
from drivers.base import DataDriver
import ftx
import os
from datetime import datetime, timezone, timedelta


class FTXBase(DataDriver, ABC):
    def __init__(self, table_name):
        self.TABLE_NAME = table_name
        self.DATASET_ID = "FTX"

        self.FTX_client = ftx.FtxClient()
        self.resolution_str = "1h"
        self.resolution = self.possible_resolutions[self.resolution_str]

        self.COINAPI_API_KEY = os.getenv("COINAPI_API_KEY")
        self.coinAPI_client = CoinAPI()

        self.FTX_launch_date = datetime(day=5, month=4, year=2019, tzinfo=timezone.utc)

        self.ticker_to_yfinance = {"LUNA-PERP": "LUNA1-USD", "UST-PERP": "UST-USD"}

        self.all_perps = self.get_all_perp_symbols()

        super().__init__(dataset_id=self.DATASET_ID, table_name=self.TABLE_NAME)

    def get_latest_date_by_ticker(self):
        query = (
            f"SELECT max(startTime) as maxStartTime, {self.unified_market_name}"
            f"FROM {self.TABLE_ID}"
            f"GROUP BY {self.unified_market_name}"
        )

        query_job = self.BQ_client.query(query)

        df = query_job.to_dataframe()

        if df.empty:
            raise ValueError(
                f"Table is either empty or doesn't contain time field named `startTime` and/or 'ticker'."
            )

        return df

    def get_all_perp_symbols(self):
        """This function returns list of dict for all assets including start and end date"""
        return self.coinAPI_client.get_all_assets_for_exchange(
            symbol_name="FTX", symbol_type="PERPETUAL"
        )

    def get_all_listed_perp_names(self):
        """This function is flawed since it only returns assets that are currently listed"""
        return [
            future["name"]
            for future in self.FTX_client.get_futures()
            if future["perpetual"]
        ]

    def get_data_per_ticker(self) -> datetime:

        query = (
            "SELECT "
            f"max({self.unified_timestamp_name}) as maxStartTime, "
            f"min({self.unified_timestamp_name}) as minStartTime, "
            f"count({self.unified_timestamp_name}) as countStartTime, "
            f"{self.unified_market_name} "
            f"FROM {self.TABLE_ID} "
            f"GROUP BY {self.unified_market_name}"
        )

        query_job = self.BQ_client.query(query)

        df = query_job.to_dataframe()

        if df.empty:
            raise ValueError(
                f"Table is either empty or doesn't contain time field named `startTime` or 'ticker'."
            )

        return df

    @property
    def possible_resolutions(self):
        return {
            "15s": 15,
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "2h": 14400,
            "1d": 86400,
        }

    def update_data(self, ticker_names=None, only_update_latest=False):

        # official FTX launch date
        start_time = self.FTX_launch_date

        end_time = datetime.now().replace(tzinfo=timezone.utc)

        master_df = self.fetch_data(
            start_time=start_time,
            end_time=end_time,
            ticker_names=ticker_names,
            only_update_latest=only_update_latest,
        )

        master_df.sort_values("startTime", inplace=True, ascending=True)

        self.load_from_dataframe(master_df)
