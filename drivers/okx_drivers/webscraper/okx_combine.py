import pandas as pd
from pathlib import Path
import logging
from drivers.base import DataDriver
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OKXCombine(DataDriver):
    def __init__(self, data_folder: Path, timeframe: str, upload: bool = False):

        self.exchange_id = "okx"
        self.table_name = "funding_rate"

        self.timeframe = timeframe

        super().__init__(
            dataset_id=self.exchange_id,
            table_name=self.table_name,
            timeframe=self.timeframe,
        )

        assert data_folder.exists()

        self.data_folder = data_folder

        self.upload = upload

    def resample_series(self, df: pd.DataFrame):

        df.reset_index(inplace=True)
        df.set_index("ts", inplace=True)
        df.sort_index(inplace=True)

        resample_px = df["px"].resample(self.timeframe).ohlc()
        resample_sz = df["sz"].resample(self.timeframe).sum()
        resample_sz.name = "volume"

        resample_data = pd.concat(
            [resample_px, resample_sz],
            axis=1,
        )

        resample_data["instId"] = df["instId"][0]

        return resample_data.reset_index()

    def combine_all_aggtrades(self):

        col_names = ["instId", "tradeId", "side", "sz", "px", "ts"]

        master_df = pd.DataFrame()

        for file in self.data_folder.glob("*.zip"):

            if "(" in file.stem:
                logger.warning(
                    f"Skipping {file.name} since it's potentially a duplicate"
                )
                continue

            try:
                df = pd.read_csv(
                    file,
                    names=col_names,
                    header=None,
                )
            except UnicodeDecodeError as e:
                df = pd.read_csv(
                    file,
                    names=col_names,
                    header=None,
                    skiprows=1,
                    encoding="iso-8859-1",
                )
            except Exception as e:
                logger.info(e)

            df["ts"] = pd.to_datetime(df["ts"], unit="ms")

            resample_data = df.groupby("instId", as_index=False).apply(
                self.resample_series
            )

            resample_data.reset_index(drop=True, inplace=True)

            master_df = pd.concat([master_df, resample_data])

            logger.info(f"Processed {file}")

        master_df.rename(
            columns={
                "ts": self.unified_timestamp_name,
                "instId": self.unified_market_name,
            },
            inplace=True,
        )

        master_df.sort_values(self.unified_timestamp_name, ascending=True, inplace=True)

        return master_df.reset_index(drop=True)

    def combine_all_swaprates(self):

        names = [
            "instrument_name",
            "contract_type",
            "funding_rate",
            "real_funding_rate",
            "funding_time",
        ]

        master_df = pd.DataFrame()

        for file in self.data_folder.glob("*.zip"):

            if "(" in file.stem:
                logger.warning(
                    f"Skipping {file.name} since it's potentially a duplicate"
                )
                continue

            try:
                df = pd.read_csv(
                    file,
                    names=names,
                    header=None,
                )
            except UnicodeDecodeError as e:
                df = pd.read_csv(
                    file,
                    names=names,
                    header=None,
                    skiprows=1,
                    encoding="iso-8859-1",
                )
            except Exception as e:
                logger.info(e)

            df.index = pd.to_datetime(df["funding_time"], unit="ms")
            df.drop(columns=["funding_time"], inplace=True)
            master_df = pd.concat([master_df, df])

            logger.info(f"Loaded {file}")

        master_df.rename_axis(self.unified_timestamp_name, inplace=True)
        master_df.rename(
            columns={"instrument_name": self.unified_market_name}, inplace=True
        )
        master_df.sort_index(ascending=True, inplace=True)

        return master_df.reset_index(drop=False)

    @property
    def schema(self):
        schema = [
            bigquery.SchemaField(
                name=self.unified_timestamp_name, field_type="DATETIME", mode="REQUIRED"
            ),
            bigquery.SchemaField(
                name=self.unified_market_name, field_type="STRING", mode="REQUIRED"
            ),
            bigquery.SchemaField(
                name="contract_type", field_type="STRING", mode="REQUIRED"
            ),
            bigquery.SchemaField(
                name="funding_rate", field_type="FLOAT", mode="REQUIRED"
            ),
            bigquery.SchemaField(
                name="real_funding_rate", field_type="FLOAT", mode="REQUIRED"
            ),
        ]
        return schema

    def fetch_data(self):
        try:
            if "swaprates" in str(self.data_folder):
                master_df = okx.combine_all_swaprates()
            elif "aggtrades" in str(self.data_folder):
                master_df = okx.combine_all_aggtrades()
                master_df = self.validate_df(
                    master_df, savefig_path=Path("./data_check.jpg"), unique_col="close"
                )
            else:
                raise NotImplementedError
        except Exception as e:
            logger.error(e)

        if self.upload:
            self.load_from_dataframe(master_df)

    def possible_resolutions(self):
        return ["8h"]


if __name__ == "__main__":

    data_folder = Path("2023-01-20_18-14-34_aggtrades_monthly")

    okx = OKXCombine(data_folder=data_folder, timeframe="1H", upload=True)
    okx.fetch_data()

    pass
