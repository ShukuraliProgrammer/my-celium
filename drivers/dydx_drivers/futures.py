from dydx3 import Client
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
from drivers.base import DataDriver
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DYDXFutures(DataDriver):
    def __init__(self, timeframe):

        self.timeframe = timeframe
        self.TABLE_NAME = f"OHLCV_{self.timeframe}"
        self.DATASET_ID = "dydx"

        self.public_client = Client(
            host="https://api.dydx.exchange",
        )

        self.markets = pd.DataFrame(
            self.public_client.public.get_markets().data["markets"]
        )

        self.max_samples = 100  # as far as I can see this is most you can get from API

        assert (
            self.timeframe in self.possible_resolutions.keys()
        ), f"{timeframe} timeframe not supported"

        super().__init__(dataset_id=self.DATASET_ID, table_name=self.TABLE_NAME)

    @property
    def possible_resolutions(self):
        return {
            "1DAY": 86400,
            "4HOURS": 14400,
            "1HOUR": 3600,
            "30MINS": 1800,
            "15MINS": 900,
            "5MINS": 300,
            "1MIN": 60,
        }

    @property
    def resolution_to_offset_alias(self):
        return {
            "1DAY": "D",
            "4HOURS": "H",
            "1HOUR": "H",
            "30MINS": "T",
            "15MINS": "T",
            "5MINS": "T",
            "1MIN": "T",
        }

    def get_candles_df(
        self, market_str: str, from_time: datetime, to_time: datetime
    ) -> pd.DataFrame:
        """slide window back in time retrieving 100 hourly funding payments at a time"""

        assert market_str in self.markets.columns, f"{market_str} not available on DYDX"

        market = self.markets[market_str]

        res_in_seconds = self.possible_resolutions.get(self.timeframe)
        assert res_in_seconds, "incorrect resolution"

        count = 0
        master = pd.DataFrame()

        from_time_original = from_time

        while True:

            if (
                to_time - from_time
            ).total_seconds() / res_in_seconds > self.max_samples:
                from_time = to_time - timedelta(hours=self.max_samples)

            if from_time_original and from_time_original > from_time:
                from_time = from_time_original

            try:
                candles = self.public_client.public.get_candles(
                    market=market_str,
                    resolution=self.timeframe,
                    from_iso=from_time.isoformat(),
                    to_iso=to_time.isoformat(),
                    limit=self.max_samples,
                )
            except Exception as e:
                logger.warning(e)

            candles_data = candles.data["candles"]
            candles_df = pd.DataFrame(candles_data)

            if candles_df.empty:
                logger.info(f"DONE: no candles found...")
                break

            candles_df.set_index("startedAt", inplace=True)

            offset_alias = self.resolution_to_offset_alias.get(self.timeframe)
            candles_df.index = pd.to_datetime(candles_df.index).round(offset_alias)
            candles_df = candles_df.loc[~candles_df.index.duplicated(keep="first")]

            count += 1
            master = pd.concat([candles_df, master])

            end = candles_df.index[0].replace(tzinfo=None)
            start = candles_df.index[-1].replace(tzinfo=None)
            logger.info(f"{market_str}({count}) {start} -> {end} ({len(candles_df)})")

            to_time = start
            from_time = to_time - timedelta(hours=self.max_samples)

            if from_time_original and start <= from_time_original:
                logger.info(f"FINISHED")
                break

        master["status"] = market["status"]

        cols_float = [
            "low",
            "high",
            "open",
            "close",
            "baseTokenVolume",
            "usdVolume",
            "startingOpenInterest",
        ]
        cols_int = ["trades"]
        cold_categorial = ["status", "market"]

        master[cols_float] = master[cols_float].astype(float)
        master[cols_int] = master[cols_int].astype(int)
        master[cold_categorial] = master[cold_categorial].astype("category")
        master.drop(columns=["updatedAt", "resolution"], inplace=True)
        master.rename(columns={"market": self.unified_market_name}, inplace=True)
        master = master.rename_axis(self.unified_timestamp_name)
        master.sort_index(inplace=True)

        return master.reset_index()

    def fetch_data(self, upload: bool = False, upload_one_at_a_time: bool = False):
        now = datetime.now()
        tracked_assets = self.get_latest_date()

        master = pd.DataFrame()
        for market in self.markets:

            logger.info(f"{'COLLECTING: ' + market:-^70}")

            if market in tracked_assets["ticker"].to_list():
                from_time = tracked_assets[tracked_assets["ticker"] == market][
                    "maxStartTime"
                ].iloc[0]
                logger.info(f"{market} found in DB, starting from {from_time}")
            else:
                from_time = datetime(2010, 1, 1)
                logger.info(f"{market} not found in DB, starting from {from_time}")

            df = self.get_candles_df(
                market_str=market, from_time=from_time, to_time=now
            )

            if upload and upload_one_at_a_time:
                self.load_from_dataframe(df)
            elif upload:
                master = pd.concat([df, master])

        if upload and not upload_one_at_a_time:
            self.load_from_dataframe(master)

    @property
    def schema(self):
        schema = [
            bigquery.SchemaField(
                name=self.unified_timestamp_name, field_type="DATETIME", mode="REQUIRED"
            ),
            bigquery.SchemaField(
                name=self.unified_market_name, field_type="STRING", mode="REQUIRED"
            ),
            bigquery.SchemaField(name="close", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(name="high", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(name="low", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(name="open", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(
                name="baseTokenVolume", field_type="FLOAT", mode="NULLABLE"
            ),
            bigquery.SchemaField(name="usdVolume", field_type="FLOAT", mode="NULLABLE"),
            bigquery.SchemaField(
                name="startingOpenInterest", field_type="FLOAT", mode="NULLABLE"
            ),
            bigquery.SchemaField(name="trades", field_type="INTEGER", mode="NULLABLE"),
            bigquery.SchemaField(name="status", field_type="STRING", mode="NULLABLE"),
        ]
        return schema


if __name__ == "__main__":
    dydx_futures = DYDXFutures(timeframe="1HOUR")
    dydx_futures.fetch_data(upload=True, upload_one_at_a_time=True)
    pass
