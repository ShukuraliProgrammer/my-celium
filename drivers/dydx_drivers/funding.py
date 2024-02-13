from dydx3 import Client
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
from drivers.base import DataDriver
from google.cloud import bigquery

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DYDXFunding(DataDriver):
    def __init__(self):

        self.TABLE_NAME = f"funding"
        self.DATASET_ID = "dydx"

        self.timeframe = "1h"
        self.timeframe_timedelta = timedelta(hours=1)

        self.public_client = Client(
            host="https://api.dydx.exchange",
        )

        self.markets = pd.DataFrame(
            self.public_client.public.get_markets().data["markets"]
        )

        self.max_samples = 100  # as far as I can see this is most you can get from API

        super().__init__(dataset_id=self.DATASET_ID, table_name=self.TABLE_NAME)

    def get_funding_df(
        self, market_str: str, from_time: datetime, to_time: datetime
    ) -> pd.DataFrame:
        """slide window back in time retrieving 100 hourly funding payments at a time"""

        assert market_str in self.markets.columns, f"{market_str} not available on DYDX"

        # funding on dydx is hourly
        res_in_seconds = 3600
        delta_window = timedelta(seconds=res_in_seconds)

        count = 0
        master = pd.DataFrame()

        to_time = to_time.replace(tzinfo=None)

        from_time_original = from_time

        logger.info(f"{'COLLECTING: ' + market_str:-^70}")

        while True:

            try:
                funding = self.public_client.public.get_historical_funding(
                    market=market_str,
                    effective_before_or_at=from_time.isoformat(),
                )
            except Exception as e:
                logger.warning(e)

            funding_data = funding.data["historicalFunding"]
            funding_df = pd.DataFrame(funding_data)

            if funding_df.empty:
                logger.info(f"DONE: no more funding found...")
                break

            funding_df.index = (
                pd.to_datetime(funding_df["effectiveAt"])
                .round("1h")
                .dt.tz_localize(None)
            )

            master = pd.concat([funding_df, master])

            to_time = funding_df.index[0].replace(tzinfo=None)
            from_time = funding_df.index[-1].replace(tzinfo=None)

            logger.info(
                f"{market_str}({count}) {from_time} -> {to_time} ({len(funding_df)})"
            )

            if from_time_original and from_time <= from_time_original:
                logger.debug(f"FINISHED")
                break

            from_time = from_time - delta_window
            count += 1

        if master.empty:
            return master

        if from_time_original is not None:
            master = master[master.index >= from_time_original]

        cols_float = ["rate", "price"]
        cold_categorial = ["market"]

        master[cols_float] = master[cols_float].astype(float)
        master[cold_categorial] = master[cold_categorial].astype("category")

        master.rename(columns={"market": self.unified_market_name}, inplace=True)
        master.drop(columns="effectiveAt", inplace=True)
        master.sort_index(inplace=True)
        master = master.rename_axis(self.unified_timestamp_name)

        return master.reset_index()

    def fetch_data(self, upload: bool, upload_one_at_a_time: bool):
        now = datetime.now()

        tracked_assets = self.get_latest_date()

        master = pd.DataFrame()

        for market in self.markets:

            if market in tracked_assets["ticker"].to_list():
                from_time = tracked_assets[tracked_assets["ticker"] == market][
                    "maxStartTime"
                ].iloc[0]
                logger.info(f"{market} found in DB, starting from {from_time}")
            else:
                from_time = datetime(2010, 1, 1)
                logger.info(f"{market} not found in DB, starting from {from_time}")

            df = self.get_funding_df(
                market_str=market, from_time=from_time, to_time=now
            )

            if df.empty:
                logger.warning(f"couldn't find {market}")
                continue

            if upload and upload_one_at_a_time:
                self.load_from_dataframe(df)
            elif upload:
                master = pd.concat([df, master])

        if upload and not upload_one_at_a_time:
            self.load_from_dataframe(master)

    @property
    def possible_resolutions(self):
        pass

    @property
    def schema(self):
        schema = [
            bigquery.SchemaField(
                name=self.unified_timestamp_name, field_type="DATETIME", mode="REQUIRED"
            ),
            bigquery.SchemaField(
                name=self.unified_market_name, field_type="STRING", mode="REQUIRED"
            ),
            bigquery.SchemaField(name="price", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(name="rate", field_type="FLOAT", mode="REQUIRED"),
        ]
        return schema


if __name__ == "__main__":
    dydx = DYDXFunding()
    dydx.fetch_data(upload=True, upload_one_at_a_time=True)
    pass
