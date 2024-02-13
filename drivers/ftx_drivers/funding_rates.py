from google.cloud import bigquery
from drivers.ftx_drivers.ftx_base import FTXBase
from abc import ABC, abstractmethod
import ftx
import pandas as pd
import logging
from datetime import datetime, timezone, timedelta
import time, requests, os
from math import ceil
from dotenv import load_dotenv

load_dotenv("./../..")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
This query can be used to remove duplicates in case the command was accidentally executed multiple times:
CREATE OR REPLACE TABLE `warehouse-366918.FTX.funding_rates`
AS
SELECT DISTINCT *
FROM `warehouse-366918.FTX.funding_rates`
"""


class FTXFundingRates(FTXBase, ABC):
    def __init__(self):
        self.TABLE_NAME = "funding_rates"
        super().__init__(name="FTX", table_name=self.TABLE_NAME)

    @property
    def schema(self):
        schema = [
            bigquery.SchemaField(
                name="startTime", field_type="DATETIME", mode="REQUIRED"
            ),
            bigquery.SchemaField(name="ticker", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="rate", field_type="FLOAT", mode="REQUIRED"),
        ]
        return schema

    def retry_fetch_funding_rate(
            self, future, start_time, end_time, max_attempts, limit=None
    ):

        for i in range(max_attempts):

            try:
                rate = self.FTX_client.get_funding_rates(
                    future=future,
                    start_time=start_time,
                    end_time=end_time,
                )
                return rate
            except Exception as e:
                sleep_time = 2 * (1 + i)
                logger.error(
                    f"Attempt {i + 1}/{max_attempts} sleep({sleep_time}) ERROR: {e}"
                )
                time.sleep(sleep_time)

        return None

    def get_funding_rate(
            self, future: str, start_time: datetime, end_time: datetime
    ) -> pd.DataFrame:
        """
        We fetch funding rate data by paginating forward in time, from the oldest date (start_time) to the
        newest (end_time) until no more data is returned by the API
        :param future: future name (i.e. BTC-PERP)
        :param start_time: the oldest (minimum) time of our range
        :param end_time: the newest (maximum) time of our range
        """

        max_attempts = 3

        max_candles = 500
        window_size_period = (end_time - start_time).total_seconds() / self.resolution
        iterations = ceil(window_size_period / max_candles)

        window_start = start_time
        window_timedelta = timedelta(seconds=self.resolution * max_candles)
        window_end = window_start + window_timedelta

        funding_rate_df = pd.DataFrame()

        for i in range(1, iterations + 1):

            logger.info(f"Query: {window_start} -> {window_end}")

            rate = self.retry_fetch_funding_rate(
                future=future,
                start_time=window_start.timestamp(),
                end_time=window_end.timestamp(),
                max_attempts=max_attempts,
            )

            # we couldn't get any data after a number of attempts, so we quit
            # most of the time this is (for example): ERROR: No such market: BTMX-PERP
            if rate is None:
                logger.error(
                    f"Failed to fetch {future} after {max_attempts} attempts, skipping"
                )

                ticker = self.ticker_to_yfinance.get(future)

                # we try to fetch from yFinance
                if not ticker:
                    return None

            # we got an empty response from FTX which probably means that wasn't being traded at the time,
            # so we shift our window back in time using a predefined number of candles
            elif len(rate) == 0:

                window_start = window_end
                window_end = window_start + window_timedelta

            # if there was some data, we shift our end_date back to the timestamp of our first sample in the response
            else:

                df = pd.DataFrame(rate)

                df["startTime"] = pd.to_datetime(df["time"])

                funding_rate_df = pd.concat([funding_rate_df, df])

                window_start = df.loc[df.index[-1], "startTime"]
                window_end = window_start + window_timedelta

                logger.info(
                    f"Resp: {df['startTime'].iloc[0]} -> {df['startTime'].iloc[-1]} funding rates: {len(rate)}"
                )

            logger.info("")

        return funding_rate_df

    def fetch_all_data(self, start_time, end_time) -> pd.DataFrame:
        """

        :param start_time:
        :param end_time:
        :return:
        """
        symbols = self.get_all_perp_symbols()

        master_df = pd.DataFrame()

        for idx, symbol in symbols.iterrows():

            symbol_id_exchange = symbol.get("symbol_id_exchange")

            header_string = (
                f" Fetching: [{symbol_id_exchange}] {int((idx / len(symbols)) * 100)}% "
            )
            logger.info(f"{header_string:-^60}")

            df = self.get_funding_rate(
                symbol_id_exchange, start_time=start_time, end_time=end_time
            )

            if df is None:
                logger.error(
                    f"DataFrame for {symbol_id_exchange} is None (failed) and skipped"
                )
                continue

            elif df.empty:
                logger.error(f"DataFrame for {symbol_id_exchange} is empty and skipped")
                continue

            df["ticker"] = symbol_id_exchange

            df.drop(["time"], axis=1, inplace=True)
            df.drop(["future"], axis=1, inplace=True)

            master_df = pd.concat([df, master_df])

            logger.info(
                f" #{idx + 1}/{len(symbols)}: {symbol_id_exchange} - {df.shape} "
                f"{df['startTime'].iloc[0]} -> {df['startTime'].iloc[-1]}\n\n"
            )

            # if idx >= 2:
            #     # print(master_df)
            #     break

        return master_df

    def fetch_latest_data(self) -> pd.DataFrame:
        """

        :return:
        """
        # we get the last timestamp for each entry in the BigQuery table
        start_times = self.get_latest_date_by_ticker()

        end_time = datetime.now().replace(tzinfo=timezone.utc)

        symbols = self.get_all_perp_symbols

        master_df = pd.DataFrame()

        for idx, symbol in symbols.iterrows():

            symbol_id_exchange = symbol.get("symbol_id_exchange")

            start_time = start_times[start_times["ticker"] == symbol_id_exchange]

            # if it's the first time we see the asset then we set the start date as the FTX launch date
            # we do this to save time and avoid submitting duplicates to BigQuery
            if not start_time:
                start_time = datetime(day=5, month=4, year=2019, tzinfo=timezone.utc)

            header_string = (
                f" Fetching: [{symbol_id_exchange}] {int((idx / len(symbols)) * 100)}% "
            )

            logger.info(f"{header_string:-^60}")

            df = self.get_funding_rate(
                symbol_id_exchange, start_time=start_time, end_time=end_time
            )

            if df is None:
                logger.error(
                    f"DataFrame for {symbol_id_exchange} is None (failed) and skipped"
                )
                continue

            elif df.empty:
                logger.error(f"DataFrame for {symbol_id_exchange} is empty and skipped")
                continue

            df["ticker"] = symbol_id_exchange

            df.drop(["time"], axis=1, inplace=True)

            master_df = pd.concat([df, master_df])

            logger.info(
                f" #{idx + 1}/{len(symbols)}: {symbol_id_exchange} - {df.shape} "
                f"{df['startTime'].iloc[0]} -> {df['startTime'].iloc[-1]}\n\n"
            )

            # if idx >= 2:
            #     break

        return master_df


if __name__ == "__main__":
    ftx_driver = FTXFundingRates()

    ftx_driver.create_dataset()
    ftx_driver.create_table()
    ftx_driver.upload_full_historical_data()


    # logger.info(ftx_driver.get_all_futures())
