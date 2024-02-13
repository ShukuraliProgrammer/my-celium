from google.cloud import bigquery
from drivers.ftx_drivers.ftx_base import FTXBase
import pandas as pd
import logging
import time, requests, os
from math import ceil
from dotenv import load_dotenv
import utils.yfinance_util as yfinance
from datetime import datetime, timezone, timedelta
from typing import Union

pd.options.mode.chained_assignment = None

load_dotenv("./../..")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
This query can be used to remove duplicates in case the command was accidentally executed multiple times:
CREATE OR REPLACE TABLE `warehouse-366918.FTX.futures`
AS
SELECT DISTINCT *
FROM `warehouse-366918.FTX.futures`
"""


class FTXFutures(FTXBase):
    def __init__(self):
        self.TABLE_NAME = "futures"
        super().__init__(table_name=self.TABLE_NAME)

    @property
    def schema(self):
        schema = [
            bigquery.SchemaField(
                name=self.unified_timestamp_name, field_type="DATETIME", mode="REQUIRED"
            ),
            bigquery.SchemaField(name=self.unified_market_name, field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="close", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(name="high", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(name="low", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(name="open", field_type="FLOAT", mode="REQUIRED"),
            bigquery.SchemaField(name="volume", field_type="FLOAT", mode="NULLABLE"),
        ]
        return schema

    def retry_fetch_ohlc(
            self, market_name, start_time, end_time, max_attempts, limit=None
    ):

        for i in range(max_attempts):

            try:
                ohlc = self.FTX_client.get_historical_data(
                    market_name=market_name,
                    start_time=start_time,
                    end_time=end_time,
                    resolution=self.resolution,
                    limit=limit,
                )
                return ohlc
            except Exception as e:
                sleep_time = 1 * (1 + i)
                logger.error(
                    f"Attempt {i + 1}/{max_attempts} sleep({sleep_time}) ERROR: {e}"
                )
                time.sleep(sleep_time)

        return None

    def get_prices(
            self, market_name: str, start_time: datetime, end_time: datetime
    ) -> Union[pd.DataFrame,None]:
        """
        We fetch OHLCV data by paginating forward in time, from the oldest date (start_time) to the newest (end_time)
        until no more data is returned by the API
        :param market_name: market name
        :param start_time: the oldest (minimum) time of our range
        :param end_time: the newest (maximum) time of our range
        """

        max_attempts = 3

        max_candles = 1501
        window_size_period = (end_time - start_time).total_seconds() / self.resolution
        iterations = ceil(window_size_period / max_candles)

        window_start = start_time
        window_timedelta = timedelta(seconds=self.resolution * max_candles)
        window_end = window_start + window_timedelta

        ohlc_df = pd.DataFrame()

        for i in range(1, iterations + 1):

            logger.info(f"Query: {window_start} -> {window_end}")

            ohlc = self.retry_fetch_ohlc(
                market_name=market_name,
                start_time=window_start.timestamp(),
                end_time=window_end.timestamp(),
                max_attempts=max_attempts,
                limit=None,
            )

            # we couldn't get any data after a number of attemps so we quit
            # most of the time this is (for example): ERROR: No such market: BTMX-PERP
            if ohlc is None:
                logger.error(
                    f"Failed to fetch {market_name} after {max_attempts} attempts, skipping"
                )
                return None

            # we got an empty response from FTX which probably means that wasn't being traded at the time
            # so we shift our window back in time using a predefined number of candles
            elif len(ohlc) == 0:

                window_start = window_end
                window_end = window_start + window_timedelta

            # if there was some data, we shift our end_date back to the timestamp of our first sample in the response
            else:

                df = pd.DataFrame(ohlc)

                df["startTime"] = pd.to_datetime(df["time"], unit="ms", utc=True)

                ohlc_df = pd.concat([ohlc_df, df])

                window_start = df.loc[df.index[-1], "startTime"]
                window_end = window_start + window_timedelta

                logger.info(
                    f"Resp: {df['startTime'].iloc[0]} -> {df['startTime'].iloc[-1]} candles: {len(ohlc)}"
                )

            logger.info("")

        if ohlc_df is None or ohlc_df.empty:
            return None

        ohlc_df.drop(["time"], axis=1, inplace=True)
        ohlc_df.rename(columns={'timestamp':self.unified_timestamp_name})

        return ohlc_df

    def fetch_data(self, start_time, end_time, ticker_names=None, only_update_latest=False) -> pd.DataFrame:
        """

        :param ticker_names:
        :param start_time:
        :param end_time:
        :return:
        """

        symbols = self.get_all_perp_symbols()

        if ticker_names:
            symbols = symbols[symbols['symbol_id_exchange'].isin(ticker_names)]

        if only_update_latest:
            start_times = self.get_latest_date_by_ticker()
        else:
            start_times = None

        master_df = pd.DataFrame()

        for idx, symbol in symbols.iterrows():

            symbol_id_exchange = symbol.get("symbol_id_exchange")

            if only_update_latest:

                start_time = start_times[start_times["ticker"] == symbol_id_exchange]

                # if it's the first time we see the asset then we set the start date as the FTX launch date
                # we do this to save time and avoid submitting duplicates to BigQuery
                if start_time.empty:
                    start_time = self.FTX_launch_date
                    logger.info(
                        f"{symbol_id_exchange} is new in the database, starting from beginning {start_time}"
                    )
                else:
                    start_time = start_time.iloc[0, 0].replace(tzinfo=timezone.utc)


            header_string = (
                f" Fetching: [{symbol_id_exchange}] {int((idx / len(symbols)) * 100)}% "
            )
            logger.info(f"{header_string:-^60}")

            df = self.get_prices(
                symbol_id_exchange, start_time=start_time, end_time=end_time
            )

            if df is None or df.empty:
                logger.error(
                    f"DataFrame: {symbol_id_exchange} failed and trying with Yahoo"
                )

                yf_ticker = self.ticker_to_yfinance.get(symbol_id_exchange)

                # we try to fetch from yFinance
                if not yf_ticker:
                    logger.error(
                        f"YahooFinance: {symbol_id_exchange} not found in ticker_to_yfinance mappings"
                    )
                    continue

                perps = self.all_perps
                perp_data = perps[perps["symbol_id_exchange"] == symbol_id_exchange]
                start_time_coinAPI = pd.to_datetime(perp_data["data_trade_start"]).iloc[0]
                end_time_coinAPI = pd.to_datetime(perp_data["data_trade_end"]).iloc[0]

                df = yfinance.get_price(yf_ticker,
                                        start_time=start_time_coinAPI,
                                        end_time=end_time_coinAPI,
                                        interval=self.resolution_str,
                                        round_to_hour=True
                                        )

                if df is None or df.empty:
                    logger.error(
                        f"YahooFinance: {df} failed so we skip"
                    )
                    continue

            df["ticker"] = symbol_id_exchange

            master_df = pd.concat([df, master_df])

            logger.info(
                f" #{idx + 1}/{len(symbols)}: {symbol_id_exchange} - {df.shape} "
                f"{df['startTime'].iloc[0]} -> {df['startTime'].iloc[-1]}\n\n"
            )

            # if idx >= 2:
            #     break

        return master_df.reset_index(drop=True)


if __name__ == "__main__":
    ftx_driver = FTXFutures()

    ftx_driver.create_dataset()
    ftx_driver.create_table()
    ftx_driver.update_data(only_update_latest=True)