from google.cloud import bigquery
from datetime import datetime, timedelta
import sys
import logging
from drivers.ccxt_driver.ccxt_base import CCXTBase
import pandas as pd
from utils.bigquery_util import get_time_partitionning_type

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CCXTDriverOHLCV(CCXTBase):
    def __init__(
            self,
            ccxt_exchange_id,
            coinapi_exchange_id,
            coinapi_symbol_type,
            timeframe,
            instrument_type,
            upload_data: bool = False,
    ):

        table_name = "OHLCV"

        self.instrument_type = instrument_type

        super().__init__(
            table_name=table_name,
            ccxt_exchange_id=ccxt_exchange_id,
            coinapi_exchange_id=coinapi_exchange_id,
            coinapi_symbol_type=coinapi_symbol_type,
            instrument_type=instrument_type,
            timeframe=timeframe,
            upload_data=upload_data,
        )

    def get_all_ohlcv_binance(
            self, market: str, from_time_dt: datetime, to_time_dt
    ) -> pd.DataFrame:
        """this function goes back in time and fetches to fetch historical values

        :param market: the ticker/market name
        :param from_time_dt: the earliest point in time to fetch
        :param to_time_dt: the latest point in time to fetch
        :return: a pandas Dataframe
        """
        to_time_dt = pd.to_datetime(to_time_dt).floor(self.timeframe)
        fetch_since_temp = max(from_time_dt, to_time_dt - self.timedelta_window)

        if self.exchange_id == "okx":
            fetch_since_temp = to_time_dt

        all_ohlcv = []

        while True:

            try:
                if self.exchange_id == "okx":
                    ohlcv = self._retry_fetch_function(
                        callable_function=self.exchange.fetch_ohlcv,
                        symbol=market,
                        timeframe=self.timeframe,
                        since=int(fetch_since_temp.timestamp() * 1000),
                    )
                elif self.exchange_id == "binance":
                    ohlcv = self._retry_fetch_function(
                        callable_function=self.exchange.fetch_ohlcv,
                        symbol=market,
                        timeframe=self.timeframe,
                        since=int(fetch_since_temp.timestamp() * 1000),
                        limit=self.limit,
                    )
            except Exception as e:
                logger.info(e)
                break

            if len(ohlcv) == 0:
                break

            earliest_datetime = datetime.utcfromtimestamp(ohlcv[0][0] / 1000)
            latest_datetime = datetime.utcfromtimestamp(ohlcv[-1][0] / 1000)
            all_ohlcv = ohlcv + all_ohlcv

            logger.info(
                f"{market} {earliest_datetime} ({earliest_datetime.timestamp()}) -> "
                f"{latest_datetime} ({latest_datetime.timestamp()}) {len(ohlcv)}/{len(all_ohlcv)} : "
                f"fetch_since_temp {fetch_since_temp} ({fetch_since_temp.timestamp()})"
            )

            if len(ohlcv) < self.limit:
                logger.info(f"COMPLETE: len(ohlcv){len(ohlcv)} < limit{self.limit}")
                # break

            if earliest_datetime < from_time_dt:
                logger.info(
                    f"COMPLETE: {market} earliest_datetime {earliest_datetime} is smaller than from_time_dt {from_time_dt}"
                )
                break

            if fetch_since_temp + self.timeframe_timedelta < earliest_datetime:
                break

            # we add 5x timeframe just to make sure we don't get any gaps, since duplicates are easy to remove
            fetch_since_temp = (
                    earliest_datetime
                    - self.timedelta_window
                    + (self.timeframe_timedelta * 5)
            )

        df = pd.DataFrame(
            all_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.rename(columns={"timestamp": self.unified_timestamp_name}, inplace=True)
        df.drop_duplicates("startTime", inplace=True)
        df.sort_values("startTime", inplace=True)
        df["ticker"] = market

        df = df[df["startTime"] >= from_time_dt]

        return df

    def get_all_ohlcv_okx(
            self, market: str, from_time_dt: datetime, to_time_dt
    ) -> bool:
        """this function goes back in time and fetches to fetch historical values

        :param market: the ticker/market name
        :param from_time_dt: the earliest point in time to fetch
        :param to_time_dt: the latest point in time to fetch
        :return: a pandas Dataframe
        """

        fetch_since_temp = to_time_dt - self.timedelta_window

        all_ohlcv = []

        if self.instrument_type == "future":
            ccxt_function = self.exchange.public_get_market_history_candles
        elif self.instrument_type == "index":
            ccxt_function = self.exchange.public_get_market_history_index_candles
        elif self.instrument_type == "mark":
            ccxt_function = self.exchange.public_get_market_history_mark_price_candles
        else:
            raise NotImplementedError(f"{self.instrument_type} not implemented for OKX")

        while True:

            try:
                resp = self._retry_fetch_function(
                    callable_function=ccxt_function,
                    instId=market,
                    bar=self.timeframe,
                    after=int(fetch_since_temp.timestamp() * 1000),
                    limit=self.limit,
                )
            except Exception as e:
                logger.info(e)
                break

            ohlcv = resp.get("data")

            if len(ohlcv) == 0:
                break

            earliest_datetime = datetime.utcfromtimestamp(int(ohlcv[-1][0]) / 1000)
            latest_datetime = datetime.utcfromtimestamp(int(ohlcv[0][0]) / 1000)
            all_ohlcv = ohlcv + all_ohlcv

            data_size_mb = sys.getsizeof(all_ohlcv) / 1e6

            logger.info(
                f"{market} {earliest_datetime} ({earliest_datetime.timestamp()}) -> "
                f"{latest_datetime} ({latest_datetime.timestamp()}) {len(ohlcv)}/{len(all_ohlcv)} : "
                f"fetch_since_temp {fetch_since_temp} ({fetch_since_temp.timestamp()}) "
                f"({data_size_mb:,.4f}MB)"
            )

            if data_size_mb > self.max_upload_size_mb:
                try:
                    self.process_and_upload_ohlcv(all_ohlcv, market, from_time_dt)
                except Exception as e:
                    logger.error(f"Couldn't upload {market}: {e}")
                else:
                    all_ohlcv = []

            if earliest_datetime < from_time_dt:
                logger.info(
                    f"COMPLETE: {market} earliest_datetime {earliest_datetime} is smaller than from_time_dt {from_time_dt}"
                )
                break

            fetch_since_temp = earliest_datetime

        if len(all_ohlcv) == 0:
            return False

        self.process_and_upload_ohlcv(all_ohlcv, market, from_time_dt)

        return True

    def process_and_upload_ohlcv(
            self, all_ohlcv: dict, symbol: str, from_time_dt: datetime
    ):

        column_names = [
            self.unified_timestamp_name,
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

        df = pd.DataFrame(all_ohlcv)
        df = df[list(range(len(column_names)))]
        df.columns = column_names
        df[self.unified_timestamp_name] = pd.to_datetime(
            df[self.unified_timestamp_name], unit="ms"
        )

        df.drop_duplicates(self.unified_timestamp_name, inplace=True)
        df.sort_values(self.unified_timestamp_name, inplace=True)
        df[self.unified_market_name] = symbol

        df = df.astype(
            {
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
            }
        )

        df = df[df[self.unified_timestamp_name] >= from_time_dt]

        if self.upload_data:
            self.load_from_dataframe(df, unique_col="close")

    @property
    def period_to_pandas(self) -> dict:
        """frequency needs to be mapped to these offset aliases:
        https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases"""
        return {"8h": "8H", "1m": "1min", "1h": "1H"}

    def fetch_data(self):

        func = None
        if self.exchange_id == "binance":
            func = self.get_all_ohlcv_binance
        elif self.exchange_id == "okx":
            func = self.get_all_ohlcv_okx
        else:
            raise NotImplementedError(f"{self.exchange_id} not implemented")

        self.get_data_foreach_market(fetch_data_function=func)

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
            bigquery.SchemaField(name="volume", field_type="FLOAT", mode="NULLABLE"),
        ]
        return schema

    def time_partitioning(self, table: bigquery.Table):

        time_partition_type = get_time_partitionning_type(self.timeframe_timedelta)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=time_partition_type,
            field=self.unified_timestamp_name,  # name of column to use for partitioning
        )

        return table

    @property
    def limit(self):
        if self.exchange_id == "binance":
            return 1500
        elif self.exchange_id == "okx":
            return 100
        else:
            raise NotImplementedError(f"limit for {self.exchange_id} not implemented")


if __name__ == "__main__":
    # ccxt_ohlcv = CCXTDriverOHLCV(
    #     ccxt_exchange_id="binance",
    #     timeframe="1h",
    #     instrument_type="future",
    #     coinapi_exchange_id="BINANCEFTS",
    #     coinapi_symbol_type="PERPETUAL",
    # )
    # ccxt_ohlcv.fetch_data(upload=True, upload_one_at_a_time=True)
    # pass

    # ccxt_ohlcv = CCXTDriverOHLCV(
    #     ccxt_exchange_id="okx",
    #     timeframe="1m",
    #     instrument_type="index",
    #     coinapi_exchange_id="OKEX",
    #     coinapi_symbol_type="PERPETUAL",
    #     upload_data=True,
    # )
    # ccxt_ohlcv.fetch_data()

    ccxt_ohlcv = CCXTDriverOHLCV(
        ccxt_exchange_id="okx",
        timeframe="1m",
        instrument_type="future",
        coinapi_exchange_id="OKEX",
        coinapi_symbol_type="PERPETUAL",
        upload_data=False,
    )
    ccxt_ohlcv.fetch_data()
    pass
