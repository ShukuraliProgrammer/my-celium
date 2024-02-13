from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Callable
import logging
from drivers.base import DataDriver
import ccxt
from ccxt.base.errors import BadSymbol
from time import sleep
import pandas as pd

logger = logging.getLogger(__name__)


class CCXTBase(DataDriver, ABC):
    def __init__(
        self,
        ccxt_exchange_id,
        table_name,
        coinapi_exchange_id,
        coinapi_symbol_type,
        instrument_type,
        timeframe,
        upload_data,
    ):

        # https://www.coinapi.io/integration
        self.coinapi_exchange_id = coinapi_exchange_id
        # https://docs.coinapi.io/#list-all-symbols-get
        self.coinapi_symbol_type = coinapi_symbol_type

        self.exchanges = ccxt.exchanges
        self.instrument_type = instrument_type

        self.upload_data = upload_data

        assert (
            ccxt_exchange_id in self.exchanges
        ), f"{ccxt_exchange_id} exchange not supported"

        self.exchange_id = ccxt_exchange_id

        self.ccxt_default_type = self.instrument_type

        if self.exchange_id == "okx":
            if self.instrument_type == "index":
                self.ccxt_default_type = "future"

        assert (
            self.ccxt_default_type in self.supported_default_types
        ), f"instrument_type:{instrument_type} must be one of {self.supported_default_types}"

        # https://github.com/ccxt/ccxt/blob/00fb5389da706c0acc3bb0892ffa211ac535bd3b/python/ccxt/binance.py#L857
        self.exchange = getattr(ccxt, self.exchange_id)(
            {
                "enableRateLimit": True,  # required by the Manual
                "options": {
                    "defaultType": self.ccxt_default_type,
                },
                # "proxies": {
                #     "http": 'http://brd-customer-hl_21b2fb22-zone-isp:futtrr86b9g7@zproxy.lum-superproxy.io:22225',
                #     "https": 'https://brd-customer-hl_21b2fb22-zone-isp:futtrr86b9g7@zproxy.lum-superproxy.io:22225',
                # }
            }
        )

        self.exchange.load_markets()

        self.max_retries = 3

        table_name = f"{table_name}_{self.instrument_type}_{timeframe}"
        self.timeframe = timeframe

        if timeframe == "1Mutc":
            self.timeframe_timedelta = timedelta(seconds=60)
        else:
            self.timeframe_timedelta = timedelta(
                seconds=self.exchange.parse_timeframe(self.timeframe)
            )

        self.timedelta_window = self.timeframe_timedelta * self.limit

        if not "funding" in table_name:
            assert (
                self.timeframe in self.possible_resolutions.keys()
            ), f"{self.timeframe} timeframe not supported"

        super().__init__(
            dataset_id=ccxt_exchange_id, table_name=table_name, timeframe=self.timeframe
        )

        self.markets = self.CoinApi.get_all_assets_for_exchange(
            coinapi_exchange_id=coinapi_exchange_id,
            coinapi_symbol_type=coinapi_symbol_type,
        )

        self.market_names = self.markets["symbol_id_exchange"]

    @property
    def supported_default_types(self):
        return ["spot", "margin", "delivery", "future"]

    @property
    @abstractmethod
    def limit(self):
        pass

    @property
    def possible_resolutions(self):
        return self.exchange.timeframes

    def get_data_foreach_market(
        self,
        fetch_data_function: Callable,
        upload: bool = False,
        upload_one_at_a_time: bool = False,
    ):
        """This function loops through each market, if it's already in the database then we fetch from that date
        And if it's not we fetch since the beginging of time

        This is a generic function, so we pass a more specific function for each data type that return the full
        timeseries as a Dataframe that is either uploaded at the end (best of periodical update) or for each asset
        (best for an upload from scratch since there's a lot of data)

        :param fetch_data_function: the function that the fetches the timeseries in question
        :param upload: whether to upload the data or not (False useful for debugging/development)
        :param upload_one_at_a_time: whether to upload for each asset or all in one go (bad idea if there's a lot data
        """

        to_time_since_dt = datetime.now()

        tracked_assets = self.get_latest_date()

        # anything older than 14 days we consider as delisted
        delisted_threshold = to_time_since_dt - timedelta(days=14)

        # we get the asset list from CoinAPI since Binance doesn't provide us with name of assets that are delisted
        coinapi_assets = self.CoinApi.get_all_assets_for_exchange(
            coinapi_exchange_id=self.coinapi_exchange_id,
            coinapi_symbol_type=self.coinapi_symbol_type,
        )

        # since it's hashable, we don't get any duplicates
        symbols = dict()

        for index, row in coinapi_assets.iterrows():

            symbol = row["symbol_id_exchange"]

            if self.exchange_id == "okx":
                if self.instrument_type == "future":
                    symbol = f"{row['asset_id_base_exchange']}-{row['asset_id_quote_exchange']}-SWAP"  # OKX: "BTC-USDT-SWAP"
                elif self.instrument_type == "index":
                    symbol = f"{row['asset_id_base_exchange']}-{row['asset_id_quote_exchange']}"  # OKX: "BTC-USDT"

            if self.instrument_type == "future":
                homogenised_symbol = f"{row['asset_id_base_exchange']}-{row['asset_id_quote_exchange']}-SWAP"  # OKX: "BTC-USDT-SWAP"
            elif self.instrument_type == "index":
                homogenised_symbol = f"{row['asset_id_base_exchange']}-{row['asset_id_quote_exchange']}"  # OKX: "BTC-USDT"
            else:
                homogenised_symbol = (
                    f"{row['asset_id_base_exchange']}-{row['asset_id_quote_exchange']}"
                )

            symbols[homogenised_symbol] = symbol

        # useful for DEBUG
        # symbols = {"ETH_USD_SWAP": "ETH-USD"}

        for homogenised_symbol, symbol in symbols.items():

            if symbol in tracked_assets["ticker"].to_list():
                since_dt = tracked_assets[tracked_assets["ticker"] == symbol][
                    "maxStartTime"
                ].iloc[0]
                logger.info(
                    f"{symbol} found in DB, starting from latest date: {since_dt}"
                )

                if since_dt + self.timeframe_timedelta > datetime.utcnow():
                    logger.info(
                        f"{symbol}: latest datetime from BQ ({since_dt}) is very recent, so we skip"
                    )
                    continue

            else:
                since_dt = datetime(2010, 1, 1)
                logger.info(
                    f"{symbol} NOT found in DB, starting from latest date: {since_dt}"
                )

            since_dt_plus_one = since_dt + self.timeframe_timedelta

            is_success = fetch_data_function(
                market=symbol,
                from_time_dt=since_dt_plus_one,
                to_time_dt=to_time_since_dt,
            )

            if is_success is False:
                logger.warning(f"no data found for {symbol}, skipping...")
            else:
                logger.info(f"Sucessfully loaded {symbol}.")

    def _retry_fetch_function(self, callable_function: Callable, *args, **kwargs):
        num_retries = 0

        while True:
            try:
                num_retries += 1
                if callable_function.__name__ == "fetch_ohlcv":
                    data = callable_function(**kwargs)
                else:
                    data = callable_function(params=kwargs)

                return data
            except BadSymbol as e:
                raise e
            except Exception as e:
                if num_retries >= self.max_retries:
                    raise Exception(
                        f"FAILED ({e}): Couldn't call {callable_function} function {num_retries}/{self.max_retries}"
                    )
                logger.warning(f"FAILED {num_retries}/{self.max_retries}: {e}")
                sleep(1)
