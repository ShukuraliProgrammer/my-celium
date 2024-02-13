import os

import pandas as pd
import requests
import logging

logger = logging.getLogger(__name__)


class CoinAPI:
    """
    for list of exchanges see here: https://www.coinapi.io/integration
    """

    def __init__(self):

        self.enpoint = "https://rest.coinapi.io"

        self.header = {"X-CoinAPI-Key": os.getenv("COINAPI_API_KEY")}

        self.exchanges = self._get_all_exchange()

        # we map symbol types from ccxt to CoinAPI
        self.symbol_types = [
            "SPOT",
            "FUTURES",
            "OPTION",
            "PERPETUAL",
            "INDEX",
            "CREDIT",
            "CONTRACT",
        ]

    def _get_all_exchange(self):

        all_exchanges = "/v1/exchanges"
        url = self.enpoint + all_exchanges

        try:
            response = requests.get(url, headers=self.header)
            response.raise_for_status()
        except Exception as e:
            raise e

        return response.json()

    def get_exchange_data(self, exchange_name):
        """list of exchanges can be found here: https://www.coinapi.io/integration

        :param exchange_name:
        :return:
        """
        for exchange in self.exchanges:

            if exchange["exchange_id"].lower() == exchange_name.lower():
                return exchange

        logger.warning(f"echange {exchange_name} is not supported by CoinAPI")
        return None

    def get_all_assets_for_exchange(
        self, coinapi_exchange_id: str, coinapi_symbol_type: str = None
    ) -> None:
        """Get all assets with which have a start and end trading date

        :param symbol_id_exchange:
        :return:
        """

        assert coinapi_symbol_type in self.symbol_types, (
            f"{coinapi_symbol_type} type requested, is not valid. "
            f"See https://docs.coinapi.io/#list-all-symbols-get"
        )

        exchange_data = self.get_exchange_data(coinapi_exchange_id)

        if exchange_data is None:
            return None

        symbol_id_exchange = exchange_data.get("exchange_id")
        all_symbols = f"/v1/symbols/{symbol_id_exchange}"

        url = self.enpoint + all_symbols
        symbols_resp = requests.get(url, headers=self.header)

        if symbols_resp.status_code != 200:
            raise Exception(f"Couldn't correct to CoinAPI: {symbols_resp.json()}")

        symbols_df = pd.DataFrame(symbols_resp.json())

        selected_type = symbols_df[
            symbols_df["symbol_type"] == coinapi_symbol_type
        ].copy()

        # do we need to filter out symbols that are missing "data_trade_start" and "data_trade_end"?

        selected_type["data_trade_start"] = pd.to_datetime(
            symbols_df["data_trade_start"]
        )
        selected_type["data_trade_end"] = pd.to_datetime(symbols_df["data_trade_end"])

        return selected_type.reset_index(drop=True)
