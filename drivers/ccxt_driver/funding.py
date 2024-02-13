from datetime import datetime, timedelta
import logging
import pandas as pd
from drivers.ccxt_driver.ccxt_base import CCXTBase
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CCXTDriverFunding(CCXTBase):
    def __init__(self, ccxt_exchange_id, coinapi_exchange_id, coinapi_symbol_type):

        table_name = "funding"
        default_type = "future"
        timeframe = "8h"

        super().__init__(
            table_name=table_name,
            ccxt_exchange_id=ccxt_exchange_id,
            coinapi_exchange_id=coinapi_exchange_id,
            coinapi_symbol_type=coinapi_symbol_type,
            timeframe=timeframe,
            instrument_type=default_type,
            full_history=True,
        )

    def get_all_funding(
        self, market: str, from_time_dt: datetime = None, to_time_dt: datetime = None
    ) -> pd.DataFrame:

        # assert market in self.market_names.to_list(), f"{market} market not supported"

        timedelta_window = self.timeframe_timedelta * self.limit
        earliest_datetime = datetime.now() if to_time_dt is None else to_time_dt
        fetch_since = earliest_datetime - timedelta_window

        all_funding = []

        while True:

            try:
                if self.exchange_id == "okx":
                    funding = self._retry_fetch_function(
                        callable_function=self.exchange.fetchFundingRateHistory,
                        market=market,
                        since=fetch_since,
                        limit=self.limit,
                    )
                elif self.exchange_id == "binance":
                    funding = self._retry_fetch_function(
                        callable_function=self.exchange.fetchFundingRateHistory,
                        symbol=market,
                        startTime=str(int(fetch_since.timestamp() * 1000)),
                        limit=self.limit,
                    )
                else:
                    raise NotImplementedError(f"{self.exchange_id} not implemented")

            except Exception as e:
                logger.warning(e)
                break

            if len(funding) == 0:
                logger.info(f"QUITTING: funding response is empty so quitting")
                break

            if self.exchange_id == "binance":
                earliest_timestamp = self.exchange.parse_date(
                    funding[0].get("datetime")
                )
                latest_timestamp = self.exchange.parse_date(funding[-1].get("datetime"))
            elif self.exchange_id == "okx":
                earliest_timestamp = funding[0].get("timestamp")
                latest_timestamp = funding[-1].get("timestamp")
            else:
                raise NotImplementedError(f"{self.exchange_id} not implemented")

            earliest_datetime = datetime.utcfromtimestamp(earliest_timestamp / 1000)
            latest_datetime = datetime.utcfromtimestamp(latest_timestamp / 1000)

            all_funding = funding + all_funding

            if earliest_datetime > (fetch_since + self.timeframe_timedelta):
                logger.info(f"earliest_datetime > fetch_since, quitting")
                break

            if self.exchange_id == "binance":
                fetch_since = (
                    earliest_datetime - timedelta_window + (self.timeframe_timedelta * 5)
                )
            elif self.exchange_id == "okx":
                fetch_since = earliest_datetime

            logger.info(
                f"{market} {earliest_datetime} -> {latest_datetime} {len(funding)}/{len(all_funding)}"
            )
            # if we have reached the checkpoint
            if len(funding) < self.limit:
                logger.info(
                    f"QUITTING: funding_length({len(funding)}) < limit({self.limit}) so quitting"
                )
                break

            elif from_time_dt is not None and earliest_datetime <= from_time_dt:
                logger.info(
                    f"QUITTING: earliest_datetime({len(funding)}) <= since_dt({from_time_dt}) so quitting"
                )
                break

        if len(all_funding) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(all_funding, columns=["timestamp", "symbol", "fundingRate"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["timestamp"] = df["timestamp"].dt.floor("S")
        df.rename(
            columns={
                "timestamp": self.unified_timestamp_name,
                "symbol": self.unified_market_name,
            },
            inplace=True,
        )

        if from_time_dt is not None:
            df = df[df[self.unified_timestamp_name] >= from_time_dt]

        # remove duplicated
        df = df[~df.duplicated(["startTime"])]

        return df.reset_index(drop=True)

    def fetch_data(self, upload: bool = False, upload_one_at_a_time: bool = False):

        self.get_data_foreach_market(
            fetch_data_function=self.get_all_funding,
            upload=upload,
            upload_one_at_a_time=upload_one_at_a_time,
        )

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
                name="fundingRate", field_type="FLOAT", mode="REQUIRED"
            ),
        ]
        return schema

    def time_partitioning(self, table: bigquery.Table):
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.HOUR,
            field=self.unified_timestamp_name,  # name of column to use for partitioning
        )

        return table

    @property
    def limit(self):
        if self.exchange_id == "binance":
            return 1000
        elif self.exchange_id == "okx":
            return 100
        else:
            raise NotImplementedError(f"limit for {self.exchange_id} not implemented")


if __name__ == "__main__":
    # ccxt_funding = CCXTDriverFunding(
    #     ccxt_exchange_id="binance",
    #     coinapi_exchange_id="BINANCEFTS",
    #     coinapi_symbol_type="PERPETUAL"
    # )
    # ccxt_funding.fetch_data(upload=False, upload_one_at_a_time=True)
    # pass

    # ccxt_funding = CCXTDriverFunding(
    #     ccxt_exchange_id="okx",
    #     coinapi_exchange_id="OKEX",
    #     coinapi_symbol_type="PERPETUAL"
    # )
    ccxt_funding = CCXTDriverFunding(
        ccxt_exchange_id="binance",
        coinapi_exchange_id="BINANCEFTS",  # BINANCE for spot
        coinapi_symbol_type="PERPETUAL",
    )
    ccxt_funding.fetch_data(upload=True, upload_one_at_a_time=True)
    pass
