# Mycelium

Mycelium is a data financial data collection software that runs periodically to update a centralised database. The data 
is primarily used for research by Quantitative Analysts.

### Data Sources

| Name     | Data Type            | Instruments       | Period      | Status        | Bulk | Updates |
|----------|----------------------|-------------------|-------------|---------------|------|---------|
| Binance  | OHLCV                | Spot, Perp        | 1D, 1H, 1M  | Implemented   | API  | API     |
| Binance  | Funding Rate         | Perp              | 8H          | Implemented   | API  | API     | 
| OKX      | OHLCV                | Spot, Perp, Index | 1D, 1H, 1M  | Implemented   | Web  | API     |
| OKX      | Funding Rate         | Perp              | 8H          | Implemented   | Web  | API     |
| Bybit    | OHLCV                | Spot, Perp, Index | 1D, 1H, 1M  | TODO          | TBC  | TBC     |
| Bybit    | Funding Rate         | Perp              | TBC         | TODO          | TBC  | TBC     |
| DyDx     | OHLCV                | Perp              | 1D, 1H, 1M  | Implemented   | API  | API     |
| DyDx     | Funding Rate         | Perp              | 1H          | Implemented   | API  | API     |
| Coinbase | OHLCV                | Perp              | 1D, 1H, 1M  | TODO          | TBC  | TBC     |
| FTX      | OHCLCV, Funding Rate | Spot, Perp        | 1D, 1H, 1M  | Deprecated    | API  | API     |

Please note that in some cases, it's not possible to retrieve historical data for delisted tickers from the API, 
we must therefore find another source. 

For example, currently the best way to obtain survivorship-bias free OHLCV data for [OKX](https://www.okx.com/) 
is to reconstruct the candles using aggregated trade data downloaded from static files 
[here](https://www.okx.com/data-download). This is why we have two sources, Bulk and Updates. Bulk is used 
once to fetch historical data and Updates is used for periodical latest data updates.

## Development

To add a new data source, create a folder in `./drivers` and in order for the class to access key 
utility function pass the [DataDriver](./drivers/base.py) class as the Base class.

## Production

In production, each data source must be included in `main.py` at the root, then the jobs are schedules using 
[ApsScheduler](https://apscheduler.readthedocs.io/en/3.x/).

The data is currently stored in [BigQuery](https://cloud.google.com/bigquery), when launching a driver we automatically 
check if the table exists and if not then we create it.

The app is currently being deployed to [Railway](https://railway.app/), using Github continious integration of the `main`
branch.

## Key Data Requirements

All these points should be checked for and handled in the `DataDriver` base class.

### 1. Data Homogenisation
- **Ticker names**:`ticker` columns should be homogenised accross all exchanges in the following format:
  - `QUOTE-BASE-TYPE`, for example `BTCUSDT` would become `BTC-USDT-SPOT` for spot market or `BTC-USDT-INDEX` for an index
- **Timestamps**: every row entry should have a timestamp column which should have an equal name
  - This name is currently defined in [DataDriver](./drivers/base.py) as `self.unified_timestamp_name = "startTime"` however we will need to change this to `ts` since it's more memory efficient and the entry does not always correspond to the starting time
### 2. Suvirvorship-bias
- It's extremely important that every Data Type contains tickers that have been removed/delisted from the exchanges, this is currently tackled by retrieving the list of assets from [CoinAPI](https://www.coinapi.io/)
### 3. Point-in-time
- An other critically important point, is to next shift the data relative to timestamp, this could introduce a look-ahead bias in the data which would be destructive. Point-in-time data refers to the fact that for a given timestamp the information presented is only available at that point in time.
### 4. Clean Data
- The data provided by the source is not always clean, we must therefore correct this. For example, on hourly data we would expect the seconds to always be `0`, we fix this using Pandas's resample function.


## Coding guidelines

1. PEP8 - please use the code formatter, black: https://github.com/psf/black
2. docstrings - please use Google style
   docstrings: https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings
3. For all other style matters, please follow the Google styleguide for python.
4. Tests - Please use pytest
5. Dependencies - please try to minimise them.
   6. Simplicity - please don't try to be too clever; code should be as simple as possible to understand its purpose.
