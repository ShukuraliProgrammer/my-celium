import yfinance as yf



def get_price(ticker, start_time, end_time, interval, round_to_hour:False):

    data = yf.download(ticker,
                       start=start_time,
                       end=end_time,
                       interval=interval
                       )

    data.drop(columns=['Adj Close'], inplace=True)

    data.rename(columns={
        "Date": "startTime",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    }, inplace=True)

    data['volume'] = None

    # sometimes yfinance returns values outside of our range
    data = data.loc[(data.index >= start_time) & (data.index <= end_time)]

    data.reset_index(inplace=True, names=['startTime'])

    # round hours
    if round_to_hour:
        data['startTime'] = data['startTime'].dt.floor('h')

    return data