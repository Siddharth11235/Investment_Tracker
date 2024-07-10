import csv
import os
from datetime import datetime
import yfinance as yf

def fetch_historical_hourly_data(symbol, start_date, end_date):
    data = yf.download(symbol, start=start_date, end=end_date, interval='60m')
    data.reset_index(inplace=True)
    data['symbol'] = symbol
    all_data = []
    for _, row in data.iterrows():
        all_data.append((row['Datetime'], symbol, row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
    return all_data

tickers = ["AAPL", "SPY", "NVDA", "META", "IMAE.AS", "VWCE.DE"]
file_path = 'data/stock_info.csv'

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 7, 10)  # today's date

for symbol in tickers:
    data = fetch_historical_hourly_data(symbol, start_date, end_date)
    file_exists = os.path.isfile(file_path)
    header = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    
    with open(file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(data)
