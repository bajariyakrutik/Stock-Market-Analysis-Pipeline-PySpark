from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import time

# Your API Key
api_key = 'API_KEY_HERE'  # Replace with your actual Alpha Vantage API key

# Initialize Alpha Vantage TimeSeries
ts = TimeSeries(key=api_key, output_format='pandas')

# Define tickers
tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA',
           'NVDA', 'META', 'NFLX', 'AMD', 'CRM']

# Dictionary to store closing prices
close_data = {}

# Download data with rate limiting
for ticker in tickers:
    try:
        print(f"Fetching {ticker}...")
        data, meta_data = ts.get_daily(symbol=ticker, outputsize='compact')  # 'compact' = last 100 days
        close_data[ticker] = data['4. close']
        time.sleep(12)  # Sleep to avoid hitting the 5-requests-per-minute limit
    except Exception as e:
        print(f"Failed to fetch {ticker}: {e}")

# Combine all into one DataFrame
df = pd.DataFrame(close_data)
df.index = pd.to_datetime(df.index)
df.sort_index(inplace=True)

# Save to CSV
df.to_csv('alphavantage_stock_closing_prices.csv')
print("✅ Data saved to 'alphavantage_stock_closing_prices.csv'")