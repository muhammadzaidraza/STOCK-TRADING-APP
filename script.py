import requests 
import os
import time
import csv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Polygon API request
url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
tickers = []
data = response.json()

# Collect first page of results
for ticker in data['results']:
    tickers.append(ticker)

# Loop through pagination
while 'next_url' in data:
    time.sleep(20)
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')

    data = response.json()

    for ticker in data['results']:
        tickers.append(ticker)

    print(f"Collected {len(tickers)} tickers so far...")

# Example ticker defines which fields we’ll write
example_ticker = {
    'ticker': 'CHRW', 
    'name': 'C.H. Robinson Worldwide, Inc.',
    'market': 'stocks',
    'locale': 'us',
    'primary_exchange': 'XNAS',
    'type': 'CS',
    'active': True,
    'currency_name': 'usd',
    'cik': '0001043277',
    'composite_figi': 'BBG000BTCH57',
    'share_class_figi': 'BBG001SB6KF5',
    'last_updated_utc': '2025-09-16T06:05:51.696907049Z'
}

# CSV file path
csv_file = "tickers.csv"

# Write to CSV
with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=example_ticker.keys())
    writer.writeheader()
    for t in tickers:
        # filter ticker dict to only include keys we care about
        row = {key: t.get(key, "") for key in example_ticker.keys()}
        writer.writerow(row)

print(f"Saved {len(tickers)} tickers to {csv_file}")