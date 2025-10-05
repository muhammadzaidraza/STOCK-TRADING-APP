import requests
import os
import time
import sys
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime

def run_stock_job():
    run_date = datetime.now().strftime('%Y-%m-%d')

    # Load environment variables
    load_dotenv()
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

    # Snowflake credentials from .env
    SF_ACCOUNT = os.getenv("SF_ACCOUNT")
    SF_USER = os.getenv("SF_USER")
    SF_PASSWORD = os.getenv("SF_PASSWORD")
    SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
    SF_DATABASE = os.getenv("SF_DATABASE")
    SF_SCHEMA = os.getenv("SF_SCHEMA")
    SF_TABLE = os.getenv("SF_TABLE", "STOCK_TICKERS")

    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SF_ACCOUNT"),
            user=os.getenv("SF_USER"),
            password=os.getenv("SF_PASSWORD"),
            warehouse=os.getenv("SF_WAREHOUSE"),
            database=os.getenv("SF_DATABASE"),
            schema=os.getenv("SF_SCHEMA"),
        )

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"✅ Connected to Snowflake. Version: {version[0]}")

    except Exception as e:
        print("❌ Failed to connect to Snowflake.")
        print("Error:", e)
        sys.exit(1)

    # Polygon API request
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    tickers = []
    data = response.json()

    # Collect first page of results
    tickers.extend(data['results'])

    # Loop through pagination
    while 'next_url' in data:
        print(len(tickers))
        time.sleep(20)
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        tickers.extend(data['results'])
        print(f"Collected {len(tickers)} tickers so far...")

    # Insert data into Snowflake
    insert_query = f"""
        INSERT INTO {SF_TABLE} (
            ticker, name, market, locale, primary_exchange, type, active, currency_name, 
            cik, composite_figi, share_class_figi, last_updated_utc, ds
        ) VALUES (%(ticker)s, %(name)s, %(market)s, %(locale)s, %(primary_exchange)s, %(type)s, 
                  %(active)s, %(currency_name)s, %(cik)s, %(composite_figi)s, %(share_class_figi)s, %(last_updated_utc)s, %(ds)s)
    """
    
    expected_keys = [
        'ticker', 'name', 'market', 'locale', 'primary_exchange', 'type',
        'active', 'currency_name', 'cik', 'composite_figi', 'share_class_figi', 'last_updated_utc', 'ds'
    ]

    # Prepare safe tickers
    safe_tickers = [
        {**{key: t.get(key, None) for key in expected_keys}, "ds": run_date}
        for t in tickers
    ]

    # Insert all rows at once
    cursor.executemany(insert_query, safe_tickers)
    conn.commit()

    print(f"Inserted {len(tickers)} tickers into Snowflake table {SF_TABLE}")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    run_stock_job()
