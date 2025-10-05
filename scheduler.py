import schedule
import time
from script import run_stock_job

from datetime import datetime

def basic_job():
    print("Job Started at: ", datetime.now())

# Run every minute
schedule.every().day.at("18:27").do(basic_job)

# Run every minute
schedule.every().day.at("18:27").do(run_stock_job)

while True:
    schedule.run_pending()
    time.sleep(1)