import os
import time
from datetime import date, timedelta

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

from bulk_insert_data import bulk_insert_data
from download_yahoo_data import DownloadYahooData
from merge_files import MergeFiles


def main():
    date_today = (date.today()-timedelta(days=3)).strftime('%Y-%m-%d')
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    business_day = pd.DatetimeIndex(start=date_today, end=date_today, freq=us_bd)
    download_path = 'E:\Projects\yahoo-data-download\Data'
    merge_path = 'E:\Projects\yahoo-data-download\MergedFiles'

    if date_today in business_day:
        # Empty folders
        remove_files(download_path)
        remove_files(merge_path)
        # Pull Yahoo Finance Data
        yahoo_data = DownloadYahooData()
        tickers = yahoo_data.get_tickers()['Symbol']
        for sym in tickers:
            yahoo_data.download_yf_data(sym, start_date=date_today, end_date=date_today)
        # Process files
        merger = MergeFiles(download_path)
        merger.combine_files()
        # Bulk insert into MS SQL Server
        bulk_insert_data()
    else:
        print("Today is a Federal Holiday, markets are closed")


def remove_files(file_path):
    files = os.listdir(file_path)
    for ff in files:
        os.remove(os.path.join(file_path, ff))
        print("Removing file: {}".format(ff))


def run_adhoc_process():
    date_today = (date.today()-timedelta(days=3)).strftime('%Y-%m-%d')
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    business_day = pd.DatetimeIndex(start=date_today, end=date_today, freq=us_bd)
    download_path = 'E:\Projects\yahoo-data-download\Data'
    merge_path = 'E:\Projects\yahoo-data-download\MergedFiles'

    if date_today in business_day:
        # Empty folders
        remove_files(download_path)
        remove_files(merge_path)
        # Pull Yahoo Finance Data
        yahoo_data_adhoc = DownloadYahooData()
        tickers = yahoo_data_adhoc.download_adhoc_symbols()['Symbol'].values
        for sym in tickers:
            yahoo_data_adhoc.download_yf_data(sym, start_date=date_today, end_date=date_today)
        # Process files
        merger = MergeFiles(download_path)
        merger.combine_files()
        # Bulk insert into MS SQL Server
        bulk_insert_data()
    else:
        print("Today is a Federal Holiday, markets are closed")


if __name__ == '__main__':
    main()
    time.sleep(20)
    for _ in range(4):
        run_adhoc_process()
