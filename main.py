from download_yahoo_data import DownloadYahooData
from merge_files import MergeFiles


def main():
    yahoo_data = DownloadYahooData()
    tickers = yahoo_data.get_tickers()
    # print(tickers)
    yahoo_data.download_yf_data('FB', '2018-4-4')
    # print(YahooData.get_id(symbol='AAPL'))
    
    download_path = 'E:\Projects\yahoo-data-download\Data'
    merger = MergeFiles(download_path)
    merger.combine_files()