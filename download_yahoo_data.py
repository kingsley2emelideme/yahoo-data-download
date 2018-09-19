import pandas as pd
import pyodbc
import os
import fix_yahoo_finance as data
from datetime import date, datetime


class DownloadYahooData:
    def __init__(self):
        self.start_date = None
        self.end_date = None

    @staticmethod
    def get_tickers():
        conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=KINGSLEY;DATABASE=Security; UID=kings;\
        Trusted_Connection=yes;')
        query = 'SELECT [Id], [Symbol] FROM [Security].[dbo].[SecurityDetails]'
        return pd.read_sql_query(query, conn)

    def get_id(self, symbol):
        df = self.get_tickers()
        symbol_id = df['Id'].loc[df['Symbol'] == str(symbol)].values[0]
        return symbol_id

    def download_yf_data(self, symbol, start_date=None, end_date=None):
        download_path = 'E:\Projects\yahoo-data-download\Data'  # type: assert isinstance(str, object)
        if self.start_date is None:
            self.start_date = date.today().strftime('%Y-%m-%d')
        if self.end_date is None:
            self.end_date = date.today().strftime('%Y-%m-%d')
        try:
            price = data.download(symbol, start=start_date, end=end_date)
            price['SymbolId'] = self.get_id(str(symbol))
            price['UpdateDate'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            price['AdjClose'] = price['Adj Close']
            price['Open_'] = price['Open']
            price['Close_'] = price['Close']
            price['PriceDate'] = price.index
            price.reset_index(level=0, inplace=True)
            to_df = price[['PriceDate', 'Open_', 'Low', 'High', 'Close_', 'AdjClose', 'Volume', 'UpdateDate',
                           'SymbolId']]
            to_df.to_csv(os.path.join(download_path, symbol + '.csv'))
            print("Data downloaded successfully for symbol: {} ".format(symbol))
        except Exception as e:
            print("Data not available for download for symbol: {0}, and error: {1} ".format(symbol, e))

    @staticmethod
    def download_adhoc_symbols():
        conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=KINGSLEY;DATABASE=Security; UID=kings;\
                Trusted_Connection=yes;')
        query = 'SELECT ltrim(rtrim(Symbol)) "Symbol" FROM [Security].[dbo].[SecurityDetails] WHERE Id NOT IN \
        (select SymbolId FROM SecurityPrice where PriceDate = CAST(GETDATE() AS DATE))'
        return pd.read_sql_query(query, conn)

