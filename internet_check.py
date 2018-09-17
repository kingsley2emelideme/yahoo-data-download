import requests
import pandas as pd

x = pd.read_csv('E:\Projects\yahoo-data-download\Data\AAC.csv', index_col=None)
x.drop(['Unnamed: 0'], axis=1, inplace=True)
print(x)
#
# def check_internet():
#     url = 'http://www.google.com/'
#     timeout = 5
#     try:
#         _ = requests.get(url, timeout=timeout)
#         return True
#     except requests.ConnectionError:
#         print("No internet connection.")
#     return False
