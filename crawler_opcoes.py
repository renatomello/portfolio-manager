#%%
from sys import stdout
from time import sleep
from os import path, listdir
from sqlite3 import connect
from pandas import DataFrame, read_table, read_sql_query
from functools import reduce
from operator import iconcat
from functions import psqlEngine

directory = '/home/renato/Desktop/dados históricos/'
db_config = 'database.ini'

# #%%
# tickers = list()
# for filename in listdir(directory):
#     df = read_table(directory + filename, encoding = 'ISO-8859-1').iloc[:-1].reset_index()
#     df.rename(columns = {df.columns[1]: 'string'}, inplace = True)
#     df['ticker'] = [elem[12:24].replace(' ', '') for elem in df.string.to_list()]
#     tickers.append(list(df.ticker.unique()))
# tickers = reduce(iconcat, tickers, [])
# tickers.sort()
#
#%%
engine = psqlEngine(db_config)
connection = engine.connect()
for filename in listdir(directory):
    print(filename.split('/')[-1].replace('COTAHIST_A', '').replace('.TXT', ''))
    df = read_table(directory + filename, encoding = 'ISO-8859-1').iloc[:-1].reset_index()
    df.rename(columns = {df.columns[1]: 'string'}, inplace = True)
    df.drop('index', axis = 1, inplace = True)
    df['date'] = [elem[2:6] + '-' + elem[6:8] + '-' + elem[8:10] for elem in df.string.to_list()]
    df['ticker'] = [elem[12:24].replace(' ', '') for elem in df.string.to_list()]
    df['currency'] = [elem[52:56].replace('R$', 'BRL') for elem in df.string.to_list()]
    df['open'] = [int(elem[56:69]) / 100 for elem in df.string.to_list()]
    df['high'] = [int(elem[69:82]) / 100 for elem in df.string.to_list()]
    df['low'] = [int(elem[82:95]) / 100 for elem in df.string.to_list()]
    df['mean'] = [int(elem[95:108]) / 100 for elem in df.string.to_list()]
    df['close'] = [int(elem[108:121]) / 100 for elem in df.string.to_list()]
    df['totneg'] = [int(elem[147:152]) for elem in df.string.to_list()]
    df['quatneg'] = [int(elem[152:170]) for elem in df.string.to_list()]
    df['volume'] = [int(elem[170:188]) for elem in df.string.to_list()]
    df['expiration_date'] = [elem[202:206] + '-' + elem[206:208] + '-' + elem[208:210] for elem in df.string.to_list()]
    start_date = read_sql_query("SELECT date FROM domestic WHERE ticker = 'BOVA11' ORDER BY date DESC LIMIT 1", connection).values[0][0]
    df = df.sort_values('ticker').loc[df.date > start_date]
    df.to_sql('domestic', connection, if_exists = 'append', index = False)
    print('Complete')
connection.close()
engine.dispose()
