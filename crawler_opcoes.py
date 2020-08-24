##%%
from sys import stdout
from time import sleep
from os import path, listdir
from sqlite3 import connect
from pandas import DataFrame, read_table, read_sql_query
from functools import reduce
from operator import iconcat

directory = '/home/renato/Desktop/dados históricos/'

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
##%%
for filename in listdir(directory):
    # if filename.find('2020') > -1:
    print(filename.split('/')[-1].replace('COTAHIST_A', '').replace('.TXT', ''))
    df = read_table(directory + filename, encoding = 'ISO-8859-1').iloc[:-1].reset_index()
    df.rename(columns = {df.columns[1]: 'string'}, inplace = True)
    df.drop('index', axis = 1, inplace = True)
    df['date'] = [elem[2:6] + '-' + elem[6:8] + '-' + elem[8:10] for elem in df.string.to_list()]
    df['ticker'] = [elem[12:24].replace(' ', '') for elem in df.string.to_list()]
    # df['currency'] = [elem[52:56].replace('R$', 'BRL') for elem in df.string.to_list()]
    df['open'] = [int(elem[56:69]) / 100 for elem in df.string.to_list()]
    df['high'] = [int(elem[69:82]) / 100 for elem in df.string.to_list()]
    df['low'] = [int(elem[82:95]) / 100 for elem in df.string.to_list()]
    df['mean'] = [int(elem[95:108]) / 100 for elem in df.string.to_list()]
    df['close'] = [int(elem[108:121]) / 100 for elem in df.string.to_list()]
    df['totneg'] = [int(elem[147:152]) for elem in df.string.to_list()]
    df['quatneg'] = [int(elem[152:170]) for elem in df.string.to_list()]
    df['volume'] = [int(elem[170:188]) for elem in df.string.to_list()]
    df['expiration_date'] = [elem[202:206] + '-' + elem[206:208] + '-' + elem[208:210] for elem in df.string.to_list()]
    # df.drop(columns = 'string', inplace = True)
    df = df.sort_values('ticker')
    tickers = df.ticker.unique()
    df = df.groupby('ticker')
    connection = connect('/home/renato/Dropbox/Data Science/portfolio-manager/options.db')
    for ticker in tickers:
        stdout.write('\r\x1b[K' + ticker)
        stdout.flush()
        dataframe = df.get_group(ticker)
        dataframe.to_sql(ticker, connection, if_exists = 'append', index = False)
        if ticker == tickers[-1]:
            stdout.write('\r\x1b[K' + '\n')
            stdout.flush()
    connection.close()

# #%%
# connection = connect('options.db')
# df = read_sql_query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", connection).name
# # connection.close()
# cursor = connection.cursor()
# for ticker in df:
#     if ticker.startswith('A'):
#         print(ticker)
#         cursor.execute('DELETE FROM "{}" WHERE date LIKE "%2004%"'.format(ticker))
#         connection.commit()
# # for ticker in df.to_list():
#     # cursor.execute('DELETE FROM "{}" WHERE date LIKE "%2020%"'.format(ticker))
#     # connection.commit()
# connection.close()
