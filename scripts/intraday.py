#%%
from sys import stdout
from os import listdir
from pandas import read_csv, read_sql_query, DataFrame, to_datetime
from functions import psqlEngine
from secret import db_config, intraday_av, key_renato

import matplotlib.pyplot as plt

# #%%
# path = '../intraday_files/year{}month{}/'
# months, years = range(1, 0, -1), range(2, 0, -1)

# #%%
# engine = psqlEngine(db_config)
# connection = engine.connect()
# for year in years[1:]:
#     for month in months:
#         print('\nYear: {}, Month: {}'.format(year, month))
#         stocks = listdir(path.format(year, month))
#         stocks.sort()
#         for stock in stocks:
#             ticker = stock.split('_')[0]
#             print_string = '{}'.format(ticker)
#             stdout.write('\r\x1b[K' + print_string)
#             stdout.flush()
#             df = read_csv(path.format(year, month) + '/' + stock)
#             df['ticker'] = [ticker]*len(df)
#             df['currency'] = ['USD']*len(df)
#             df.to_sql('usa_stocks_intraday_1min', connection, if_exists = 'append', index = False)
# connection.close()
# engine.dispose()

#%%
engine = psqlEngine(db_config)
connection = engine.connect()
df = read_sql_query("SELECT * FROM usa_stocks_intraday_d WHERE ticker = 'TSLA'", connection)
# tickers = read_sql_query('SELECT DISTINCT ticker FROM usa_stocks_intraday_4h ORDER BY ticker', connection).ticker.to_list()
# df = read_sql_query("SELECT time, open, high, low, close, volume FROM usa_stocks_intraday_1min WHERE ticker = 'AAPL' ORDER BY ticker LIMIT 25", connection)
connection.close()
engine.dispose()
df['time'] = to_datetime(df.time, format='%Y-%m-%d %H:%M:%S')
df.set_index('time', inplace=True)
# resampled = df.resample('5min', base = 30, label = 'right')
# resampled.volume.sum().index.strftime('%Y-%m-%d %H:%M:%S').to_list()

#%%
df.close.plot()

#%%
engine = psqlEngine(db_config)
connection = engine.connect()
# intervals = ['15min', '30min', '1h', '2h', '3h', '4h']
intervals = ['D', 'W', 'M']
for interval in intervals:
    print('\nInterval: {}'.format(interval))
    for ticker in tickers:
        print_string = '{}'.format(ticker)
        stdout.write('\r\x1b[K' + print_string)
        stdout.flush()
        dataf = DataFrame()
        df = read_sql_query("SELECT time, open, high, low, close, volume FROM usa_stocks_intraday_1min WHERE ticker = '{}' ORDER BY time".format(ticker), connection)
        df['time'] = to_datetime(df.time)
        df.set_index('time', inplace=True)
        df['hours'] = df.index.time.astype(str)
        df = df.loc[(df.hours >= '09:30') & (df.hours < '16:00')]
        df.drop(columns = 'hours', inplace = True)
        resampled = df.resample(interval, base = 30, label = 'right')
        dataf['time'] = resampled.volume.sum().index.strftime('%Y-%m-%d %H:%M:%S').to_list()
        dataf['time'] = dataf.time.astype(str)
        dataf['open'] = resampled.open.first().to_list()
        dataf['high'] = resampled.high.max().to_list()
        dataf['low'] = resampled.low.min().to_list()
        dataf['close'] = resampled.close.last().to_list()
        dataf['volume'] = resampled.volume.sum().to_list()
        dataf['ticker'] = [ticker]*len(dataf)
        dataf['currency'] = ['USD']*len(dataf)
        dataf.to_sql('usa_stocks_intraday_{}'.format(interval.lower()), connection, if_exists = 'append', index = False)
connection.close()
engine.dispose()
