#%%
from time import sleep, time
from os import listdir
from functions import psqlEngine
from secret import db_config, intraday_av, key_renato as key
from pandas import read_sql_query, read_csv

#%%
# dataframe = read_csv('../extended_intraday_dataf_15min_year1month1_adjusted.csv')
# engine = psqlEngine(db_config)
# connection = engine.connect()
# tickers = ['clov']
# tickers = ['amzn', 'intc', 'ipod-u', 'ipoe-u', 'ipof-u', 'spot']
# tickers = read_sql_query("SELECT DISTINCT ticker FROM usa_stocks ORDER BY ticker", connection).ticker.to_list()
# connection.close()
# engine.dispose()

#%%
timeline = 'year1month1'
path = '../intraday_files/{}/'.format(timeline)
tickers = list()
for stock in listdir(path):
    ticker = stock.split('_')[0]
    tickers.append(ticker)
tickers.sort()

#%%
# timeline = 'year2month9'
for ticker in tickers:
    print(ticker.upper())
    # dataframe = read_csv('../intraday_files/{}.csv'.format(timeline))#, '1min', timeline, key))
    dataframe = read_csv(intraday_av.format(ticker.upper(), '1min', timeline, key))
    # dataframe.to_csv('../intraday_files/{}.csv'.format(timeline))#, ticker.upper(), '1min', timeline, key))
    dataframe.to_csv(path + '{}_{}_{}.csv'.format(ticker.upper(), '1min', timeline))
    # dataframe.rename(columns = {'time': 'datetime'}, inplace = True)
    # dataframe['date'] = [data.split(' ')[0] for data in dataframe.datetime]
    # dataframe['time'] = [data.split(' ')[1] for data in dataframe.datetime]
 
    # # engine = psqlEngine(db_config)
    # # connection = engine.connect()
    # start_date = read_sql_query("SELECT MAX(date) FROM usa_stocks WHERE ticker = '{}'".format(ticker.upper()), connection).values[0][0]
    # # connection.close()
    # # engine.dispose()
    # dataframe = dataframe.loc[(dataframe.date > start_date) & (dataframe.time >= '09:30:00') & (dataframe.time <= '16:30:00')]
    # dataframe.sort_values(['date', 'time'], inplace = True)

    # data = dataframe.date.unique()
    # open = dataframe.groupby('date')[['time', 'open']].agg({'time': [min]}).droplevel(2, axis = 1).time.open.to_list()
    # close = dataframe.groupby('date')[['time', 'close']].agg({'time': [max]}).droplevel(2, axis = 1).time.close.to_list()

    # dataf = DataFrame({
    #     'date': data,
    #     'open': open,
    #     'high': dataframe.groupby('date').max()[['open', 'high', 'low', 'close']].max(axis = 1).to_list(),
    #     'low': dataframe.groupby('date').min()[['open', 'high', 'low', 'close']].min(axis = 1).to_list(),
    #     'close': close,
    #     'adjusted_close': close,
    #     'volume': dataframe.groupby('date').volume.sum().to_list(),
    # })

    # dataframe.drop(columns = ['datetime', 'time'], inplace = True)
    # dataf = dataf.loc[dataf.date > start_date]
    # dataf['dividend_amount'] = [0.]*len(dataf)
    # dataf['split_coefficient'] = [1.]*len(dataf)
    # dataf['ticker'] = [ticker.upper()]*len(dataf)
    # dataf['currency'] = ['USD']*len(dataf)
    # dataf.to_sql('usa_stocks', connection, if_exists = 'append', index = False)
    if ticker != tickers[-1]:
        sleep(15.1)
# connection.close()
# engine.dispose()
