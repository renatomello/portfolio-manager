#%%
from sys import stdout
from os import listdir
from pandas import read_table, read_sql_query
from functions import psqlEngine

directory = '/home/renato/Desktop/dados históricos/'
db_config = 'database.ini'

engine = psqlEngine(db_config)
connection = engine.connect()
for filename in listdir(directory):
    if filename.endswith('.TXT'):
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
        df['split_numerator'] = [1.] * len(df)
        df['split_denominator'] = [1.] * len(df)
        df['adjusted_open'] = df.open.to_list()
        df['adjusted_high'] = df.high.to_list()
        df['adjusted_low'] = df.low.to_list()
        df['adjusted_mean'] = df['mean'].to_list()
        df['adjusted_close'] = df.close.to_list()
        # print('benchmark')
        # start_date = read_sql_query("SELECT MAX(date) FROM benchmarks WHERE ticker = 'BOVA11'", connection).values[0][0]
        # dataf = df.loc[(df.date > start_date) & (df.ticker == 'BOVA11')]
        # dataf.to_sql('benchmarks', connection, if_exists = 'append', index = False)
        print('brazil_stocks')
        start_date = read_sql_query("SELECT MAX(date) FROM brazil_stocks WHERE ticker = 'BOVA11'", connection).values[0][0]
        df = df.sort_values('ticker').loc[df.date > start_date]
        tickers_1 = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks WHERE LENGTH(ticker) <= 5 ORDER BY ticker", connection).ticker.to_list()
        tickers_2 = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks WHERE LENGTH(ticker) = 6 AND (ticker LIKE '%%11' OR ticker LIKE '%%34') ORDER BY ticker", connection).ticker.to_list()
        tickers_3 = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks WHERE LENGTH(ticker) = 6 AND ticker LIKE '%%F' ORDER BY ticker", connection).ticker.to_list()
        tickers_4 = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks WHERE LENGTH(ticker) = 7 AND (ticker LIKE '%%11F' OR ticker LIKE '%%34F') ORDER BY ticker", connection).ticker.to_list()
        tickers = tickers_1 + tickers_2 + tickers_3 + tickers_4
        dataf = df.loc[df.ticker.isin(tickers)]
        dataf.to_sql('brazil_stocks', connection, if_exists = 'append', index = False)
        del dataf
        print('brazil_options')
        start_date = read_sql_query("SELECT MAX(date) FROM brazil_options", connection).values[0][0]
        df = df.sort_values('ticker').loc[df.date > start_date]
        df = df.loc[~df.ticker.isin(tickers)]
        # df.sort_values(['ticker', 'date'], inplace = True)
        df.to_sql('brazil_options', connection, if_exists = 'append', index = False)
print('Complete')
connection.close()
engine.dispose()
