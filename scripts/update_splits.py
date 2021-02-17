#%%
from os import read
from sys import stdout
from numpy.lib.shape_base import split
from pandas import read_sql_query, read_csv
from pandas.core.frame import DataFrame
from secret import db_config
from functions import psqlEngine

#%%
engine = psqlEngine(db_config)
connection = engine.raw_connection()
cursor = connection.cursor()
# cursor.execute("DROP TABLE brazil_stocks_3")
cursor.execute("DROP TABLE brazil_stocks")
cursor.execute("DROP TABLE brazil_stocks_2")
cursor.execute("ALTER TABLE brazil_stocks_3 RENAME TO brazil_stocks")
connection.commit()
# df = read_sql_query('SELECT date, ticker, open, high, low, mean, close, split_numerator, split_denominator FROM brazil_stocks ORDER BY ticker, date', connection)
# df = read_sql_query('SELECT date, ticker, split_numerator, split_denominator FROM brazil_stocks ORDER BY ticker, date', connection)
# tickers = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks ORDER BY ticker", connection).ticker.to_list()
# df = read_sql_query("SELECT COUNT(*) FROM brazil_stocks", connection)
# df = read_sql_query('SELECT * FROM brazil_stocks', connection)
# tickers_1 = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks WHERE LENGTH(ticker) <= 5 ORDER BY ticker", connection).ticker.to_list()
# tickers_2 = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks WHERE LENGTH(ticker) = 6 AND (ticker LIKE '%%11' OR ticker LIKE '%%34') ORDER BY ticker", connection).ticker.to_list()
# tickers_3 = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks WHERE LENGTH(ticker) = 6 AND ticker LIKE '%%F' ORDER BY ticker", connection).ticker.to_list()
# tickers_4 = read_sql_query("SELECT DISTINCT ticker FROM brazil_stocks WHERE LENGTH(ticker) = 7 AND (ticker LIKE '%%11F' OR ticker LIKE '%%34F') ORDER BY ticker", connection).ticker.to_list()
# tickers = tickers_1 + tickers_2 + tickers_3 + tickers_4
# dataf = df.loc[df.ticker.isin(tickers)]
# dataf.to_sql('brazil_stocks_2', connection, if_exists = 'append', index = False)
connection.close()
engine.dispose()
# df['split_denominator'] = [1.]*len(df)
# df['split_numerator'] = [1.]*len(df)

#%%
splits = read_csv('../support_data/splits.csv')

engine = psqlEngine(db_config)
connection = engine.raw_connection()
for ticker in tickers:
    df = read_sql_query("SELECT * FROM brazil_stocks WHERE ticker = '{}' ORDER BY date".format(ticker), connection)
    for row in splits.loc[splits.ticker == ticker].iterrows():
        print(row[1].date, row[1].ticker)
        df.loc[(df.ticker == row[1].ticker) & (df.date <= row[1].date), 'split_numerator'] = [float(x) * row[1].numerator for x in df.loc[(df.ticker == row[1].ticker) & (df.date <= row[1].date), 'split_numerator']]
        df.loc[(df.ticker == row[1].ticker) & (df.date <= row[1].date), 'split_denominator'] = [float(x) * row[1].denominator for x in df.loc[(df.ticker == row[1].ticker) & (df.date <= row[1].date), 'split_denominator']]
        df['adjusted_open'] = df.open.astype('float') / (df.split_numerator.astype('float') / df.split_denominator.astype('float'))
        df['adjusted_high'] = df.high.astype('float') / (df.split_numerator.astype('float') / df.split_denominator.astype('float'))
        df['adjusted_low'] = df.low.astype('float') / (df.split_numerator.astype('float') / df.split_denominator.astype('float'))
        df['adjusted_mean'] = df['mean'].astype('float') / (df.split_numerator.astype('float') / df.split_denominator.astype('float'))
        df['adjusted_close'] = df.close.astype('float') / (df.split_numerator.astype('float') / df.split_denominator.astype('float'))
    df.to_sql('brazil_stocks_3', engine, if_exists = 'append', index = False)
connection.close()
engine.dispose()

#%%
df

#%%
engine = psqlEngine(db_config)
connection = engine.raw_connection()
cursor = connection.cursor()
cursor.executemany("UPDATE brazil_stocks_2 SET split_numerator = '{}' WHERE ticker = '{}' AND date = '{}' ", \
    ((numerator, ticker, date) for numerator, ticker, date in zip(df.split_numerator, df.ticker, df.date)))
connection.commit()
# df = read_sql_query("SELECT * FROM brazil_stocks_2", connection)
# df['adjusted_close'] = df.close.astype('float') / (df.split_numerator / df.split_denominator)
# df.to_sql('brazil_stocks_2', connection, if_exists = 'append', index = False)
connection.close()
engine.dispose()

#%%
engine = psqlEngine(db_config)
connection = engine.connect()
df.to_sql('brazil_stocks_2', connection, if_exists = 'append', index = False)
connection.close()
engine.dispose()
