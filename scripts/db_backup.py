#%%
from sys import stdout
from datetime import datetime as dt
from pandas import read_sql_query, DataFrame, read_csv, concat
from functions import psqlEngine

db_config = 'database.ini'
database = 'domestic'

#%%
engine = psqlEngine(db_config)
connection = engine.raw_connection()
tickers_backup = read_sql_query("SELECT DISTINCT ticker FROM domestic_backup ORDER BY ticker", connection).ticker.to_list()
# for ticker in tickers[-1:]:
#     cursor = connection.cursor()
#     cursor.execute("DELETE FROM domestic_backup WHERE ticker = '{}'".format(ticker))
#     connection.commit()
connection.close()
engine.dispose()

#%%
engine = psqlEngine(db_config)
connection = engine.raw_connection()
tickers = read_sql_query("SELECT DISTINCT ticker FROM domestic ORDER BY ticker", connection).ticker.to_list()
for k, ticker in enumerate(tickers[len(tickers_backup):]):
    print_string = '{}  {}'.format(len(tickers) - k - 1, ticker)
    stdout.write('\r\x1b[K' + print_string)
    stdout.flush()
    df = read_sql_query("SELECT * FROM domestic WHERE ticker = '{}' ORDER BY date".format(ticker), connection)
    df.to_sql('domestic_backup', engine, if_exists = 'append', index = False)
connection.close()
engine.dispose()

#%%
splits = read_csv('splits.csv')
tickers = splits.ticker.unique()
engine = psqlEngine(db_config)
connection = engine.raw_connection()
cursor = connection.cursor()
for ticker in tickers:
    print(ticker)
    df = read_sql_query("SELECT * FROM {} WHERE ticker = '{}' ORDER BY date".format(database, ticker), connection)
    # print(df)
    dataframe = splits.loc[splits['ticker'] == ticker, ['date', 'numerator', 'denominator']]
    for date, num, den in zip(dataframe.date, dataframe.numerator, dataframe.denominator):
        split = num / den
        df['open'] = [open_p / split for open_p in df.loc[df.date < date, 'open'].to_list()] + df.loc[df.date >= date, 'open'].to_list()
        df['high'] = [high_p / split for high_p in df.loc[df.date < date, 'high'].to_list()] + df.loc[df.date >= date, 'high'].to_list()
        df['low'] = [low_p / split for low_p in df.loc[df.date < date, 'low'].to_list()] + df.loc[df.date >= date, 'low'].to_list()
        df['mean'] = [mean_p / split for mean_p in df.loc[df.date < date, 'mean'].to_list()] + df.loc[df.date >= date, 'mean'].to_list()
        df['close'] = [open_p / split for open_p in df.loc[df.date < date, 'close'].to_list()] + df.loc[df.date >= date, 'close'].to_list()
        cursor.execute("DELETE FROM {} WHERE ticker = '{}'".format(database, ticker))
        connection.commit()
        df.to_sql(database, engine, if_exists = 'append', index = False)
connection.close()
engine.dispose()

#%%
df.loc[(df.date >= '2007-06-01') & (df.date <= '2007-08-01')]

#%%
df = df.loc[df.date >= '2006-01-01']
df.set_index('date', inplace = True)
df.close.plot()