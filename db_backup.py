#%%
from sys import stdout
from pandas import read_sql_query, DataFrame
from functions import psqlEngine

db_config = 'database.ini'

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
