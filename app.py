#%%
from sys import stdout
from time import sleep
from datetime import datetime as dt
from sqlite3 import connect

from pandas import DataFrame, read_sql_query, concat

from assets import Update_Assets
from investiments import Investments
from functions import psqlEngine

import matplotlib.pyplot as plt

filename = 'database.ini'
key = ''

#%%
# Update_Assets(key = key, database = 'international.db')

# connection = connect('database.db')
# tickers = read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", connection).name.to_list()
# connection.close()

# for ticker in tickers:
#     stdout.write('\r\x1b[K' + ticker)
#     stdout.flush()
#     connection = connect('database.db')
#     df = read_sql_query('SELECT * FROM "{}" ORDER BY date'.format(ticker), connection)
#     connection.close()

#     engine = psqlEngine(filename = filename)
#     connection = engine.connect()
#     df.to_sql(ticker, connection, if_exists = 'replace', index = False)
#     connection.close()
#     engine.dispose()

#%%
investments = Investments(start_date = '2020-01-01')
portfolio, portfolio_aggregate = investments('portfolio')
dollar = investments('dollar')
# investments('save')
time_series = investments('time_series')
# time_series

#%%
x = time_series.index
date_plot = [x[k] for k in range(0, len(x), 11)]
y1 = time_series.return_portfolio
y2 = time_series.return_SPY
y3 = time_series.return_BOVA11

fig, ax = plt.subplots(1, figsize = (8, 5))
fig.tight_layout()
ax.plot(x, y1, label = 'Portfolio')
ax.plot(x, y2, label = 'SPY')
ax.plot(x, y3, label = 'BOVA11 (USD)')

leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')

ax.set_ylabel('Cumulative Returns (%)')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.show()

#%%
start = 180
x = time_series.index[-start:]
date_plot = [x[k] for k in range(0, len(x), 11)]
y1 = time_series.cagr_portfolio.iloc[-start:]
y2 = time_series.cagr_SPY.iloc[-start:]
y3 = time_series.cagr_BOVA11.iloc[-start:]

fig, ax = plt.subplots(1, figsize = (8, 5))
fig.tight_layout()
ax.plot(x, y1, label = 'Portfolio')
ax.plot(x, y2, label = 'SPY')
ax.plot(x, y3, label = 'BOVA11 (USD)')

leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')

ax.set_ylabel('DCF CAGR (%)')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.show()

#%%
x = time_series.index
date_plot = [x[k] for k in range(0, len(x), 15)]
y1 = time_series.portfolio
y2 = time_series.portfolio_invested

fig, ax = plt.subplots(1, figsize = (8, 5))
fig.tight_layout()
ax.plot(x, y2, label = 'Invested', color = 'black')
ax.fill_between(x, y1, label = 'Portfolio', alpha = 0.6)

leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')
ax.set_ylabel('Price of Assets (USD)')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.show()

#%%
vector = []
aux = DataFrame({
    'asset': ['domestics funds'],
    'value_brl': vector,
    'value_usd': [item * dollar for item in vector],
})
portfolio_aggregate = concat([portfolio_aggregate, aux])
portfolio_aggregate.sort_values(by = ['value_usd'], ascending = False, inplace = True)
portfolio_aggregate

#%%
plt.pie(
    x = portfolio_aggregate.value_usd, 
    labels = portfolio_aggregate.asset,
    shadow = True,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    )
plt.tight_layout()
plt.show()

#%%
plt.pie(
    x = portfolio.loc['international stocks'].value_usd, 
    labels = portfolio.loc['international stocks'].asset,
    shadow = True,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    )
plt.tight_layout()
plt.show()

#%%
plt.pie(
    x = portfolio.loc['domestic stocks'].value_usd, 
    labels = portfolio.loc['domestic stocks'].asset,
    shadow = True,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    )
plt.tight_layout()
plt.show()
