#%%
from time import sleep
from datetime import datetime as dt
from sqlite3 import connect

from pandas import DataFrame, read_sql_query, concat

from assets import Update_Assets
from investiments import Investments

import matplotlib.pyplot as plt

import yfinance as yf

key = '12FCWWSQ0N28V8QV'

Update_Assets(key = key)

#%%
investments = Investments()
portfolio, portfolio_aggregate = investments('portfolio')
dollar = investments('dollar')
investments('save')
time_series = investments('time_series')
time_series

#%%
x = time_series.index
date_plot = [x[k] for k in range(0, len(x), 11)]
y1 = time_series.return_position
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
x = time_series.index
date_plot = [x[k] for k in range(0, len(x), 11)]
y1 = time_series.cagr_position
y2 = time_series.cagr_SPY
y3 = time_series.cagr_BOVA11

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
date_plot = [x[k] for k in range(0, len(x), 8)]
y1 = time_series.position
y2 = time_series.SPY
y3 = time_series.BOVA11

fig, ax = plt.subplots(1, figsize = (8, 5))
fig.tight_layout()
ax.plot(x, y1, label = 'Portfolio')

leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')
ax.set_ylabel('Price of Assets (USD)')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.show()

#%%
# vector = [4308.83]
# aux = DataFrame({
#     # 'asset': ['domestic stocks', 'domestics funds'],
#     'asset': ['domestics funds'],
#     'value_brl': vector,
#     'value_usd': [item * dollar for item in vector],
# })
# portfolio_aggregate = concat([portfolio_aggregate, aux])
# portfolio_aggregate.sort_values(by = ['value_usd'], ascending = False, inplace = True)
# portfolio_aggregate

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
    x = portfolio.loc['domestic_stocks'].value_usd, 
    labels = portfolio.loc['domestic_stocks'].asset,
    shadow = True,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    )
plt.tight_layout()
plt.show()
