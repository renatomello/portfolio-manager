#%%
from time import sleep
from sqlite3 import connect

from pandas import DataFrame, concat, read_sql_query

from assets import Update_Assets
from investiments import Investments
from functions import psqlEngine
import matplotlib.pyplot as plt

filename = 'database.ini'
key = '12FCWWSQ0N28V8QV'
quandl_key = 'yn5h-QKf33TUxixs2ex2'

#%%
# update = Update_Assets(key = key, database = filename, asset_class = 'usa_stocks')
# update = Update_Assets(key = key, database = filename, asset_class = 'uk_stocks')
# update = Update_Assets(key = key, database = filename, asset_class = 'currencies')

#%%
investments = Investments(start_date = '2020-05-01')
portfolio, portfolio_aggregate = investments('portfolio')
portfolio = portfolio.loc[~(portfolio.quotas == 0.)]
dollar = investments('dollar')
investments('save')
time_series = investments('time_series')
time_series

#%%
x = time_series.index
date_plot = [x[k] for k in range(0, len(x), 11)]
y1 = time_series.return_portfolio
y2 = time_series.return_port_bench

fig, ax = plt.subplots(1, figsize = (8, 5))
fig.tight_layout()
ax.plot(x, y1, label = 'Portfolio')
ax.plot(x, y2, label = '60/40 portfolio')

leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')

ax.set_ylabel('Cumulative Returns (%)')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.show()

#%%
start = 60
x = time_series.index[-start:]
date_plot = [x[k] for k in range(0, len(x), 11)]
y1 = time_series.cagr_portfolio.iloc[-start:]
y2 = time_series.cagr_port_bench.iloc[-start:]

fig, ax = plt.subplots(1, figsize = (8, 5))
fig.tight_layout()
ax.plot(x, y1, label = 'Portfolio')
ax.plot(x, y2, label = '60/40 portfolio')

leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')

ax.set_ylabel('DCF CAGR (%)')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.show()

#%%
x = time_series.index
date_plot = [x[k] for k in range(0, len(x), 24)]
y1 = time_series.portfolio
y2 = time_series.portfolio_invested

fig, ax = plt.subplots(1, figsize = (15, 8))
fig.tight_layout()
ax.plot(x, y2, label = 'Invested', color = 'black')
ax.plot(x, y1, label = 'Portfolio')
ax.fill_between(x, y1, alpha = 0.4)

leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')
ax.set_ylabel('Valuation of Assets (USD)')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.savefig('plot.png', bbox_inches = 'tight')
plt.show()

#%%
my_circle_1 = plt.Circle( (0,0), 0.55, color='white')
my_circle_2 = plt.Circle( (0,0), 0.55, color='white')
my_circle_3 = plt.Circle( (0,0), 0.55, color='white')
my_circle_4 = plt.Circle( (0,0), 0.55, color='white')
my_circle_5 = plt.Circle( (0,0), 0.55, color='white')
my_circle_6 = plt.Circle( (0,0), 0.55, color='white')

#%%
plt.pie(
    x = portfolio_aggregate.value_usd, 
    labels = portfolio_aggregate.asset,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    wedgeprops = { 'linewidth' : 7, 'edgecolor' : 'white' },
    )
p = plt.gcf()
p.gca().add_artist(my_circle_1)
plt.tight_layout()
plt.title('Categories')
plt.show()

fig, ax = plt.subplots(1, 2, figsize = (15, 8))
ax[0].set_title('International Stocks')
ax[0].pie(
    x = portfolio.loc['international stocks'].value_usd, 
    labels = portfolio.loc['international stocks'].asset,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    wedgeprops = { 'linewidth' : 6, 'edgecolor' : 'white' },
    )
p = plt.gcf()
p.gca().add_artist(my_circle_2)

ax[1].set_title('Domestic Stocks')
ax[1].pie(
    x = portfolio.loc['domestic stocks'].value_usd, 
    labels = portfolio.loc['domestic stocks'].asset,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    wedgeprops = { 'linewidth' : 7, 'edgecolor' : 'white' },
    )
p = plt.gcf()
p.gca().add_artist(my_circle_3)
plt.tight_layout()
plt.show()

#%%
growth_international = ['AMZN', 'ARKG', 'ARKK', 'GOOGL', 'IPOC', 'IPOD-U', 'IPOE-U', 'IPOF-U', 'MP', 'OPEN', 'QS', 'SPCE', 'SPOT', 'TSLA', 'WORK']
value_international = ['IBM', 'HON']
value_domestic = ['EGIE3', 'TAEE11', 'TIET11', 'WHRL4']
portfolio_growth_value = portfolio.loc[~(portfolio.asset == 'domestic bonds')]
df = DataFrame()
for index in portfolio_growth_value.index.unique(0):
    barbell = list()
    aux = portfolio_growth_value.loc[index]
    for asset in aux.asset:
        if asset in growth_international:
            barbell.append('growth')
        elif (asset in value_domestic) or (asset in value_international):
            barbell.append('value')
        else:
            barbell.append('others')
    aux['barbell'] = barbell
    df = concat([df, aux])
    del aux
portfolio_growth_value = df
del df

plt.pie(
    x = portfolio_growth_value.groupby('barbell').sum()[['value_usd']], 
    labels = portfolio_growth_value.groupby('barbell').sum()[['value_usd']].index,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    wedgeprops = { 'linewidth' : 5, 'edgecolor' : 'white' },
    )
p = plt.gcf()
p.gca().add_artist(my_circle_4)
plt.title('Barbell Strategy')
plt.tight_layout()
plt.show()

#%%
plt.pie(
    x = portfolio_growth_value.loc[portfolio_growth_value.barbell == 'growth', 'value_usd'], 
    labels = portfolio_growth_value.loc[portfolio_growth_value.barbell == 'growth', 'asset'],
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    wedgeprops = { 'linewidth' : 5, 'edgecolor' : 'white' },
    )
p = plt.gcf()
p.gca().add_artist(my_circle_5)
plt.title('Growth Stocks')
plt.tight_layout()
plt.show()

#%%
plt.pie(
    x = portfolio_growth_value.loc[portfolio_growth_value.barbell == 'value', 'value_usd'], 
    labels = portfolio_growth_value.loc[portfolio_growth_value.barbell == 'value', 'asset'],
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    wedgeprops = { 'linewidth' : 5, 'edgecolor' : 'white' },
    )
p = plt.gcf()
p.gca().add_artist(my_circle_6)
plt.title('Value Stocks')
plt.tight_layout()
plt.show()
