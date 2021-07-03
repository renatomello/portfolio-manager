#%%
from datetime import date, datetime as dt
from os import read

from pandas import DataFrame, concat, read_sql_query, read_csv

from secret import db_config, key_felipe as key

from assets import Update_Assets
from investiments import Investments

import matplotlib.pyplot as plt

from functions import psqlEngine

# #%%
# def row_count(input):
#     with open(input) as f:
#         for i, l in enumerate(f):
#             pass
#     return i

# table = '/home/renato/Desktop/usa_stocks_intraday_5min_A-M.csv'
# end = row_count(table)
# end

# #%%
# engine = psqlEngine(db_config)
# connection = engine.raw_connection()

# # table = 'usa_stocks_intraday_5min_A-M'
# # df = read_csv('/home/renato/Desktop/{}.csv'.format(table), skiprows = [1000000, end])
# df = read_sql_query('SELECT DISTINCT ticker FROM currencies ORDER BY ticker', connection).ticker.to_list()
# # df.to_sql(table.replace('_A-M', '').replace('_N-Z', ''), engine, if_exists = 'replace', index = False)

# connection.close()
# engine.dispose()
# # update = Update_Assets(key = key, database = db_config, asset_class = 'usa_stocks', asset = df[40:])
# df[22:]

#%%
# update = Update_Assets(key = key, database = db_config, asset_class = 'usa_stocks')
# update = Update_Assets(key = key, database = db_config, asset_class = 'uk_stocks')
# update = Update_Assets(key = key, database = db_config, asset_class = 'currencies')
# update = Update_Assets(key = key, database = db_config, asset_class = 'benchmarks')

#%%
investments = Investments(start_date = '2020-04-01')
portfolio, portfolio_aggregate = investments('portfolio')
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
ax.plot(x, y1, label = 'Portfolio - CAGR = {:.1f}%'.format(time_series.cagr_portfolio.iloc[-1]))
ax.plot(x, y2, label = '60/40 portfolio - CAGR = {:.1f}%'.format(time_series.cagr_port_bench.iloc[-1]))

leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')

ax.set_ylabel('Cumulative Returns (%)')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.show()

# #%%
# start = 60
# x = time_series.index[-start:]
# date_plot = [x[k] for k in range(0, len(x), 11)]
# y1 = time_series.cagr_portfolio.iloc[-start:]
# y2 = time_series.cagr_port_bench.iloc[-start:]

# fig, ax = plt.subplots(1, figsize = (8, 5))
# fig.tight_layout()
# ax.plot(x, y1, label = 'Portfolio')
# ax.plot(x, y2, label = '60/40 portfolio')

# leg = plt.legend(loc = 'upper left', frameon = False)
# for text in leg.get_texts():
#     plt.setp(text, color = 'black')

# ax.set_ylabel('DCF CAGR (%)')
# plt.xticks(date_plot)
# plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
# plt.show()

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
# plt.savefig('plot.png', bbox_inches = 'tight')
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
# try:
#     ax[1].set_title('Domestic Stocks')
#     ax[1].pie(
#         x = portfolio.loc['domestic stocks'].value_usd, 
#         labels = portfolio.loc['domestic stocks'].asset,
#         autopct = '%1.1f%%',
#         pctdistance = 0.80,
#         startangle = 90,
#         wedgeprops = { 'linewidth' : 7, 'edgecolor' : 'white' },
#         )
#     p = plt.gcf()
#     p.gca().add_artist(my_circle_3)
# except:
#     pass
ax[1].set_title('Crypto assets')
ax[1].pie(
    x = portfolio.loc['crypto'].value_usd,
    labels = portfolio.loc['crypto'].asset,
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
growth_international = ['AMZN', 'API', 'ARKF', 'ARKG', 'ARKK', 'dataf', 'GOOGL', 'IPOD-U', 'IPOE-U', 'IPOF-U', 'MP', 'OPEN', 'QS', 'SPCE', 'SPOT', 'TSLA']
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
    # aux['barbell'] = barbell
    portfolio_growth_value.loc[index, 'barbell'] = barbell
    # df = concat([df, aux])
    # del aux
# portfolio_growth_value = df
# del df

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
portfolio_growth_value

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
