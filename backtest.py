#%%
from numpy import nan, cumprod
from datetime import datetime as dt
from pandas import read_sql_query, read_csv, DataFrame
from functions import psqlEngine

from alpha_vantage.timeseries import TimeSeries

import matplotlib.pyplot as plt

db_config = 'database.ini'
key = '12FCWWSQ0N28V8QV'

#%%
def get_returns(df, reference, flag = 'cumulative'):
    df.reset_index(inplace = True)
    reference.reset_index(inplace = True)
    returns = list()
    if flag == 'cumulative':
        for date in df.datetime.iloc[1:]:
            end = df.loc[df.datetime == date, 'portfolio'].index[0]
            start = end - 1
            if date not in reference['datetime'].to_list():
                retorno = (df.portfolio.iloc[end] - df.portfolio.iloc[start]) / df.portfolio.iloc[start]
                returns.append(retorno)
            if date in reference['datetime'].to_list():
                cash_flow = reference.loc[reference.datetime == date, 'purchase_price'].iloc[0]
                retorno = (df.portfolio.iloc[end] - (df.portfolio.iloc[start] + cash_flow)) / (df.portfolio.iloc[start] + cash_flow)
                returns.append(retorno)
        returns = [0] + returns
        returns = list(map(lambda x: x + 1, returns))
        returns = 100 * (cumprod(returns) - 1)
    # if flag == 'cagr':
    #     for date in df['date'].iloc[1:]:
    #         end = df.loc[df.date == date, 'portfolio'].index[0]
    #         start = df.index[1]
    #         exponent = 365 / (end - start)
    #         cash_flow = reference.loc[(reference.date >= start_date) & (reference.date <= date), 'purchase_price'].sum()
    #         retorno = 100 * (((df.portfolio.iloc[end] / (df.portfolio.iloc[start] + cash_flow)) ** exponent) - 1)
    #         returns.append(retorno)
    #     returns = [0] + returns
    return returns

#%%
link = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={}&interval={}&slice={}&apikey={}'

#%%
df = read_csv(link.format('GOOG', '15min', 'year1month1', key))
df['date'] = [dt.strptime(date, '%Y-%m-%d %H:%M:%S').date().strftime('%Y-%m-%d') for date in df.time]
df['time'] = [dt.strptime(date, '%Y-%m-%d %H:%M:%S').time().strftime('%H:%M:%S') for date in df.time]
df = df.loc[(df.time >= '09:30:00') & (df.time <= '16:00:00')]
df

#%%
df.close.plot()
#%%
# engine = psqlEngine(db_config)
# connection = engine.connect()
# df = read_sql_query("SELECT date, open, high, low, close, volume FROM domestic WHERE ticker LIKE '%%PETR4%%' ORDER BY date", connection)
# connection.close()
# engine.dispose()

# ts = TimeSeries(key = key)#, output_format = 'pandas')
# df, _ = ts.get_intraday_extended(symbol = 'MSFT', interval = '15min', slice = 'year1month12')
# df
# df = df.loc[df.date >= '2020-10-01']
# df.set_index('date', inplace = True)
denominator = (df.high - df.low)
denominator.replace(0, 1e-4, inplace = True)
df['MFM'] = 100 * ((df.close - df.low) - (df.high - df.close)) / denominator
df
#%%
threshold = 90
buy, sell = list(), list()
buy_plot, sell_plot = list(), list()
for k, (first, second, third) in enumerate(zip(df.MFM.iloc[:-2], df.MFM.iloc[:-1], df.MFM.iloc[2:])):
    if (third <= -threshold) and (first >= second) and (second >= third):
        buy.append(third)
        buy_plot.append(df.close.iloc[k])
        sell.append(0)
        sell_plot.append(0)
    elif (third >= threshold) and (first <= second) and (second <= third):
        buy.append(0)
        buy_plot.append(0)
        sell.append(third)
        sell_plot.append(df.close.iloc[k])
    else:
        buy.append(0)
        buy_plot.append(0)
        sell.append(0)
        sell_plot.append(0)

buysell = DataFrame({
    'date': df.date.iloc[2:].to_list(),
    'time': df.time.iloc[2:].to_list(),
    'datetime': [date + ' ' +  time for date, time in zip(df.date.iloc[2:].to_list(), df.time.iloc[2:].to_list())],
    'MFM': df.MFM.iloc[2:],
    'buy': buy,
    'sell': sell,
    'buy_plot': buy_plot,
    'sell_plot': sell_plot,
})
buysell['purchase_price'] = buysell.buy_plot - buysell.sell_plot
diff = list()
for k, d in enumerate(buysell.purchase_price):
    # if k == 0:
    #     diff.append(1)
    # else:
    if d > 0:
        diff.append(0.01)
    if d < 0:
        diff.append(-0.01)
    if d == 0:
        diff.append(0.)
buysell['shares'] = diff
buysell['portfolio'] = [close * price for close, price in zip(buysell['purchase_price'], buysell['shares'])]
ticks = [buysell.datetime.iloc[k] for k in range(1, len(buysell), 15)]
buysell

#%%
get_returns(
    buysell.loc[buysell.purchase_price != 0][['datetime', 'purchase_price', 'shares', 'portfolio']],
    buysell.loc[buysell.shares != 0][['datetime', 'purchase_price', 'shares', 'portfolio']],
)

#%%
plt.plot(buysell.datetime, buysell.MFM)
plt.scatter(buysell.datetime.loc[buysell.buy != 0], buysell.buy.loc[buysell.buy != 0], color = 'g')
plt.scatter(buysell.datetime.loc[buysell.sell != 0], buysell.sell.loc[buysell.sell != 0], color = 'r')
plt.xticks(ticks)
plt.show()

#%%
plt.plot(buysell.datetime, df.close.iloc[2:])
plt.scatter(buysell.datetime.loc[buysell.buy_plot != 0], buysell.buy_plot.loc[buysell.buy_plot != 0], color = 'g')
plt.scatter(buysell.datetime.loc[buysell.sell_plot != 0], buysell.sell_plot.loc[buysell.sell_plot != 0], color = 'r')
plt.xticks(ticks)
plt.show()
