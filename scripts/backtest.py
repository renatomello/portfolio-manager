#%%
from functools import reduce
from operator import iconcat
from datetime import datetime as dt
from datetime import timedelta
from numpy import nan, cumprod, cumsum, transpose, unique, asarray
from numpy import max as np_max
from numpy import abs as np_abs
from datetime import datetime as dt
from pandas import read_sql_query, read_csv, DataFrame, concat
from secret import db_config
from secret import key_renato as key
from functions import psqlEngine

from alpha_vantage.timeseries import TimeSeries

import matplotlib.pyplot as plt


def strategy(df, threshold: float):
    df.reset_index(inplace = True)
    buy_long, sell_long = list(), list()
    buy_short, sell_short = list(), list()
    for k, mfm in zip(df.dropna().index[:-1], df.dropna().MFM.iloc[:-1]):
        if (mfm <= -threshold) and (mfm <= df.MFM.iloc[k - 1]) and (df.MFM.iloc[k - 1] <= df.MFM.iloc[k - 2]):
            buy_long.append([
                int(k + 1),
                str(df.date.iloc[k + 1]), 
                float(df.ATR.iloc[k + 1]), 
                # float(traded_shares_long * df.avg.iloc[k + 1]), 
                float(traded_shares_long * df.close.iloc[k + 1]), 
                float(traded_shares_long), 
                'buy_long'
            ])
        if (mfm >= threshold) and (mfm >= df.MFM.iloc[k - 1]) and (df.MFM.iloc[k - 1] >= df.MFM.iloc[k - 2]):
            sell_short.append([
                int(k + 1), 
                str(df.date.iloc[k + 1]), 
                float(df.ATR.iloc[k + 1]), 
                # float(-traded_shares_short * df.avg.iloc[k + 1]), 
                float(-traded_shares_short * df.close.iloc[k + 1]), 
                float(-traded_shares_short), 
                'short_sell'
            ])

    buy_long = asarray(buy_long)
    sell_short = asarray(sell_short)

    for row in buy_long:
        for k, row_df in df.loc[df.date > row[1]].iterrows():
            if row_df.high >= float(row[3]) / traded_shares_long + reward * float(row[2]):
                trade_type = 'profit'
                rr = reward
            elif row_df.low <= float(row[3]) / traded_shares_long - risk * float(row[2]):
                trade_type = 'loss'
                rr = -risk
            else:
                trade_type = None
            if trade_type:
                sell_long.append([
                    row[0], 
                    k, 
                    df.date.iloc[k], 
                    -traded_shares_long * (float(row[3]) / traded_shares_long + rr * float(row[2])), 
                    -float(traded_shares_long), 
                    trade_type, 
                    (traded_shares_long * (float(row[3]) / traded_shares_long + rr * float(row[2])) - float(row[3])) / float(row[3])
                ])
                break
    for row in sell_short:
        for k, row_df in df.loc[df.date > row[1]].iterrows():
            if row_df.low <= -float(row[3]) / traded_shares_short - reward * float(row[2]):
                trade_type = 'loss'
                rr = -reward
            elif row_df.high >= -float(row[3]) / traded_shares_short + risk * float(row[2]):
                trade_type = 'profit'
                rr = risk
            else:
                trade_type = None
            if trade_type:
                buy_short.append([
                    row[0], 
                    k, 
                    df.date.iloc[k], 
                    -traded_shares_short * (float(row[3]) / traded_shares_short + rr * float(row[2])), 
                    float(traded_shares_short), 
                    trade_type, 
                    -(traded_shares_short * (float(row[3]) / traded_shares_short + rr * float(row[2])) - float(row[3])) / float(row[3])
                ])
                break
    columns_initial = ['index_0', 'date', 'ATR', 'purchase_price', 'share', 'trade_type']
    columns_final = ['index_0', 'index_1', 'date', 'purchase_price', 'share', 'trade_type', 'returns']
    initial_positions = DataFrame(buy_long, columns = columns_initial)#.append(DataFrame(sell_short, columns = columns_initial))
    initial_positions['purchase_price'] = initial_positions.purchase_price.astype(float)
    initial_positions['share'] = initial_positions.share.astype(float)
    final_positions = DataFrame(sell_long, columns = columns_final)#.append(DataFrame(buy_short, columns = columns_final))
    return initial_positions, final_positions

#%%
tickers = ['AMZN', 'GOOG', 'TWTR', 'JPM', 'GS', 'IBM']#, 'VALE', 'DELL', 'F', 'INTC', 'MMM']
dictionary = dict()
engine = psqlEngine(db_config)
connection = engine.connect()
for ticker in tickers:
    print(ticker)
    ticker = ticker.upper()
    df = read_sql_query("SELECT time as date, open, high, low, close, volume FROM usa_stocks_intraday_15min WHERE ticker = '{}' ORDER BY date".format(ticker.upper()), connection)
    # df = read_sql_query("SELECT date, adjusted_open as open, adjusted_high as high, adjusted_low as low, adjusted_mean as avg, adjusted_close as close, volume FROM brazil_stocks WHERE ticker = '{}' ORDER BY date".format(ticker.upper()), connection)
    dictionary[ticker.lower()] = df
connection.close()
engine.dispose()

df = concat(dictionary)
df.reset_index(inplace = True)
df.drop(columns = 'level_1', inplace = True)
df.rename(columns = {'level_0': 'ticker'}, inplace = True)
df.set_index('ticker', inplace =  True)
df.sort_values(by = ['ticker', 'date'], inplace = True)
min_dates = dict()
for ticker in df.index.unique():
    min_dates[ticker] = [df.loc[df.index == ticker].date.min()]
start_date = DataFrame(min_dates).max(axis = 1).values[0]
df = df.loc[df.date >= start_date]
df.reset_index(inplace = True)
df['open'] = df.open.astype('float')
df['high'] = df.high.astype('float')
df['low'] = df.low.astype('float')
# df['avg'] = df.avg.astype('float')
df['close'] = df.close.astype('float')
df['volume'] = df.volume.astype('float')

# df = df.loc[(df.date >= '2011-01-01') & (df.date <= '2020-02-01')]
# #%%
# ticker = tickers[0]
# dates = df.loc[df['ticker'] == ticker, 'date'].to_list()
# close = df.loc[df['ticker'] == ticker, 'close'].astype('float').to_list()
# ticks = [dates[k] for k in range(0, len(dates), 25)]
# plt.figure(figsize=(10, 6))
# plt.plot(dates, close)
# plt.xticks(ticks, rotation = '90')
# plt.tight_layout()
# plt.show()

mf, mfm, tr, atr = list(), list(), list(), list()
for ticker in df['ticker'].unique():
    dataframe = df.loc[df['ticker'] == ticker]
    denominator = (dataframe.high - dataframe.low)
    denominator.replace(0, 1e-3, inplace = True)
    mf.append((dataframe.volume * (dataframe.high + dataframe.low + dataframe.close) / 3).to_list())
    mfm.append((100 * ((dataframe.close - dataframe.low) - (dataframe.high - dataframe.close)) / denominator).to_list())
    true_range = [
        (dataframe.high - dataframe.low).to_list(),
        np_abs((dataframe.high - dataframe.close.shift(periods = 1)).to_list()),
        np_abs(dataframe.low - dataframe.close.shift(periods = 1)).to_list(),
    ]
    true_range = transpose(true_range)
    dataframe_2 = DataFrame.from_records(true_range)
    dataframe_2['TR'] = dataframe_2.max(axis = 1)
    tr.append(dataframe_2.max(axis = 1).to_list())
    atr.append(dataframe_2.TR.rolling(window = 50, win_type = 'exponential').mean(tau = 10, std = 1).to_list())
df['MF'] = reduce(iconcat, mf, [])
df['MFM'] = reduce(iconcat, mfm, [])
df['TR'] = reduce(iconcat, tr, [])
df['ATR'] = reduce(iconcat, atr, [])
#%%
threshold = 90
purchase_price, shares = list(), list()
dates, times = list(), list()
MFMs = list()
traded_shares_long = 1
traded_shares_short = traded_shares_long / 1
risk, reward = 1, 1
dates, plots = list(), list()
dataframes = DataFrame({'date': df.date})

initial_dict, final_dict = dict(), dict()
for ticker in df['ticker'].unique():
    print(ticker)
    dataframe = df.loc[df['ticker'] == ticker]
    initial, final = strategy(dataframe, threshold)
    initial_dict[ticker] = initial
    final_dict[ticker] = final
initial_positions = concat(initial_dict)
final_positions = concat(final_dict)

print("{:.3f}".format(len(final_positions.loc[final_positions.trade_type == 'profit', 'trade_type']) / len(final_positions)))

test_2 = final_positions.groupby('date').sum().reset_index()

dates = test_2.date.to_list()
ticks = [test_2.date.iloc[k] for k in range(0, len(test_2), 25)]
plot_1 = [1000] + list(1000 * (1 + test_2.returns.iloc[1:]).fillna(1).cumprod())
plot_2 = [0] + list(100 * ((1 + test_2.returns.iloc[1:]).fillna(1).cumprod() - 1))
cagr = [0] + [(y / 1000) ** (252 / x) - 1 for x, y in enumerate(plot_1[1:], 1)]

fig, (ax1, ax2) = plt.subplots(figsize=(10, 8), nrows = 2, sharex = True)

ax1.plot(dates, plot_1)
ax2.plot(dates, plot_2, label = 'CAGR = {:.2f}%'.format(cagr[-1]))

ax1.set_ylabel('Cumulative Portfolio (R$)')
ax2.set_ylabel('Cumulative Returns (%)')

plt.xticks(ticks, rotation = '90')
plt.legend()
plt.tight_layout()
plt.show()
