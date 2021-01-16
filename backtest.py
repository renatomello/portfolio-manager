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
from functions import psqlEngine

from investiments_copy import Investments

from alpha_vantage.timeseries import TimeSeries

import matplotlib.pyplot as plt

db_config = 'database.ini'
key = '12FCWWSQ0N28V8QV'
binance_api_key = 'Y6Db2xbHp4YTi7deLSczDCBQ7kb3UufPqYpJsjy5bkg18yqRhZJzwZIe61E6oapy'
binance_api_secret = 'FGGnKdkClmdWKXA2mGTqHZ7MsrZMfuu3d16aNTNuODREA5bhvPOknke1PiRt9IgO'

# #%%
# def get_returns(df, reference, flag = 'cumulative'):
#     df.reset_index(inplace = True)
#     reference.reset_index(inplace = True)
#     returns = list()
#     if flag == 'cumulative':
#         for date in df.date.iloc[1:]:
#             end = df.loc[df.date == date, 'portfolio'].index[0]
#             start = end - 1
#             if date not in reference['date'].to_list():
#                 retorno = (df.portfolio.iloc[end] - df.portfolio.iloc[start]) / df.portfolio.iloc[start]
#                 returns.append(retorno)
#             if date in reference['date'].to_list():
#                 cash_flow = reference.loc[reference.date == date, 'purchase_price'].iloc[0]
#                 retorno = (df.portfolio.iloc[end] - (df.portfolio.iloc[start] + cash_flow)) / (df.portfolio.iloc[start] + cash_flow)
#                 returns.append(retorno)
#         returns = [0] + returns
#         returns = list(map(lambda x: x + 1, returns))
#         returns = 100 * (cumprod(returns) - 1)
#     return returns

# #%%
#link = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={}&interval={}&slice={}&apikey={}'

# #%%
# df = DataFrame()
# for k in range(3, 0, -1):
#     dataframe = read_csv(link.format('AAPL', '15min', 'year1month{}'.format(k), key))
#     df = df.append(dataframe)
# df.rename(columns = {'time': 'datetime'}, inplace = True)
# df.sort_values(by = ['datetime'], ascending = True, inplace = True)
# df.reset_index(inplace= True)
# df['date'] = [dt.strptime(date, '%Y-%m-%d %H:%M:%S').date().strftime('%Y-%m-%d') for date in df.datetime]
# df['time'] = [dt.strptime(date, '%Y-%m-%d %H:%M:%S').time().strftime('%H:%M:%S') for date in df.datetime]
# df = df.loc[(df.time >= '09:30:00') & (df.time <= '16:00:00')]
# denominator = (df.high - df.low)
# denominator.replace(0, 1e-4, inplace = True)
# df['MFM'] = 100 * ((df.close - df.low) - (df.high - df.close)) / denominator
# df.close.plot()

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
                float(traded_shares_long * df.avg.iloc[k + 1]), 
                float(traded_shares_long), 
                'buy_long'
            ])
        if (mfm >= threshold) and (mfm >= df.MFM.iloc[k - 1]) and (df.MFM.iloc[k - 1] >= df.MFM.iloc[k - 2]):
            sell_short.append([
                int(k + 1), 
                str(df.date.iloc[k + 1]), 
                float(df.ATR.iloc[k + 1]), 
                float(-traded_shares_short * df.avg.iloc[k + 1]), 
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
    initial_positions = DataFrame(buy_long, columns = columns_initial).append(DataFrame(sell_short, columns = columns_initial))
    initial_positions['purchase_price'] = initial_positions.purchase_price.astype('float64')
    initial_positions['share'] = initial_positions.share.astype('float64')
    final_positions = DataFrame(sell_long, columns = columns_final).append(DataFrame(buy_short, columns = columns_final))
    return initial_positions, final_positions

ticker_splits = [
    'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'ITSA4', 'TAEE11', 
    'BBDC4', 'BBDC4', 'BBDC4', 'BBDC4', 'BBDC4', 'BBDC4', 'BBDC4', 'BBDC4', 'BBDC4', 'WEGE3', 'WEGE3', 'WEGE3', 'MGLU3', 
    'MGLU3', 'MGLU3', 'MGLU3', 'EGIE3', 'CIEL3', 'CIEL3', 'CIEL3','CIEL3', 'CIEL3', 'CIEL3', 
]
splits = [
    1.1, 1.1, 1.1, 1.008, 1.004, 1.1, 1.002, 1.1, 1.1, 1.007, 1.011, 1.1, 3, 1.1, 1.2, 1.005, 1.1, 1.1, 1.1, 1.1, 1.2,
    1.1, 1.3, 2, 1.3, 1./8, 8, 8, 4, 1.25, 1.2, 1.2, 2, 1.2, 1.2, 1.2
]
dates_split = [
    '2012-04-26', '2012-04-27', '2013-05-02', '2013-05-08', '2014-02-19', '2014-05-05', '2015-02-11' ,'2015-05-05', 
    '2016-05-02', '2017-02-21', '2018-02-23', '2018-06-01', '2012-12-05', '2013-03-26', '2015-03-27', '2015-12-18',
    '2016-04-18', '2017-05-02', '2018-03-28', '2018-04-02', '2019-04-01', '2020-04-14', '2014-04-24', '2015-04-01',
    '2018-04-25', '2015-10-01', '2017-09-05', '2019-08-06', '2020-10-14', '2018-12-12', '2012-04-23', '2013-04-29',
    '2014-04-01', '2015-04-13', '2016-04-11', '2017-04-13', 
]

df_splits = DataFrame({
    'date': dates_split,
    'ticker': ticker_splits,
    'split': splits,
})

#%%
tickers = ['bova11', 'petr4', 'vale3', 'itsa4', 'taee11', 'whrl3',  'bbas3', 'bbdc4', 'wege3', 'mglu3', 'egie3', 'ciel3']
currency = 'BTCUSD'
dictionary = dict()
engine = psqlEngine(db_config)
connection = engine.connect()
for ticker in tickers:
    ticker = ticker.upper()
    # df = read_sql_query("SELECT date, open, high, low, close, volume FROM currencies WHERE ticker = '{}' ORDER BY date".format(currency.upper()), connection)
    df = read_sql_query("SELECT date, open, high, low, mean as avg, close, volume FROM domestic WHERE ticker = '{}' ORDER BY date".format(ticker.upper()), connection)
    for date, split in zip(df_splits.loc[df_splits['ticker'] == ticker, 'date'], df_splits.loc[df_splits['ticker'] == ticker, 'split']):
        df['open'] = [open_p / split for open_p in df.loc[df.date < date, 'open'].to_list()] + df.loc[df.date >= date, 'open'].to_list()
        df['high'] = [high_p / split for high_p in df.loc[df.date < date, 'high'].to_list()] + df.loc[df.date >= date, 'high'].to_list()
        df['low'] = [low_p / split for low_p in df.loc[df.date < date, 'low'].to_list()] + df.loc[df.date >= date, 'low'].to_list()
        df['avg'] = [mean_p / split for mean_p in df.loc[df.date < date, 'avg'].to_list()] + df.loc[df.date >= date, 'avg'].to_list()
        df['close'] = [open_p / split for open_p in df.loc[df.date < date, 'close'].to_list()] + df.loc[df.date >= date, 'close'].to_list()
        # df['quatneg'] = [quant * split for quant in df.loc[df.date < date, 'quatneg'].to_list()] + df.loc[df.date >= date, 'quatneg'].to_list()
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

#%%
ticker = tickers[-1]
dates = df.loc[df['ticker'] == ticker, 'date'].to_list()
close = df.loc[df['ticker'] == ticker, 'close'].to_list()
ticks = [dates[k] for k in range(0, len(dates), 150)]
plt.figure(figsize=(10, 6))
plt.plot(dates, close)
plt.xticks(ticks, rotation = '90')
plt.tight_layout()
plt.show()

#%%
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
    # dataframe['ATR'] = dataframe.TR.rolling(window = 50).mean()
    tr.append(dataframe_2.max(axis = 1).to_list())
    atr.append(dataframe_2.TR.rolling(window = 50, win_type = 'exponential').mean(tau = 10, std = 1).to_list())
df['MF'] = reduce(iconcat, mf, [])
df['MFM'] = reduce(iconcat, mfm, [])
df['TR'] = reduce(iconcat, tr, [])
df['ATR'] = reduce(iconcat, atr, [])
df

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
    dataframe = df.loc[df['ticker'] == ticker]
    initial, final = strategy(dataframe, threshold)
    initial_dict[ticker] = initial
    final_dict[ticker] = final
initial_positions = concat(initial_dict)
final_positions = concat(final_dict)

#%%
"{:.3f}".format(len(final_positions.loc[final_positions.trade_type == 'profit', 'trade_type']) / len(final_positions))

#%%
test_2 = final_positions.groupby('date').sum().reset_index()

dates = test_2.date.to_list()
plots = [0] + list(100 * ((1 + test_2.returns.iloc[1:]).fillna(1).cumprod() - 1))

plt.figure(figsize=(10, 6))
plt.plot(dates, plots)
ticks = [test_2.date.iloc[k] for k in range(0, len(test_2), 30)]
plt.title('Full Portfolio')
plt.ylabel('Cumulative Returns (%)')
plt.xticks(ticks, rotation = '90')
plt.tight_layout()
plt.show()
# ax = test_2[['date', 'returns']].set_index('date').plot(figsize = (10,5), title = 'Portfolio Returns')
# ax.set_ylabel('Cumulative Returns (%)')

#%%
(final_positions.trade_type.loc[final_positions.trade_type == 'profit'].count() / len(final_positions)).round(3)

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




#%%
# true_range = [
#     (df.high - df.low).to_list(),
#     np_abs((df.high - df.close.shift(periods = 1)).to_list()),
#     np_abs(df.low - df.close.shift(periods = 1)).to_list(),
# ]
# true_range = transpose(true_range)
# dataframe = DataFrame.from_records(true_range)
# dataframe['TR'] = dataframe.max(axis = 1)
# # dataframe['ATR'] = dataframe.TR.rolling(window = 50).mean()
# df['ATR'] = dataframe.TR.rolling(window = 50, win_type = 'exponential').mean(tau = 10, std = 1).to_list()
# df.reset_index(inplace = True)
# df.drop(columns = ['index'], inplace = True)
# df
# #%%

# if 'avg' not in df.columns:
#     df['avg'] = (df.open + df.close) / 2
#     plot_title = currency.upper()
# else:
#     plot_title = ticker.upper()
# # df = df.loc[df.date >= '2019-01-01']

# denominator = (df.high - df.low)
# denominator.replace(0, 1e-3, inplace = True)
# df['MF'] = df.volume * (df.high + df.low + df.close) / 3
# df['MFM'] = 100 * ((df.close - df.low) - (df.high - df.close)) / denominator

# # #%%
# # ax = df.loc[df.date >= '2020-01-01'].set_index('date').MFM.plot(figsize = (10, 5))
# # ax
# # #%%
# true_range = [
#     (df.high - df.low).to_list(),
#     np_abs((df.high - df.close.shift(periods = 1)).to_list()),
#     np_abs(df.low - df.close.shift(periods = 1)).to_list(),
# ]
# true_range = transpose(true_range)
# dataframe = DataFrame.from_records(true_range)
# dataframe['TR'] = dataframe.max(axis = 1)
# # dataframe['ATR'] = dataframe.TR.rolling(window = 50).mean()
# df['ATR'] = dataframe.TR.rolling(window = 50, win_type = 'exponential').mean(tau = 10, std = 1).to_list()
# df.reset_index(inplace = True)
# df.drop(columns = ['index'], inplace = True)

threshold = 90
purchase_price, shares = list(), list()
dates, times = list(), list()
MFMs = list()
traded_shares_long = 1
traded_shares_short = traded_shares_long / 1
risk, reward = 1, 1

dates, plots = list(), list()
dataframes = DataFrame({'date': df.date})

for reward in range(1, 10):

    buy_long, sell_long = list(), list()
    buy_short, sell_short = list(), list()
    for k, mfm in zip(df.dropna().index[:-1], df.dropna().MFM.iloc[:-1]):
        if (mfm <= -threshold) and (mfm <= df.MFM.iloc[k - 1]) and (df.MFM.iloc[k - 1] <= df.MFM.iloc[k - 2]):
            buy_long.append([
                int(k + 1),
                str(df.date.iloc[k + 1]), 
                float(df.ATR.iloc[k + 1]), 
                float(traded_shares_long * df.avg.iloc[k + 1]), 
                float(traded_shares_long), 
                'buy_long'
            ])
        if (mfm >= threshold) and (mfm >= df.MFM.iloc[k - 1]) and (df.MFM.iloc[k - 1] >= df.MFM.iloc[k - 2]):
            sell_short.append([
                int(k + 1), 
                str(df.date.iloc[k + 1]), 
                float(df.ATR.iloc[k + 1]), 
                float(-traded_shares_short * df.avg.iloc[k + 1]), 
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
            # print('{:.2f}  {:.2f}  {:.2f}  {:.2f}'.format(row_df.low, -float(row[3]) / traded_shares_short - reward * float(row[2]), row_df.high, -float(row[3]) / traded_shares_short + risk * float(row[2])))
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
    initial_positions = DataFrame(buy_long, columns = columns_initial).append(DataFrame(sell_short, columns = columns_initial))
    # initial_positions = DataFrame(sell_short, columns = columns_initial)
    initial_positions['purchase_price'] = initial_positions.purchase_price.astype('float64')
    initial_positions['share'] = initial_positions.share.astype('float64')
    final_positions = DataFrame(sell_long, columns = columns_final).append(DataFrame(buy_short, columns = columns_final))
    # final_positions = DataFrame(buy_short, columns = columns_final)

    test = initial_positions.append(final_positions)[['date', 'purchase_price', 'share']]
    test.sort_values(by = 'date', inplace = True)
# test.to_csv('investments_backtest/stocks/domestic/{}.csv'.format(ticker.lower()), index = False)

    test_2 = final_positions.groupby('date').sum().reset_index()

    dates = test_2.date.to_list()
    plots = [0] + list(100 * ((1 + test_2.returns.iloc[1:]).fillna(1).cumprod() - 1))

    test_3 = DataFrame({'date': dates, 'risk:rew = 1:{}'.format(reward): plots})
    dataframes = dataframes.merge(test_3, on='date')

ax = dataframes.set_index('date').plot(figsize = (10,5), title = plot_title)
ax.set_ylabel('Cumulative Returns (%)')


# test_2 = final_positions.groupby('date').sum().reset_index()
# test_2['cum_return'] = [0] + list(100 * ((1 + test_2.returns.iloc[1:]).fillna(1).cumprod() - 1)) 
# test_2['cagr_return'] = [0] + [100 * ((item / test_2.purchase_price.iloc[0]) ** (250 / k) - 1) for k, item in enumerate(test_2.purchase_price.iloc[1:], 1)]

# print('CAGR: {:.2f}%'.format(test_2.cagr_return.iloc[-1]))

#%%
plt.title('{}'.format(ticker.upper()))
ticks = [test_2.date.iloc[k] for k in range(0, len(test_2), 5)]
plt.plot(test_2.date, test_2.cum_return)
plt.ylabel('Cumulative Returns (%)')
plt.xticks(ticks, rotation = '45')
plt.tight_layout()
# plt.savefig('backtest_{}.png'.format(ticker), bbox = 'tight')

#%%
initial_positions.append(final_positions).drop(columns = ['ATR', 'index_1']).sort_values(by = ['index_0', 'date']).head(10)

#%%
final_positions

#%%
final_positions.loc[
    ((final_positions.trade_type == 'loss') & (final_positions.returns >= 0)) |
    ((final_positions.trade_type == 'profit') & (final_positions.returns <= 0))
]

#%%
len(final_positions.loc[final_positions['trade_type'] == 'profit']) - len(final_positions.loc[final_positions['trade_type'] == 'loss'])

#%%
initial_positions.append(final_positions).drop(columns = ['ATR', 'index_1'])#.head(10)#.groupby(by = 'index_0').sum()
