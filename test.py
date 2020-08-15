#%%
from time import sleep
from datetime import date, timedelta
from datetime import datetime as dt
from os import path, listdir
from sqlite3 import connect

from pandas import DataFrame, read_csv, read_sql_query, concat

from assets import Update_Assets

import matplotlib.pyplot as plt

from yahoo_earnings_calendar import YahooEarningsCalendar

import FundamentalAnalysis as fa

import yfinance

from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models
from pypfopt import expected_returns

key = '6932feb0028eb33d25c8f70b36258084'

#%%
tickers = [
    # 'AMZN',
    # 'ARKK',
    # 'BYND',
    # 'HON',
    # 'IBM',
    # 'INTC',
    # 'GOOGL',
    # 'GS',
    # 'IPOB',
    # 'IPOC',
    # 'SPCE',
    # 'SPOT',
    # 'TSLA',
    # 'TWTR',
    # 'WORK',
    # 'BIDI11.SA',
    # 'BBAS3.SA',
    # 'BBDC4.SA',
    # 'EGIE3.SA',
    # 'ITSA4.SA',
    # 'TAEE11.SA',
    # 'TIET11.SA',
    # 'WEGE3.SA',
    # 'WHRL4.SA',
]

#%%
df_new = yfinance.Ticker('ITSA4.SA').history().reset_index()
df_new = df_new.rename(columns = {'Date': 'date'})

connection = connect('database.db')
df = read_sql_query('SELECT * FROM ITSA4 ORDER BY date', connection)
df = df.rename(columns = {'Date': 'date'})
df['date'] = [dt.strptime(elem, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') for elem in df.date]

ticker = 'ITSA4'
new = ticker + '_new'
df_new.to_sql(new, connection, if_exists = 'replace', index = False)
dataframe = read_sql_query('SELECT * FROM {} UNION SELECT * FROM {} ORDER BY date DESC'.format(ticker, new),connection)
cursor = connection.cursor()
cursor.execute('DROP TABLE {}'.format(new))
connection.commit()

dataframe = dataframe.rename(columns = {'Date': 'date'})
dataframe['date'] = [dt.strptime(elem, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') for elem in dataframe.date]
dataframe.drop_duplicates('date', inplace = True)
connection = connect('database.db')
dataframe.to_sql(ticker, connection, if_exists = 'replace', index = False)
connection.close()

#%%
dataframe

#%%
def dataframe(ticker, key, query, period = 'quarter'):
    try:
        if query == 'dcf':
            df = fa.discounted_cash_flow(ticker = ticker, api_key = key, period = period)
        if query == 'balance':
            df = fa.balance_sheet_statement(ticker = ticker, api_key = key, period = period)
        if query == 'income':
            df = fa.income_statement(ticker = ticker, api_key = key, period = period)
        if query == 'cf':
            df = fa.cash_flow_statement(ticker = ticker, api_key = key, period = period)
        if query == 'ratio':
            df = fa.financial_ratios(ticker = ticker, api_key = key, period = period)
        if query == 'growth':
            df = fa.financial_statement_growth(ticker = ticker, api_key = key, period = period)
        df = df.transpose().sort_index()
    except:
        df = DataFrame()
    return df

#%%
# queries = [
#     'dcf',
#     'balance',
#     'income',
#     'cf',
#     'ratio',
#     'growth',
# ]

tickers = [
    # 'AMZN',
    # 'ARKK',
    # 'BYND',
    # 'HON',
    # 'IBM',
    # 'INTC',
    # 'GOOGL',
    # 'GS',
    # 'IPOB',
    # 'IPOC',
    # 'SPCE',
    # 'SPOT',
    # 'TSLA',
    # 'TWTR',
    # 'WORK',
    'BIDI11.SA',
    'BBAS3.SA',
    'BBDC4.SA',
    'EGIE3.SA',
    'ITSA4.SA',
    'TAEE11.SA',
    'TIET11.SA',
    'WEGE3.SA',
    'WHRL4.SA',
]

#%%
tickers = [
    'ENBR3.SA',
    # 'KO',
    # 'BABA',
    'BIOM3.SA',
    'SMLL.SA',
    'BRML3.SA',
    'VIVA3.SA',
    'OIBR3.SA',
    # 'GLD',
    'OZ1D.SA',
    'OZ2D.SA',
    'OZ3D.SA',
    # 'AAPL',
    # 'FB',
    # 'NFLX',
    # 'MSFT',
    # 'BRK.A',
    # 'SPY',
    # 'IVV',
    # 'VGTSX',
    # 'IAU',
    # 'FXI',
    # 'EFV',
    # 'EWY',
]

#%%
for ticker in tickers:
    print(ticker.replace('.SA', ''))
    # for query in queries:
        # df = dataframe(ticker, key, query)
    df =  yfinance.Ticker('ITSA4.SA').history(period = 'max').reset_index()
    # if df.empty == False:
    # connection = connect('{}.db'.format(query))
    connection = connect('database.db')
    df.to_sql(ticker.replace('.SA', ''), connection, if_exists = 'replace')
    connection.close()
    sleep(5.0)

#%%
connection = connect('ratio.db')
ibm_ratio = read_sql_query('SELECT * FROM IBM', connection)
connection.close()
connection = connect('growth.db')
ibm_growth = read_sql_query('SELECT * FROM IBM', connection)
connection.close()

#%%
ibm_ratio.loc[ibm_ratio['index'] >= '2018-01'].plot('index', 'priceEarningsToGrowthRatio')
ibm_ratio.loc[ibm_ratio['index'] >= '2018-01'].plot('index', 'priceEarningsRatio')

#%%
ibm_growth.loc[ibm_growth['index'] >= '2015-01'].plot('index', 'priceEarningsRatio')

#%%
ibm_ratio[['index', 'cashPerShare']].tail()

#%%
ibm_ratio.loc[ibm['index'] >= '2000-01'].plot('index', 'cashPerShare')
ibm_ratio.loc[ibm['index'] >= '2002-01'].plot('index', 'priceToFreeCashFlowsRatio')

#%%
ibm_ratio.loc[ibm_ratio['index'] >= '2013-10'].plot('index', 'priceEarningsToGrowthRatio')

#%%
yec = YahooEarningsCalendar()
vector = yec.get_earnings_of('ibm')
df = DataFrame(vector)
df['startdatetime'] = [df['startdatetime'].str.split('T')[k][0] for k in range(len(df))]

#%%
df.dropna().sort_values('startdatetime').plot('startdatetime', 'epsactual')
df

#%%

#%%
class Investments():
    def __init__(self, path = 'investments/', name = 'get_investments', **kwargs):
        self.path = path
        self.bonds_path, self.crypto_path = self.path + 'bonds/', self.path + 'crypto/'
        self.domestic_stocks_path = self.path + 'stocks/domestic'
        self.international_stocks_path = self.path + 'stocks/international/'
        self.list_paths = [self.bonds_path, self.crypto_path, self.domestic_stocks_path, self.international_stocks_path]

        self.get_dollar()
        self.get_all_assets()

        self.domestic_bond_returns()
        # self.portfolio_domestic_stocks = self.get_quotas('domestic_stocks')
        self.portfolio_international_stocks = self.get_quotas('international_stocks')
        self.portfolio_crypto = self.get_quotas('crypto')

        self.get_portfolio()
        self.get_aggregate()

        self.get_benchmarks()
        self.get_time_series()

    def __call__(self, flag = 'assets'):
        if flag == 'dollar':
            return self.dollar
        if flag == 'bonds':
            return self.bonds, self.interests
        if flag == 'stocks':
            # return self.domestic_tickers, self.international_tickers
            return self.international_tickers
        if flag == 'crypto':
            return self.crypto, self.fractions
        if flag == 'portfolio':
            return self.portfolio.round(2), self.portfolio_aggregate.round(2)
        if flag == 'save':
            rounded = self.portfolio.round(2)
            rounded2 = self.portfolio_aggregate.round(2)
            connection = connect('database.db')
            rounded.to_sql('portfolio', connection, if_exists = 'replace', index = False)
            rounded2.to_sql('aggregate', connection, if_exists = 'replace', index = False)
            connection.close()
        if flag == 'time_series':
            return self.portfolio_time_series


    def get_dollar(self):
        currency = 'BRLUSD'
        connection = connect('database.db')
        self.dollar = read_sql_query('SELECT * FROM {}'.format(currency), connection).iloc[0].close
        connection.close()
    
    def domestic_bond_returns(self):
        end = dt.strptime(date.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
        cdi = read_csv('interests/cdi.csv')
        cdi['date'] = [dt.strptime(data, '%d/%m/%Y') for data in cdi.data]
        self.total_returns_brl = 0
        for k in range(len(self.bonds)):
            start = dt.strptime(self.bonds.date[k], '%Y-%m-%d')
            cut = cdi.loc[(cdi.date >= start) & (cdi.date <= end)][1:]
            value = self.bonds.price[k]
            for interest in cut.valor:
                value = value * (1 + interest * self.bonds.pct_cdi[k] / 100)
            self.total_returns_brl += value
        self.total_returns_usd = self.total_returns_brl * self.dollar
        self.portfolio_bonds = DataFrame({
            'asset': ['DOMESTIC BONDS'],
            'quotas': [1],
            'value_usd': [self.total_returns_usd],
            'value_brl': [self.total_returns_brl],
        })

    def get_all_assets(self):
        self.interests, self.fractions = list(), list()
        self.domestic_tickers, self.international_tickers = list(), list()
        for directory in self.list_paths:
            list_files = list()
            for filename in listdir(directory):
                if filename.endswith('.csv'):
                    list_files.append(path.join(directory, filename))
                    if directory == self.bonds_path:
                        self.interests.append(filename.replace('.csv', ''))
                    if directory == self.domestic_stocks_path:
                        self.domestic_tickers.append(filename.replace('.csv', ''))
                    if directory == self.international_stocks_path:
                        self.international_tickers.append(filename.replace('.csv', ''))
                    if directory == self.crypto_path:
                        self.fractions.append(filename.replace('.csv', ''))
            dictionary = dict()
            if directory == self.bonds_path:
                for filename, interest in zip(list_files, self.interests):
                    df = read_csv(filename)
                    dictionary[interest] = df
                self.bonds = concat(dictionary)
            else:
                if directory == self.domestic_stocks_path:
                    symbols = self.domestic_tickers
                if directory == self.international_stocks_path:
                    symbols = self.international_tickers
                if directory == self.crypto_path:
                    symbols = self.fractions
                for filename, ticker in zip(list_files, symbols):
                    df = read_csv(filename)
                    df['cum_share'] = df.share.cumsum()
                    df['price_share'] = (df.purchase_price / df.share).round(2)
                    df['cum_price_share'] = df.price_share.expanding().mean().round(2)
                    dictionary[ticker] = df
                self.stocks = concat(dictionary)
                if directory == self.domestic_stocks_path:
                    self.domestic_stocks = concat(dictionary)
                if directory == self.international_stocks_path:
                    self.international_stocks = concat(dictionary)
                if directory == self.crypto_path:
                    self.crypto = concat(dictionary)

    def get_quotas(self, asset):
        quotas = dict()
        if asset == 'domestic_stocks':
            list_tickers  = self.domestic_tickers
        if asset == 'international_stocks':
            list_tickers  = self.international_tickers
        if asset == 'crypto':
            list_tickers = self.fractions
        for ticker in list_tickers:
            key = ticker.upper()
            if asset == 'domestic_stocks':
                quotas[key] = self.domestic_stocks.loc[ticker].cum_share.iloc[-1]
            if asset == 'international_stocks':
                quotas[key] = self.international_stocks.loc[ticker].cum_share.iloc[-1]
            if asset == 'crypto':
                quotas[key] = self.crypto.loc[ticker].cum_share.iloc[-1]
        portfolio = DataFrame({
            'asset': list(quotas.keys()),
            'quotas': list(quotas.values())
        })
        value_usd, value_brl = list(), list()
        connection = connect('database.db')
        for asset in list(quotas.keys()):
            close_price = read_sql_query("SELECT adjusted_close FROM {} ORDER BY date DESC LIMIT 1".format(asset), connection).values.flatten()[0]
            value_usd.append((close_price * quotas.get(asset)).round(2))
            value_brl.append((close_price * quotas.get(asset) / self.dollar).round(2))
        connection.close()
        portfolio['value_usd'] = value_usd
        portfolio['value_brl'] = value_brl
        portfolio.sort_values(by = ['value_usd'], ascending = False, inplace = True)
        return portfolio

    def get_portfolio(self):
        self.portfolio = dict()
        self.portfolio['bonds'] = self.portfolio_bonds
        # self.portfolio['domestic_stocks'] = self.portfolio_domestic_stocks
        self.portfolio['international stocks'] = self.portfolio_international_stocks
        self.portfolio['crypto'] = self.portfolio_crypto
        self.portfolio = concat(self.portfolio)
        self.portfolio = self.portfolio.loc[~(self.portfolio.asset == 'IPOC')].loc[~(self.portfolio.asset == 'IPOB')]

    def get_aggregate(self):
        assets = list(self.portfolio.index.unique(level = 0))
        value_brl, value_usd = list(), list()
        for asset in assets:
            value_brl.append(self.portfolio.loc[asset].sum().value_brl)
            value_usd.append(self.portfolio.loc[asset].sum().value_usd)
        self.portfolio_aggregate = DataFrame({
            'asset': assets,
            'value_brl': value_brl,
            'value_usd': value_usd,
        })

    def insert_weekends(self, df):
        df.set_index('date', inplace = True)
        start, end = df.index[0], df.index[-1]
        start = dt.strptime(start, '%Y-%m-%d').date()
        end = dt.strptime(end, '%Y-%m-%d').date()
        dates = [str(start + timedelta(days = x)) for x in range(0, (end - start).days + 1, 1)]
        df = df.reindex(dates, fill_value = 0)
        df.reset_index(inplace = True)
        adjusted_close = list()
        for value in df.adjusted_close:
            if value != 0:
                adjusted_close.append(value)
            if value == 0:
                adjusted_close.append(adjusted_close[-1])
        df.adjusted_close = adjusted_close
        return df

    def get_benchmarks(self):
        connection = connect('database.db')
        self.spy = read_sql_query('SELECT date, adjusted_close FROM SPY ORDER BY date', connection)
        connection.close()
        self.spy = self.insert_weekends(self.spy)

    def get_returns(self, df):
        reference = concat([self.international_stocks[['date', 'purchase_price']], self.crypto[['date', 'purchase_price']]])
        reference = reference.groupby(by = 'date')['purchase_price'].sum()
        reference = DataFrame(reference).reset_index()
        df.reset_index(inplace = True)
        returns = list()
        for date in df['date'].iloc[1:]:
            end = df.loc[df.date == date, 'position'].index[0]
            start = end - 1
            if date not in reference['date'].to_list():
                retorno = (df.position.iloc[end] - df.position.iloc[start]) / df.position.iloc[start]
                returns.append(retorno)
            if date in reference['date'].to_list():
                cash_flow = reference.loc[reference.date == date, 'purchase_price'].iloc[0]
                retorno = (df.position.iloc[end] - (df.position.iloc[start] + cash_flow)) / (df.position.iloc[start] + cash_flow)
                returns.append(retorno)
        returns = [0] + returns
        return returns

    def get_time_series(self):
        df = concat([self.international_stocks[['date', 'share']], self.crypto[['date', 'share']]])
        quotes = df.index.unique(level = 0).to_list()
        connection = connect('database.db')
        self.portfolio_time_series = DataFrame()
        for quote in quotes:
            prices = read_sql_query("SELECT date, adjusted_close FROM {} ORDER BY date".format(quote), connection)
            prices = self.insert_weekends(prices)
            for data in df.loc[quote].date:
                share = df.loc[quote].set_index('date').loc[data].iloc[0]
                close_price = prices.loc[prices['date'] >= data]
                close_price['position'] = [price * share for price in close_price.adjusted_close]
                self.portfolio_time_series = concat([self.portfolio_time_series, close_price])
        self.portfolio_time_series = self.portfolio_time_series.groupby(by = ['date'])['position'].sum()
        self.portfolio_time_series = DataFrame(self.portfolio_time_series)
        self.portfolio_time_series['SPY'] = self.spy.loc[(self.spy.date >= self.portfolio_time_series.index[0]) & (self.spy.date <= self.portfolio_time_series.index[-1])]['adjusted_close'].to_list()
        # self.portfolio_time_series = self.portfolio_time_series.loc[self.portfolio_time_series.index >= '2020-03-22']
        self.portfolio_time_series['return_SPY'] = (self.portfolio_time_series.SPY.pct_change() + 1).fillna(1).cumprod() - 1
        self.portfolio_time_series['return_position'] = self.get_returns(self.portfolio_time_series)
        self.portfolio_time_series['return_position'] = (self.portfolio_time_series.return_position + 1).cumprod() - 1

#%%
investments = Investments()
# domestic_tickers, international_tickers = investments('stocks')
international_tickers = investments('stocks')
_, crypto = investments('crypto')
# asset_class = Update_Assets(asset = international_tickers + crypto)
bonds, interests = investments('bonds')
portfolio, portfolio_aggregate = investments('portfolio')
dollar = investments('dollar')
# investments('save')
time_series = investments('time_series')

#%%
time_series.return_SPY.corr(time_series.return_position)

#%%
x = time_series.date
date_plot = [x.iloc[k] for k in range(0, len(x), 8)]
y1 = time_series.return_position
y2 = time_series.return_SPY

fig, ax = plt.subplots(1, figsize = (8, 5))
fig.tight_layout()
ax.plot(x, y1, label = 'Portfolio')
ax.plot(x, y2, label = 'SPY')
leg = plt.legend(loc = 'upper left', frameon = False)
for text in leg.get_texts():
    plt.setp(text, color = 'black')
ax.set_ylabel('Cumulative Returns')
plt.xticks(date_plot)
plt.setp(ax.xaxis.get_majorticklabels(), rotation = 90)
plt.show()

#%%
vector = [1047.01, 4345.12]
aux = DataFrame({
    'asset': ['domestic stocks', 'domestics funds'],
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
    x = portfolio[1:-2].value_usd, 
    labels = portfolio[1:-2].asset,
    shadow = True,
    autopct = '%1.1f%%',
    pctdistance = 0.80,
    startangle = 90,
    )
plt.tight_layout()
plt.show()

# %%
