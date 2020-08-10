#%%
from datetime import date
from datetime import datetime as dt
from os import path, listdir
from sqlite3 import connect

from pandas import DataFrame, read_csv, read_sql_query, concat

from assets import Update_Assets

import matplotlib.pyplot as plt


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
        # self.portfolio = self.portfolio.loc[~(self.portfolio.asset == 'IPOC')].loc[~(self.portfolio.asset == 'IPOB')]

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


#%%
investments = Investments()
# domestic_tickers, international_tickers = investments('stocks')
international_tickers = investments('stocks')
_, crypto = investments('crypto')
# asset_class = Update_Assets(asset = international_tickers + crypto)
bonds, interests = investments('bonds')
portfolio, portfolio_aggregate = investments('portfolio')
dollar = investments('dollar')
investments('save')

#%%
vector = [1088.86, 4423.30]
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
