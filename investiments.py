from os import path, listdir
from time import sleep
from sqlite3 import connect
from datetime import date, timedelta
from datetime import datetime as dt

from numpy import cumprod
from pandas import DataFrame, read_sql_query, read_csv, concat

class Investments():
    def __init__(self, path = 'investments/', name = 'get_investments', **kwargs):
        self.kwargs = kwargs
        self.path = path
        self.hyperparameters()
        self.get_dollar()
        self.get_all_assets()

        self.domestic_bond_returns()
        self.portfolio_domestic_stocks = self.get_quotas('domestic_stocks')
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
            return self.domestic_tickers, self.international_tickers
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

    def hyperparameters(self):
        self.database = self.kwargs.get('database', 'database.db')
        self.bonds_path, self.crypto_path = self.path + 'bonds/', self.path + 'crypto/'
        self.domestic_stocks_path = self.path + 'stocks/domestic'
        self.international_stocks_path = self.path + 'stocks/international/'
        self.list_paths = [self.bonds_path, self.crypto_path, self.domestic_stocks_path, self.international_stocks_path]

    def get_dollar(self):
        currency = 'BRLUSD'
        connection = connect('database.db')
        self.dollar = read_sql_query('SELECT * FROM {}'.format(currency), connection).iloc[0].close
        self.dollar_full = read_sql_query('SELECT date, close as adjusted_close FROM {} ORDER BY date'.format(currency), connection)
        connection.close()
        self.dollar_full = self.insert_weekends(self.dollar_full)

    def get_benchmarks(self):
        connection = connect('database.db')
        self.spy = read_sql_query('SELECT date, adjusted_close FROM SPY ORDER BY date', connection)
        self.bova = read_sql_query('SELECT date, adjusted_close FROM BOVA11 ORDER BY date', connection)
        connection.close()
        self.spy = self.insert_weekends(self.spy)
        self.bova = self.insert_weekends(self.bova)
        self.bova = self.bova.loc[(self.bova.date >= self.dollar_full.date.iloc[0]) & (self.bova.date <= self.dollar_full.date.iloc[-1])]
        self.bova['adjusted_close_dollar'] = [x*y for x, y in zip(self.bova.adjusted_close.to_list(), self.dollar_full.adjusted_close.to_list())]

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
            'asset': ['domestic bonds'],
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
                        self.interests.append(filename.replace('.csv', '').upper())
                    if directory == self.domestic_stocks_path:
                        self.domestic_tickers.append(filename.replace('.csv', '').upper())
                    if directory == self.international_stocks_path:
                        self.international_tickers.append(filename.replace('.csv', '').upper())
                    if directory == self.crypto_path:
                        self.fractions.append(filename.replace('.csv', '').upper())
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
                    if ticker in self.domestic_tickers:
                        price_dollar = list()
                        for price, data in zip(df.purchase_price, df.date):
                            price_dollar.append(price * self.dollar_full.loc[self.dollar_full.date == data, 'adjusted_close'].iloc[0])
                        df['purchase_price'] = price_dollar
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
        domestic = False
        if asset == 'domestic_stocks':
            list_tickers  = self.domestic_tickers
            domestic = True
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
            if asset in self.fractions:
                close_price = read_sql_query("SELECT close FROM {} ORDER BY date DESC LIMIT 1".format(asset), connection).values.flatten()[0]
            else:
                close_price = read_sql_query("SELECT adjusted_close FROM {} ORDER BY date DESC LIMIT 1".format(asset), connection).values.flatten()[0]
            if domestic == False:
                value_usd.append(close_price * quotas.get(asset))
                value_brl.append(close_price * quotas.get(asset) / self.dollar)
            if domestic == True:
                value_usd.append(close_price * quotas.get(asset) * self.dollar)
                value_brl.append(close_price * quotas.get(asset))
        connection.close()
        portfolio['value_usd'] = value_usd
        portfolio['value_brl'] = value_brl
        portfolio.sort_values(by = ['value_usd'], ascending = False, inplace = True)
        return portfolio

    def get_portfolio(self):
        self.portfolio = dict()
        self.portfolio['domestic bonds'] = self.portfolio_bonds
        self.portfolio['domestic stocks'] = self.portfolio_domestic_stocks
        self.portfolio['international stocks'] = self.portfolio_international_stocks
        self.portfolio['crypto'] = self.portfolio_crypto
        self.portfolio = concat(self.portfolio)
        self.portfolio = self.portfolio.loc[~(self.portfolio.quotas == 0.)]
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

    def insert_weekends(self, df, crypto = False):
        df.set_index('date', inplace = True)
        start, end = df.index[0], df.index[-1]
        start = dt.strptime(start, '%Y-%m-%d').date()
        end = dt.strptime(end, '%Y-%m-%d').date()
        dates = [str(start + timedelta(days = x)) for x in range(0, (end - start).days + 1, 1)]
        df = df.reindex(dates, fill_value = 0)
        df.reset_index(inplace = True)
        adjusted_close = list()
        if crypto == True:
            for value in df.close:
                if value != 0:
                    adjusted_close.append(value)
                if value == 0:
                    adjusted_close.append(adjusted_close[-1])
            df.close = adjusted_close
        if crypto == False:
            for value in df.adjusted_close:
                if value != 0:
                    adjusted_close.append(value)
                if value == 0:
                    adjusted_close.append(adjusted_close[-1])
            df.adjusted_close = adjusted_close
        return df

    def get_returns(self, df, flag = 'cumulative'):
        reference = concat([self.domestic_stocks[['purchase_price']], self.international_stocks[['date', 'purchase_price']], self.crypto[['date', 'purchase_price']]])
        reference = reference.groupby(by = 'date')['purchase_price'].sum()
        reference = DataFrame(reference).reset_index()
        df.reset_index(inplace = True)
        returns = list()
        if flag == 'cumulative':
            for date in df['date'].iloc[1:]:
                end = df.loc[df.date == date, 'portfolio'].index[0]
                start = end - 1
                if date not in reference['date'].to_list():
                    retorno = (df.portfolio.iloc[end] - df.portfolio.iloc[start]) / df.portfolio.iloc[start]
                    returns.append(retorno)
                if date in reference['date'].to_list():
                    cash_flow = reference.loc[reference.date == date, 'purchase_price'].iloc[0]
                    retorno = (df.portfolio.iloc[end] - (df.portfolio.iloc[start] + cash_flow)) / (df.portfolio.iloc[start] + cash_flow)
                    returns.append(retorno)
            returns = [0] + returns
            returns = list(map(lambda x: x + 1, returns))
            returns = 100 * (cumprod(returns) - 1)
        if flag == 'cagr':
            for date in df['date'].iloc[1:]:
                end = df.loc[df.date == date, 'portfolio'].index[0]
                start = df.index[1]
                exponent = 365 / (end - start)
                cash_flow = reference.loc[(reference.date >= self.start_date) & (reference.date <= date), 'purchase_price'].sum()
                retorno = 100 * (((df.portfolio.iloc[end] / (df.portfolio.iloc[start] + cash_flow)) ** exponent) - 1)
                returns.append(retorno)
            returns = [0] + returns
        return returns

    def get_start_date(self):
        start_domestic = self.domestic_stocks[['date']].sort_values(by = 'date').iloc[0].values[0]
        start_international = self.international_stocks[['date']].sort_values(by = 'date').iloc[0].values[0]
        start_crypto = self.crypto[['date']].sort_values(by = 'date').iloc[0].values[0]
        start_date = min(start_domestic, start_international, start_crypto)
        self.start_date = self.kwargs.get('start_date_returns', start_date)
        self.start_date = dt.strptime(self.start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
    
    def get_end_date(self):
        end_spy = self.spy[['date']].sort_values(by = 'date').iloc[-1].values[0]
        end_bova =  self.bova[['date']].sort_values(by = 'date').iloc[-1].values[0]
        df = concat([self.domestic_stocks[['date']], self.international_stocks[['date']], self.crypto[['date']]])
        quotes = df.index.unique(level = 0).to_list()
        dates = [end_spy, end_bova]
        connection = connect(self.database)
        for quote in quotes:
            date = read_sql_query('SELECT date FROM "{}" ORDER BY date'.format(quote), connection).iloc[-1].values[0]
            dates.append(date)
        connection.close()
        end_date = min(dates)
        self.end_date = self.kwargs.get('end_date_returns', end_date)
        self.end_date = dt.strptime(self.end_date, '%Y-%m-%d').strftime('%Y-%m-%d')

    def get_time_series(self):
        self.get_start_date()
        self.get_end_date()
        dates = [(dt.strptime(self.start_date, '%Y-%m-%d').date() + timedelta(days = k)).strftime('%Y-%m-%d') \
            for k in range((dt.strptime(self.end_date, '%Y-%m-%d').date() - dt.strptime(self.start_date, '%Y-%m-%d').date()).days + 1)]

        dataframe = DataFrame()
        df = concat([self.domestic_stocks[['date', 'share']], self.international_stocks[['date', 'share']], self.crypto[['date', 'share']]])
        quotes = df.index.unique(level = 0).to_list()
        for quote in quotes:
            connection = connect(self.database)
            if quote in self.fractions:
                prices = read_sql_query("SELECT date, close FROM '{}' ORDER BY date".format(quote), connection)
                prices = self.insert_weekends(prices, crypto = True)
            else:
                prices = read_sql_query("SELECT date, adjusted_close FROM '{}' ORDER BY date".format(quote), connection)
                prices = self.insert_weekends(prices)
            connection.close()
            for data, share in zip(df.loc[quote].date, df.loc[quote].share):
                close_price = prices.loc[prices.date >=  data]
                conversion = self.dollar_full.loc[self.dollar_full.date == data, 'adjusted_close'].iloc[0] if quote in self.domestic_tickers else 1.
                if quote in self.fractions:
                    close_price['portfolio'] = [price * share * conversion for price in close_price.close]
                else:
                    close_price['portfolio'] = [price * share * conversion for price in close_price.adjusted_close]
                dataframe = concat([dataframe, close_price])
        dataframe = dataframe.groupby(by = ['date']).sum().drop(columns = {'adjusted_close', 'close'})
        dataframe = DataFrame(dataframe).loc[(dataframe.index >= self.start_date) & (dataframe.index <= self.end_date)]

        self.portfolio_time_series = DataFrame()
        self.portfolio_time_series['date'] = dates
        self.portfolio_time_series['portfolio'] = dataframe.portfolio.to_list()
        self.portfolio_time_series['SPY'] = self.spy.loc[(self.spy.date >= self.start_date) & (self.spy.date <= self.end_date), 'adjusted_close'].to_list()
        self.portfolio_time_series['BOVA11'] = self.bova.loc[(self.bova.date >= self.start_date) & (self.bova.date <= self.end_date)]['adjusted_close_dollar'].to_list()
        self.portfolio_time_series.sort_values(by = 'date', inplace = True)
        self.portfolio_time_series['return_portfolio'] = self.get_returns(self.portfolio_time_series)
        self.portfolio_time_series['return_SPY'] = 100 * ((self.portfolio_time_series.SPY.pct_change() + 1).fillna(1).cumprod() - 1)
        self.portfolio_time_series['return_BOVA11'] = 100 * ((self.portfolio_time_series.BOVA11.pct_change() + 1).fillna(1).cumprod() - 1)
        self.portfolio_time_series['cagr_portfolio'] = self.get_returns(self.portfolio_time_series, flag = 'cagr')
        self.portfolio_time_series['cagr_SPY'] = [0] + [100 * ((cagr / self.portfolio_time_series.SPY.iloc[0]) ** (365 / k) - 1) for k, cagr in enumerate(self.portfolio_time_series.SPY.iloc[1:], 1)]
        self.portfolio_time_series['cagr_BOVA11'] = [0] + [100 * ((cagr / self.portfolio_time_series.BOVA11.iloc[0]) ** (365 / k) - 1) for k, cagr in enumerate(self.portfolio_time_series.BOVA11.iloc[1:], 1)]
        self.portfolio_time_series.drop(columns = {'level_0', 'index'}, inplace = True)
        self.portfolio_time_series.set_index('date', inplace = True)