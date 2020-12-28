from os import path, listdir
from time import sleep
from sqlite3 import connect
from datetime import timedelta
from datetime import date as dt_date
from datetime import datetime as dt

from numpy import cumprod
from pandas import DataFrame, read_sql_query, read_csv, concat

from functions import psqlEngine

class Investments():
    def __init__(self, path = 'investments/', name = 'get_investments', **kwargs):
        self.kwargs = kwargs
        self.path = path
        self.hyperparameters()
        self.get_dollar()
        self.get_all_assets()

        self.domestic_bond_returns()
        self.get_benchmarks()

        self.portfolio_domestic_stocks = self.get_quotas('domestic_stocks')
        self.portfolio_international_stocks = self.get_quotas('international_stocks')
        self.portfolio_crypto = self.get_quotas('crypto')
        self.portfolio_domestic_options = self.get_quotas('domestic_options')
        self.portfolio_domestic_funds = self.get_quotas('domestic_funds')

        self.get_portfolio()
        self.get_aggregate()
        self.get_time_series()

    def __call__(self, flag = 'assets'):
        if flag == 'dollar':
            return self.dollar
        if flag == 'bonds':
            return self.domestic_bonds, self.interests
        if flag == 'stocks':
            return self.domestic_tickers, self.international_tickers
        if flag == 'crypto':
            return self.crypto, self.fractions
        if flag == 'portfolio':
            return self.portfolio.round(2), self.portfolio_aggregate.round(2)
        if flag == 'save':
            rounded = self.portfolio.round(2)
            rounded2 = self.portfolio_aggregate.round(2)
            engine = psqlEngine(self.database)
            connection = engine.connect()
            rounded.to_sql('portfolio', connection, if_exists = 'replace', index = False)
            rounded2.to_sql('aggregate', connection, if_exists = 'replace', index = False)
            connection.close()
            engine.dispose()
        if flag == 'time_series':
            return self.portfolio_time_series.round(2)

    def hyperparameters(self):
        self.database = self.kwargs.get('database', 'database.ini')
        self.domestic_database = self.kwargs.get('domestic_database', 'domestic')
        self.international_database = self.kwargs.get('international_database', 'usa_stocks')
        self.portfolio_database = self.kwargs.get('portfolio_database', 'portfolio.db')
        self.currency_database = self.kwargs.get('currency_database', 'currencies')
        self.domestic_bonds_path = '{}bonds/'.format(self.path)
        self.crypto_path = '{}crypto/'.format(self.path)
        self.domestic_stocks_path = '{}stocks/domestic/'.format(self.path)
        self.international_stocks_path = '{}stocks/international/'.format(self.path)
        self.domestic_options_path = '{}options/domestic/'.format(self.path)
        self.domestic_funds_path = '{}funds/domestic/'.format(self.path)
        self.list_paths = [
            self.domestic_bonds_path, 
            self.crypto_path, 
            self.domestic_stocks_path, 
            self.international_stocks_path, 
            self.domestic_options_path,
            self.domestic_funds_path,
        ]

    def get_dollar(self):
        currency = 'BRLUSD'
        engine = psqlEngine(self.database)
        connection = engine.connect()
        # connection = connect(self.currency_database)
        self.dollar = read_sql_query("SELECT * FROM {} WHERE ticker = '{}'".format(self.currency_database, currency), connection).iloc[0].close
        self.dollar_full = read_sql_query("SELECT date, close FROM {} WHERE ticker = '{}' ORDER BY date".format(self.currency_database, currency), connection)
        connection.close()
        engine.dispose()
        self.dollar_full = self.insert_weekends(self.dollar_full)

    def get_benchmarks(self):
        engine = psqlEngine(self.database)
        connection = engine.connect()
        # connection = connect(self.international_database)
        self.spy = read_sql_query("SELECT date, adjusted_close as close FROM {} WHERE ticker = 'SPY' ORDER BY date".format(self.international_database), connection)
        # connection.close()
        # engine = psqlEngine(self.database)
        # connection = engine.connect()
        self.bova = read_sql_query("SELECT date, close FROM {} WHERE ticker = 'BOVA11' ORDER BY date".format(self.domestic_database), connection)
        connection.close()
        engine.dispose()
        self.spy = self.insert_weekends(self.spy)
        self.bova = self.insert_weekends(self.bova)
        self.bova = self.bova.loc[(self.bova.date >= self.dollar_full.date.iloc[0]) & (self.bova.date <= self.dollar_full.date.iloc[-1])]
        self.bova['close_dollar'] = [x*y for x, y in zip(self.bova.close.to_list(), self.dollar_full.close.to_list())]

    def get_return_benchmark_portfolio(self):
        value_bond, value_bova = 400, 600
        value = list()
        dates = self.bova.loc[(self.bova.date >= self.start_date) & (self.bova.date <= self.end_date), 'date'].to_list()
        bova_dollar = self.bova.loc[(self.bova.date >= self.start_date) & (self.bova.date <= self.end_date), 'close_dollar']
        interests = self.insert_weekends(self.cdi[['date', 'interest']], asset = '6040').interest
        for interest, return_bova in zip(interests, bova_dollar.pct_change().fillna(0)):
            value_bond = value_bond * interest
            value_bova = value_bova * (1 + return_bova)
            value.append(value_bond + value_bova)
        self.benchmark_portfolio = DataFrame({
            'date': dates,
            'portfolio': value,
        })

    def domestic_bond_returns(self):
        end = dt_date.today().strftime('%Y-%m-%d')
        self.cdi = read_csv('interests/cdi.csv')
        self.cdi['date'] = [dt.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d') for date in self.cdi.date]
        self.cdi['interest'] = [1 + interest / 100 for interest in self.cdi.cdi]
        total_returns_brl = 0
        for date, purchase_price, share in zip(self.domestic_bonds.date, self.domestic_bonds.purchase_price, self.domestic_bonds.share):
            cdi = self.cdi.loc[(self.cdi.date >= date) & (self.cdi.date <= end)]
            value = purchase_price
            for interest in cdi.interest:
                value = value * (interest * share)
            total_returns_brl += value
        total_returns_usd = total_returns_brl * self.dollar
        self.portfolio_bonds = DataFrame({
            'asset': ['domestic bonds'],
            'quotas': [1],
            'value_usd': [total_returns_usd],
            'value_brl': [total_returns_brl],
        })

    def get_all_assets(self):
        self.interests, self.fractions = list(), list()
        self.domestic_tickers, self.international_tickers = list(), list()
        self.domestic_options_tickers = list()
        self.domestic_funds_tickers = list()
        for directory in self.list_paths:
            list_files = list()
            for filename in listdir(directory):
                if filename.endswith('.csv'):
                    list_files.append(path.join(directory, filename))
                    if directory == self.domestic_bonds_path:
                        self.interests.append(filename.replace('.csv', '').upper())
                    if directory == self.crypto_path:
                        self.fractions.append(filename.replace('.csv', '').upper())
                    if directory == self.domestic_stocks_path:
                        self.domestic_tickers.append(filename.replace('.csv', '').upper())
                    if directory == self.international_stocks_path:
                        self.international_tickers.append(filename.replace('.csv', '').upper())
                    if directory == self.domestic_options_path:
                        self.domestic_options_tickers.append(filename.replace('.csv', '').upper())
                    if directory == self.domestic_funds_path:
                        self.domestic_funds_tickers.append(filename.replace('.csv', '').upper())
            dictionary = dict()
            if directory == self.domestic_bonds_path:
                for filename, interest in zip(list_files, self.interests):
                    df = read_csv(filename)
                    dictionary[interest] = df
                self.domestic_bonds = concat(dictionary)
                self.domestic_bonds = self.domestic_bonds.rename(columns = {'pct_cdi': 'share'})
                self.domestic_bonds['purchase_price_dollar'] = [price * self.dollar_full.loc[self.dollar_full.date == date, 'close'].iloc[0] for date, price in zip(self.domestic_bonds.date, self.domestic_bonds.purchase_price) ]
            else:
                if directory == self.crypto_path:
                    symbols = self.fractions
                if directory == self.domestic_stocks_path:
                    symbols = self.domestic_tickers
                if directory == self.international_stocks_path:
                    symbols = self.international_tickers
                if directory == self.domestic_options_path:
                    symbols = self.domestic_options_tickers
                if directory == self.domestic_funds_path:
                    symbols = self.domestic_funds_tickers
                for filename, ticker in zip(list_files, symbols):
                    df = read_csv(filename)                        
                    if ticker in self.domestic_funds_tickers:
                        df.set_index('date', inplace = True)
                        df['purchase_price'] = df.purchase_price.diff()
                        df = df.dropna()
                        df.reset_index(inplace = True)
                    if (ticker in self.domestic_tickers) or (ticker in self.domestic_options_tickers) or (ticker in self.domestic_funds_tickers):
                        price_dollar = list()
                        for price, data in zip(df.purchase_price, df.date):
                            price_dollar.append(price * self.dollar_full.loc[self.dollar_full.date == data, 'close'].iloc[0])
                        df['purchase_price'] = price_dollar
                        dictionary[ticker] = df
                    df['cum_share'] = df.share.cumsum()
                    df['price_share'] = (df.purchase_price / df.share)
                    df['cum_price_share'] = df.price_share.expanding().mean()
                    dictionary[ticker] = df
                self.stocks = concat(dictionary)
                if directory == self.crypto_path:
                    self.crypto = concat(dictionary)
                if directory == self.domestic_stocks_path:
                    self.domestic_stocks = concat(dictionary)
                if directory == self.international_stocks_path:
                    self.international_stocks = concat(dictionary)
                if directory == self.domestic_options_path:
                    self.domestic_options = concat(dictionary)
                if directory == self.domestic_funds_path:
                    self.domestic_funds = concat(dictionary)

    def get_quotas(self, asset):
        quotas = dict()
        domestic = False
        if asset == 'crypto':
            list_tickers = self.fractions
        if asset == 'domestic_stocks':
            list_tickers = self.domestic_tickers
            domestic = True
        if asset == 'international_stocks':
            list_tickers = self.international_tickers
        if asset == 'domestic_options':
            list_tickers = self.domestic_options_tickers
            domestic = True
        if asset == 'domestic_funds':
            list_tickers = self.domestic_funds_tickers
            domestic = True
        for ticker in list_tickers:
            key = ticker.upper()
            if asset == 'crypto':
                quotas[key] = self.crypto.loc[ticker].cum_share.iloc[-1]
            if asset == 'domestic_stocks':
                quotas[key] = self.domestic_stocks.loc[ticker].cum_share.iloc[-1]
            if asset == 'international_stocks':
                quotas[key] = self.international_stocks.loc[ticker].cum_share.iloc[-1]
            if asset == 'domestic_options':
                quotas[key] = self.domestic_options.loc[ticker].cum_share.iloc[-1]
            if asset == 'domestic_funds':
                quotas[key] = 1.
        portfolio = DataFrame({
            'asset': list(quotas.keys()),
            'quotas': list(quotas.values())
        })
        value_usd, value_brl = list(), list()
        engine = psqlEngine(self.database)
        connection = engine.connect()
        for asset in list(quotas.keys()):
            if asset in self.fractions:
                # connection = connect(self.currency_database)
                close_price = read_sql_query("SELECT close FROM {} WHERE ticker = '{}' ORDER BY date DESC LIMIT 1".format(self.currency_database, asset), connection).values.flatten()[0]
                # connection.close()
            elif (asset in self.domestic_options_tickers) or (asset in self.domestic_tickers):
                # engine = psqlEngine(self.database)
                # connection = engine.connect()
                close_price = read_sql_query("SELECT close FROM {} WHERE ticker = '{}' ORDER BY date DESC LIMIT 1".format(self.domestic_database, asset), connection).values.flatten()[0]
                # connection.close()
                # engine.dispose()
            elif asset in self.domestic_funds_tickers:
                close_price = read_csv(self.domestic_funds_path + '{}.csv'.format(asset.lower())).share.iloc[-1]
            else:
                # connection = connect(self.international_database)
                close_price = read_sql_query("SELECT adjusted_close as close FROM {} WHERE ticker = '{}' ORDER BY date DESC LIMIT 1".format(self.international_database, asset), connection).values.flatten()[0]
                # connection.close()
            if domestic == False:
                value_usd.append(close_price * quotas.get(asset))
                value_brl.append(close_price * quotas.get(asset) / self.dollar)
            if domestic == True:
                value_usd.append(close_price * quotas.get(asset) * self.dollar)
                value_brl.append(close_price * quotas.get(asset))
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
        self.portfolio['domestic options'] = self.portfolio_domestic_options
        self.portfolio['domestic funds'] = self.portfolio_domestic_funds
        self.portfolio = concat(self.portfolio)

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

    def insert_weekends(self, df, asset = 'stock'):
        df.set_index('date', inplace = True)
        start, end = df.index[0], df.index[-1]
        start = dt.strptime(start, '%Y-%m-%d').date()
        end = dt.strptime(end, '%Y-%m-%d').date()
        dates = [str(start + timedelta(days = x)) for x in range(0, (end - start).days + 1, 1)]
        df = df.reindex(dates, fill_value = 0)
        df.reset_index(inplace = True)
        close = list()
        if asset == '6040':
            for value in df.interest:
                if value != 0:
                    close.append(value)
                if value == 0:
                    close.append(1.)
            df['interest'] = close
        if asset == 'bond':
            for value in df.portfolio:
                if value != 0:
                    close.append(value)
                if value == 0:
                    close.append(close[-1])
            df['portfolio'] = close
        if asset == 'crypto':
            for value in df.close:
                if value != 0:
                    close.append(value)
                if value == 0:
                    close.append(close[-1])
            df['close'] = close
        if asset == 'stock':
            for value in df.close:
                if value != 0:
                    close.append(value)
                if value == 0:
                    close.append(close[-1])
            df['close'] = close
        return df
    
    def get_concat_dataframe(self, columns, options = True):
        columns_bonds = list()
        for elem in columns:
            if elem == 'share':
                columns_bonds.append('purchase_price')
            elif elem == 'purchase_price':
                columns_bonds.append('purchase_price_dollar')
            else:
                columns_bonds.append(elem)
        domestic_bonds = self.domestic_bonds[columns_bonds].rename(columns = {'purchase_price_dollar': 'purchase_price'})
        if options == True:
            df = concat([domestic_bonds, self.domestic_stocks[columns], self.international_stocks[columns], self.crypto[columns], self.domestic_funds[columns], self.domestic_options[columns]])
        if options == False:
            df = concat([domestic_bonds, self.domestic_stocks[columns], self.international_stocks[columns], self.crypto[columns], self.domestic_funds[columns]])
        return df
    
    def get_portfolio_invested(self, df):
        df.set_index('date', inplace = True)
        start, end = df.index[0], df.index[-1]
        start = dt.strptime(start, '%Y-%m-%d').date()
        end = dt.strptime(end, '%Y-%m-%d').date()

        reference = self.get_concat_dataframe(['date', 'purchase_price'])
        reference = reference.groupby(by = 'date')['purchase_price'].sum()
        reference = DataFrame(reference).reset_index()
        reference['close'] = reference.purchase_price.cumsum()
        reference = reference.loc[(reference.date >= start.strftime('%Y-%m-%d')) & (reference.date <= end.strftime('%Y-%m-%d'))]
        reference = self.insert_weekends(reference)
        reference = reference.drop(columns = {'purchase_price'}).rename(columns = {'close': 'invested'})

        ref_start = dt.strptime(reference.date.iloc[0], '%Y-%m-%d').date()
        ref_end = dt.strptime(reference.date.iloc[-1], '%Y-%m-%d').date()
        dates_beginning = [str(start + timedelta(days = x)) for x in range(0, (ref_start - start).days, 1)]
        dates_end = [str(ref_end + timedelta(days = x)) for x in range(1, (end - ref_end).days + 1, 1)]

        aux = [reference.invested.iloc[0] for _ in range(len(dates_beginning))]
        aux2 = [reference.invested.iloc[-1] for _ in range(len(dates_end))]
        reference = DataFrame({
            'date': dates_beginning + reference.date.to_list() + dates_end,
            'invested': aux + reference.invested.to_list() + aux2,
        })
        return reference.invested.to_list()

    def get_returns(self, df, flag = 'cumulative'):
        reference = self.get_concat_dataframe(['date', 'purchase_price'])
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
        start_domestic_options = self.domestic_options[['date']].sort_values(by = 'date').iloc[0].values[0]
        start_domestic_funds = self.domestic_funds[['date']].sort_values(by = 'date').iloc[0].values[0]
        start_date = min(start_domestic, start_international, start_crypto, start_domestic_options, start_domestic_funds)
        self.start_date = self.kwargs.get('start_date', start_date)
        self.start_date = dt.strptime(self.start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
    
    def get_end_date(self):
        end_spy = self.spy[['date']].sort_values(by = 'date').iloc[-1].values[0]
        end_bova =  self.bova[['date']].sort_values(by = 'date').iloc[-1].values[0]
        df = self.get_concat_dataframe(['date'], options = False)
        quotes = df.index.unique(level = 0).to_list()
        dates = [end_spy, end_bova]
        engine = psqlEngine(self.database)
        connection = engine.connect()
        for quote in quotes:
            if quote in self.international_tickers:
                # connection = connect(self.international_database)
                date = read_sql_query("SELECT date FROM {} WHERE ticker = '{}' ORDER BY date".format(self.international_database, quote), connection).iloc[-1].values[0]
                # connection.close()
            elif quote in self.fractions:
                # connection = connect(self.currency_database)
                date = read_sql_query("SELECT date FROM {} WHERE ticker = '{}' ORDER BY date".format(self.currency_database, quote), connection).iloc[-1].values[0]
                # connection.close()
            elif quote in self.interests:
                date = self.cdi.date.iloc[-1]
            elif quote in self.domestic_funds_tickers:
                date = self.domestic_funds[['date']].sort_values(by = 'date').iloc[-1].values[0]
            else:
                date = read_sql_query("SELECT date FROM {} WHERE ticker = '{}' ORDER BY date".format(self.domestic_database, quote), connection).iloc[-1].values[0]
            dates.append(date)
        connection.close()
        engine.dispose()
        end_date = min(dates)
        self.end_date = self.kwargs.get('end_date', end_date)
        self.end_date = dt.strptime(self.end_date, '%Y-%m-%d').strftime('%Y-%m-%d')

    def get_time_series(self):
        self.get_start_date()
        self.get_end_date()
        dates = [(dt.strptime(self.start_date, '%Y-%m-%d').date() + timedelta(days = k)).strftime('%Y-%m-%d') \
            for k in range((dt.strptime(self.end_date, '%Y-%m-%d').date() - dt.strptime(self.start_date, '%Y-%m-%d').date()).days + 1)]

        dataframe = DataFrame()
        df = self.get_concat_dataframe(['date', 'share'])
        quotes = df.index.unique(level = 0).to_list()
        for quote in quotes:
            if quote in self.interests:
                for data, value in zip(df.loc[quote].date, df.loc[quote].purchase_price):
                    interests = self.cdi.loc[self.cdi.date >= data, ['date', 'interest']]
                    lista = [value * interests.interest.iloc[0]]
                    for interest in interests.interest.iloc[1:]:
                        lista.append(lista[-1] * interest)
                    interests['portfolio'] = lista
                    lista = list()
                    for date, price in zip(interests.date, interests.portfolio):
                        conversion = self.dollar_full.loc[self.dollar_full.date ==  date, 'close'].iloc[0]
                        lista.append(price * conversion)
                    interests['portfolio'] = lista
                    interests.drop(columns = {'interest'}, inplace = True)
                    interests = self.insert_weekends(interests, asset = 'bond')
                    dataframe = concat([dataframe, interests])
            elif quote in self.domestic_funds_tickers:
                prices = self.domestic_funds[['date', 'share']].sort_values(by = 'date')
                prices.rename(columns = {'share': 'close'}, inplace = True)
                lista = list()
                for date, price in zip(prices.date, prices.close):
                    conversion = self.dollar_full.loc[self.dollar_full.date ==  date, 'close'].iloc[0]
                    lista.append(price * conversion)
                prices['portfolio'] = lista
                prices.drop(columns = {'close'}, inplace = True)
                dataframe = concat([dataframe, prices])
            else:
                engine = psqlEngine(self.database)
                connection = engine.connect()
                if quote in self.fractions:
                    # connection = connect(self.currency_database)
                    prices = read_sql_query("SELECT date, close FROM {} WHERE ticker = '{}' ORDER BY date".format(self.currency_database, quote), connection)
                    # connection.close()
                    prices = self.insert_weekends(prices, asset = 'crypto')
                elif quote in self.international_tickers:
                    # connection = connect(self.international_database)
                    prices = read_sql_query("SELECT date, adjusted_close as close FROM {} WHERE ticker = '{}' ORDER BY date".format(self.international_database, quote), connection)
                    # connection.close()
                    prices = self.insert_weekends(prices)
                else:
                    prices = read_sql_query("SELECT date, close FROM domestic WHERE ticker = '{}' ORDER BY date".format(quote), connection).drop_duplicates('date')
                    prices = self.insert_weekends(prices)
                    lista = list()
                    for date, price in zip(prices.date, prices.close):
                        conversion = self.dollar_full.loc[self.dollar_full.date ==  date, 'close']
                        lista.append(price * conversion)
                    prices['close'] = lista
                connection.close()
                engine.dispose()
                for data, share in zip(df.loc[quote].date, df.loc[quote].share):
                    close_price = prices.loc[prices.date >=  data]
                    close_price['portfolio'] = [price * share for price in close_price.close]
                    dataframe = concat([dataframe, close_price])
        dataframe = dataframe.groupby(by = ['date']).sum().drop(columns = {'close'})
        dataframe = DataFrame(dataframe).loc[(dataframe.index >= self.start_date) & (dataframe.index <= self.end_date)]
        self.portfolio_time_series = DataFrame()
        self.portfolio_time_series['date'] = dates
        self.portfolio_time_series['portfolio'] = dataframe.portfolio.to_list()
        self.portfolio_time_series['portfolio_invested'] = self.get_portfolio_invested(self.portfolio_time_series)
        self.portfolio_time_series['SPY'] = self.spy.loc[(self.spy.date >= self.start_date) & (self.spy.date <= self.end_date), 'close'].to_list()
        self.portfolio_time_series['BOVA11'] = self.bova.loc[(self.bova.date >= self.start_date) & (self.bova.date <= self.end_date)]['close_dollar'].to_list()
        self.portfolio_time_series.sort_values(by = 'date', inplace = True)
        self.portfolio_time_series['return_portfolio'] = self.get_returns(self.portfolio_time_series)
        self.portfolio_time_series['return_SPY'] = 100 * ((self.portfolio_time_series.SPY.pct_change() + 1).fillna(1).cumprod() - 1)
        self.portfolio_time_series['return_BOVA11'] = 100 * ((self.portfolio_time_series.BOVA11.pct_change() + 1).fillna(1).cumprod() - 1)
        self.get_return_benchmark_portfolio()
        self.portfolio_time_series['port_bench'] = self.benchmark_portfolio.portfolio.to_list()
        self.portfolio_time_series['return_port_bench'] = [0] + 100 *((self.benchmark_portfolio.portfolio.pct_change() + 1).fillna(1).cumprod() - 1)
        self.portfolio_time_series['cagr_portfolio'] = self.get_returns(self.portfolio_time_series, flag = 'cagr')
        self.portfolio_time_series['cagr_SPY'] = [0] + [100 * ((cagr / self.portfolio_time_series.SPY.iloc[0]) ** (250 / k) - 1) for k, cagr in enumerate(self.portfolio_time_series.SPY.iloc[1:], 1)]
        self.portfolio_time_series['cagr_BOVA11'] = [0] + [100 * ((cagr / self.portfolio_time_series.BOVA11.iloc[0]) ** (250 / k) - 1) for k, cagr in enumerate(self.portfolio_time_series.BOVA11.iloc[1:], 1)]
        self.portfolio_time_series['cagr_port_bench'] = [0] + [100 * ((cagr / self.benchmark_portfolio.portfolio.iloc[0]) ** (250 / k) - 1) for k, cagr in enumerate(self.benchmark_portfolio.portfolio.iloc[1:], 1)]
        self.portfolio_time_series.drop(columns = {'index'}, inplace = True)
        self.portfolio_time_series.set_index('date', inplace = True)
