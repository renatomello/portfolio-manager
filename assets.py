from time import sleep
from sqlite3 import connect

from pandas import read_sql_query, read_csv

from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.cryptocurrencies import CryptoCurrencies
from alpha_vantage.foreignexchange import ForeignExchange

from functions import psqlEngine


class Update_Assets():
    def __init__(self, asset = '', name = 'asset_import', **kwargs):
        self.kwargs = kwargs
        self.hyperparameters()
        self.get_asset_list()
        self.get_fx_crypto_list()
        self.asset = self.asset_list if asset == '' else asset
        self.check_assets()
        self.get_credentials()
        self.update_asset_database()

    def hyperparameters(self):
        self.key = self.kwargs.get('key')
        self.database = self.kwargs.get('database', 'database.ini')
        self.asset_class = self.kwargs.get('asset_class', 'currencies')

        self.currencies = self.kwargs.get('currencies', 'currencies.csv')
        self.currencies = read_csv(self.currencies)

        self.cryptos = self.kwargs.get('cryptos', 'cryptos.csv')
        self.cryptos = read_csv(self.cryptos)

        self.timer = False

    def check_assets(self):
        if (isinstance(self.asset, str) == False) and (isinstance(self.asset, list) == False):
            raise Exception('{} not {} nor {}'.format(self.asset, str, list))
        if isinstance(self.asset, list) == True:
            if any(isinstance(item, str) == False for item in self.asset):
                raise Exception('At least one element in {} is not {}'.format(self.asset, str))
            else:
                self.asset = [elem.upper() for elem in self.asset]
                if len(self.asset) > 4:
                    self.timer = True
        if isinstance(self.asset, str) == True:
            self.asset = [self.asset.upper()]

    def get_credentials(self):
        self.ts = TimeSeries(key = self.key, output_format = 'pandas')
        self.fx = ForeignExchange(key = self.key, output_format = 'pandas')
        self.cc = CryptoCurrencies(key = self.key, output_format='pandas')

    def get_fx_crypto_list(self):
        self.fx_list, self.crypto_list = list(), list()
        for ticker in self.asset_list:
            currency_from, currency_to = ticker[:3], ticker[3:]
            if (currency_from in self.currencies.currency_code.to_list()) & (currency_to in self.currencies.currency_code.to_list()):
                self.fx_list.append(ticker)
            if (currency_from in self.cryptos.currency_code.to_list()) & (currency_to in self.currencies.currency_code.to_list()):
                self.crypto_list.append(ticker)

    def get_asset_list(self):
        engine = psqlEngine(self.database)
        connection = engine.connect()
        self.asset_list = read_sql_query('SELECT DISTINCT ticker FROM {} ORDER BY ticker'.format(self.asset_class), connection).ticker
        # self.asset_list = read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", connection).name
        connection.close()
        engine.dispose()
        self.asset_list = list(self.asset_list)
        self.asset_list.sort()
        if isinstance(self.asset_list, list):
            try:
                self.asset_list.remove('aggregate')
                self.asset_list.remove('portfolio')
            except ValueError:
                pass

    def rename_reset(self, df, crypto = False):
        keys = list(df.columns)
        if crypto == False:
            values = [item[3:].replace(' ', '_') for item in list(df.columns)]
        if crypto == True:
            values = [item[3:].replace(' (USD)', '').replace(' ', '_') for item in list(df.columns)]
            for k, item in enumerate(values):
                if item.startswith('_'):
                    values[k] = item.replace('_', '')
        dictionary = dict(zip(keys, values))
        df.rename(columns = dictionary, inplace = True)
        df.reset_index(inplace = True)
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    def update_stocks(self, dataframe, ticker, flag):
        ticker = ticker.replace('.SA', '')
        dataframe = self.rename_reset(dataframe)
        dataframe['date'] = [elem.date().strftime('%Y-%m-%d') for elem in dataframe.date]
        dataframe['ticker'] = [ticker]*len(dataframe)
        if self.asset_class == 'usa_stocks':
            dataframe['currency'] = ['USD']*len(dataframe)
        engine = psqlEngine(self.database)
        connection = engine.raw_connection()
        if flag == 'include':
            dataframe.to_sql(self.asset_class, engine, if_exists = 'append', index = False)
        if flag == 'update':
            reference = read_sql_query("SELECT date FROM {} WHERE {}.ticker LIKE '%%{}%%' ORDER BY date DESC LIMIT 1".format(self.asset_class, self.asset_class, ticker), connection)
            reference = reference.values[0][0]
            cursor = connection.cursor()
            cursor.execute("DELETE FROM {} WHERE date LIKE '%%{}%%' AND ticker LIKE '%%{}%%'".format(self.asset_class, reference, ticker))
            connection.commit()
            cursor.close()
            dataframe = dataframe.loc[dataframe.date >= reference]
            dataframe.to_sql(self.asset_class, engine, if_exists = 'append', index = False)
        connection.close()
        engine.dispose()
    
    def update_fx(self, df, currency):
        print('FX: {}'.format(currency))
        df = self.rename_reset(df)
        df['date'] = [elem.date().strftime('%Y-%m-%d') for elem in df.date]
        df['ticker'] = [currency]*len(df)
        engine = psqlEngine(self.database)
        connection = engine.raw_connection()
        if currency not in self.fx_list:
            df.to_sql(self.asset_class, connection, if_exists = 'append', index = False)
        if currency in self.fx_list:
            reference = read_sql_query("SELECT date FROM {} WHERE {}.ticker LIKE '%%{}%%' ORDER BY date DESC LIMIT 1".format(self.asset_class, self.asset_class, currency), connection)
            reference = reference.values[0][0]
            cursor = connection.cursor()
            cursor.execute("DELETE FROM {} WHERE date LIKE '%%{}%%' AND ticker LIKE '%%{}%%'".format(self.asset_class, reference, currency))
            connection.commit()
            cursor.close()
            df = df.loc[df.date >= reference]
            df.to_sql(self.asset_class, engine, if_exists = 'append', index = False)
        connection.close()
        engine.dispose()

    def update_crypto(self, df, currency):
        print('Crypto: {}'.format(currency))
        df = self.rename_reset(df, crypto = True)
        df['date'] = [elem.date().strftime('%Y-%m-%d') for elem in df.date]
        df['ticker'] = [currency]*len(df)
        engine = psqlEngine(self.database)
        connection = engine.raw_connection()
        if currency not in self.crypto_list:
            df.to_sql(self.asset_class, connection, if_exists = 'append', index = False)
        if currency in self.crypto_list:
            reference = read_sql_query("SELECT date FROM {} WHERE {}.ticker LIKE '%%{}%%' ORDER BY date DESC LIMIT 1".format(self.asset_class, self.asset_class, currency), connection)
            reference = reference.values[0][0]
            cursor = connection.cursor()
            cursor.execute("DELETE FROM {} WHERE date LIKE '%%{}%%' AND ticker like '%%{}%%'".format(self.asset_class, reference, currency))
            connection.commit()
            cursor.close()
            df = df.loc[df.date >= reference]
            df.to_sql(self.asset_class, engine, if_exists = 'append', index = False)
        connection.close()
        engine.dispose()

    def update_stock_database(self, ticker):
        print('Stock: {}'.format(ticker))
        if any(letter.isdigit() for letter in ticker) == True:
            ticker = ticker + '.SA'
        if ticker in self.asset_list:
            old_asset, _ = self.ts.get_daily_adjusted(symbol = ticker)
            self.update_stocks(old_asset, ticker, 'update')
        if ticker not in self.asset_list:
            new_asset, _ = self.ts.get_daily_adjusted(symbol = ticker, outputsize = 'full')
            self.update_stocks(new_asset, ticker, 'include')

    def update_fx_database(self, currency_from, currency_to):
        currency = currency_from + currency_to
        if currency not in self.fx_list:
            df, _ = self.fx.get_currency_exchange_daily(from_symbol = currency_from, to_symbol = currency_to, outputsize = 'full')
        if currency in self.fx_list:
            df, _ = self.fx.get_currency_exchange_daily(from_symbol = currency_from, to_symbol = currency_to)
        self.update_fx(df, currency)

    def update_crypto_database(self, currency_from, currency_to):
        currency = currency_from + currency_to
        df, _ = self.cc.get_digital_currency_daily(symbol = currency_from, market = currency_to)
        self.update_crypto(df, currency)

    def update_asset_database(self):
        print('Updating database...')
        for count, ticker in enumerate(self.asset):
            if (count % 2 == 0) and (count != 0) and (count != len(self.asset) - 1):
                sleep(30.0)
            currency_from, currency_to = ticker[:3], ticker[3:]
            if (currency_from in self.currencies.currency_code.to_list()) & (currency_to in self.currencies.currency_code.to_list()):
                self.update_fx_database(currency_from = currency_from, currency_to = currency_to)
            elif (currency_from in self.cryptos.currency_code.to_list()) & (currency_to in self.currencies.currency_code.to_list()):
                self.update_crypto_database(currency_from = currency_from, currency_to = currency_to)
            else:
                self.update_stock_database(ticker)
