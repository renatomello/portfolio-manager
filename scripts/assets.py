from os import listdir
from sys import stdout
from time import sleep
from sqlite3 import connect

from pandas import read_sql_query, read_csv, read_table

from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.cryptocurrencies import CryptoCurrencies
from alpha_vantage.foreignexchange import ForeignExchange

from functions import psqlEngine


class Update_Assets():
    def __init__(self, asset = '', name = 'asset_import', **kwargs):
        self.kwargs = kwargs
        self.hyperparameters()
        self.get_connection()
        self.get_asset_list()
        self.get_fx_crypto_list()
        self.asset = self.asset_list if asset == '' else asset
        self.check_assets()
        self.get_credentials()
        self.update_asset_database()
        self.close_connection()


    def hyperparameters(self):
        self.key = self.kwargs.get('key')
        self.database = self.kwargs.get('database', 'database.ini')
        self.asset_class = self.kwargs.get('asset_class', 'currencies')

        self.currencies = self.kwargs.get('currencies', '../support_data/currencies.csv')
        self.currencies = read_csv(self.currencies)

        self.cryptos = self.kwargs.get('cryptos', '../support_data/cryptos.csv')
        self.cryptos = read_csv(self.cryptos)

        self.timer = False

        self.directory = self.kwargs.get('directory', '/home/renato/Desktop/dados históricos/')
    

    def get_connection(self):
        self.engine = psqlEngine(self.database)
        self.connection = self.engine.raw_connection()
        self.cursor = self.connection.cursor()
    

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        self.engine.dispose()


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
        self.cc = CryptoCurrencies(key = self.key, output_format = 'pandas')


    def get_fx_crypto_list(self):
        self.fx_list, self.crypto_list = list(), list()
        for ticker in self.asset_list:
            currency_from, currency_to = ticker[:3], ticker[3:]
            if (currency_from in self.currencies.currency_code.to_list()) & (currency_to in self.currencies.currency_code.to_list()):
                self.fx_list.append(ticker)
            if (currency_from in self.cryptos.currency_code.to_list()) & (currency_to in self.currencies.currency_code.to_list()):
                self.crypto_list.append(ticker)


    def get_asset_list(self):
        self.asset_list = read_sql_query('SELECT DISTINCT ticker FROM {} ORDER BY ticker'.format(self.asset_class), self.connection).ticker
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
            values = [item[3:].replace(' ', '_') for item in keys]
        if crypto == True:
            values = [item[3:].replace(' (USD)', '').replace(' ', '_') for item in keys]
            for k, item in enumerate(values):
                if item.startswith('_'):
                    values[k] = item.replace('_', '')
        dictionary = dict(zip(keys, values))
        df.rename(columns = dictionary, inplace = True)
        df.reset_index(inplace = True)
        df = df.loc[:, ~df.columns.duplicated()]
        return df
    

    def update_benchmarks(self):
        print('\nBenchmarks\n')
        list_of_B3_files = listdir(self.directory)
        for filename in list_of_B3_files:
            if (filename.endswith('.TXT')) and (filename.contains('2021')):
                df = read_table(self.directory + filename, encoding = 'ISO-8859-1').iloc[:-1].reset_index()
                df.rename(columns = {df.columns[1]: 'string'}, inplace = True)
                df.drop('index', axis = 1, inplace = True)
                df['date'] = [elem[2:6] + '-' + elem[6:8] + '-' + elem[8:10] for elem in df.string.to_list()]
                df['ticker'] = [elem[12:24].replace(' ', '') for elem in df.string.to_list()]
                df['currency'] = [elem[52:56].replace('R$', 'BRL') for elem in df.string.to_list()]
                df['open'] = [int(elem[56:69]) / 100 for elem in df.string.to_list()]
                df['high'] = [int(elem[69:82]) / 100 for elem in df.string.to_list()]
                df['low'] = [int(elem[82:95]) / 100 for elem in df.string.to_list()]
                df['mean'] = [int(elem[95:108]) / 100 for elem in df.string.to_list()]
                df['close'] = [int(elem[108:121]) / 100 for elem in df.string.to_list()]
                df['totneg'] = [int(elem[147:152]) for elem in df.string.to_list()]
                df['quatneg'] = [int(elem[152:170]) for elem in df.string.to_list()]
                df['volume'] = [int(elem[170:188]) for elem in df.string.to_list()]
                df['expiration_date'] = [elem[202:206] + '-' + elem[206:208] + '-' + elem[208:210] for elem in df.string.to_list()]
                df['split_numerator'] = [1.] * len(df)
                df['split_denominator'] = [1.] * len(df)
                df['adjusted_open'] = df.open.to_list()
                df['adjusted_high'] = df.high.to_list()
                df['adjusted_low'] = df.low.to_list()
                df['adjusted_mean'] = df['mean'].to_list()
                df['adjusted_close'] = df.close.to_list()
                start_date = read_sql_query("SELECT MAX(date) FROM benchmarks WHERE ticker = 'BOVA11'", self.connection).values[0][0]
                dataf = df.loc[(df.date > start_date) & (df.ticker == 'BOVA11')]
                dataf.to_sql('benchmarks', self.connection, if_exists = 'append', index = False)
                del df, start_date, dataf
        start_date = read_sql_query("SELECT MAX(date) FROM benchmarks WHERE ticker = '{}'".format('BRLUSD'), self.connection).values[0][0]
        df = read_sql_query("SELECT * FROM currencies WHERE ticker = '{}' AND date > '{}' ORDER BY date".format('BRLUSD', start_date), self.connection)
        df.to_sql('benchmarks', self.engine, if_exists='append', index = False)
        start_date = read_sql_query("SELECT MAX(date) FROM benchmarks WHERE ticker = '{}'".format('SPY'), self.connection).values[0][0]
        df = read_sql_query("SELECT * FROM usa_stocks WHERE ticker = '{}' AND date > '{}' ORDER BY date".format('SPY', start_date), self.connection)
        df.to_sql('benchmarks', self.engine, if_exists = 'append', index = False)


    def update_stocks(self, dataframe, ticker, flag):
        ticker = ticker.replace('.SA', '')
        dataframe = self.rename_reset(dataframe)
        dataframe['date'] = [elem.date().strftime('%Y-%m-%d') for elem in dataframe.date]
        dataframe['ticker'] = [ticker]*len(dataframe)
        if self.asset_class == 'usa_stocks':
            dataframe['currency'] = ['USD']*len(dataframe)
        if self.asset_class == 'uk_stocks':
            dataframe['currency'] = ['GBP']*len(dataframe)
        if flag == 'include':
            dataframe.to_sql(self.asset_class, self.engine, if_exists = 'append', index = False)
        if flag == 'update':
            reference = read_sql_query("SELECT date FROM {} WHERE {}.ticker LIKE '%%{}%%' ORDER BY date DESC LIMIT 1".format(self.asset_class, self.asset_class, ticker), self.connection)
            reference = reference.values[0][0]
            self.cursor.execute("DELETE FROM {} WHERE date LIKE '%%{}%%' AND ticker LIKE '%%{}%%'".format(self.asset_class, reference, ticker))
            self.connection.commit()
            dataframe = dataframe.loc[dataframe.date >= reference]
            dataframe.to_sql(self.asset_class, self.engine, if_exists = 'append', index = False)


    def update_fx(self, df, currency):
        print_string = 'FX: {}'.format(currency)
        stdout.write('\r\x1b[K' + print_string)
        stdout.flush()
        df = self.rename_reset(df)
        df['date'] = [elem.date().strftime('%Y-%m-%d') for elem in df.date]
        df['ticker'] = [currency]*len(df)
        if currency not in self.fx_list:
            df.to_sql(self.asset_class, self.engine, if_exists = 'append', index = False)
        if currency in self.fx_list:
            reference = read_sql_query("SELECT date FROM {} WHERE {}.ticker LIKE '%%{}%%' ORDER BY date DESC LIMIT 1".format(self.asset_class, self.asset_class, currency), self.connection)
            reference = reference.values[0][0]
            self.cursor.execute("DELETE FROM {} WHERE date LIKE '%%{}%%' AND ticker LIKE '%%{}%%'".format(self.asset_class, reference, currency))
            df = df.loc[df.date >= reference]
            df.to_sql(self.asset_class, self.engine, if_exists = 'append', index = False)


    def update_crypto(self, df, currency):
        print_string = 'Crypto: {}'.format(currency)
        stdout.write('\r\x1b[K' + print_string)
        stdout.flush()
        df = self.rename_reset(df, crypto = True)
        df['date'] = [elem.date().strftime('%Y-%m-%d') for elem in df.date]
        df['ticker'] = [currency]*len(df)
        if currency not in self.crypto_list:
            df.to_sql(self.asset_class, self.engine, if_exists = 'append', index = False)
        if currency in self.crypto_list:
            reference = read_sql_query("SELECT date FROM {} WHERE {}.ticker LIKE '%%{}%%' ORDER BY date DESC LIMIT 1".format(self.asset_class, self.asset_class, currency), self.connection)
            reference = reference.values[0][0]
            self.cursor.execute("DELETE FROM {} WHERE date LIKE '%%{}%%' AND ticker like '%%{}%%'".format(self.asset_class, reference, currency))
            df = df.loc[df.date >= reference]
            df.to_sql(self.asset_class, self.engine, if_exists = 'append', index = False)


    def update_stock_database(self, ticker):
        print_string = 'Stock: {}'.format(ticker.replace('.LON', ''))
        stdout.write('\r\x1b[K' + print_string)
        stdout.flush()
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
        print('\nUpdating database...')
        if self.asset_class == 'benchmarks':
            self.update_benchmarks()
        else:
            for ticker in self.asset:
                currency_from = ticker[:4] if len(ticker) > 6 else ticker[:3]
                currency_to = ticker[4:] if len(ticker) > 6 else ticker[3:]
                if (currency_from in self.currencies.currency_code.to_list()) & (currency_to in self.currencies.currency_code.to_list()):
                    self.update_fx_database(currency_from = currency_from, currency_to = currency_to)
                elif (currency_from in self.cryptos.currency_code.to_list()) & (currency_to in self.currencies.currency_code.to_list()):
                    self.update_crypto_database(currency_from = currency_from, currency_to = currency_to)
                else:
                    self.update_stock_database(ticker)
                sleep(15.1)
