#%%
from time import sleep
from sqlite3 import connect

from pandas import read_sql_query
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.foreignexchange import ForeignExchange


class Update_Assets():
    def __init__(self, asset, name = 'asset_import', **kwargs):
        self.database = kwargs.get('database', 'database.db')
        self.timer = False

        self.asset = asset
        self.check_assets()

        self.get_asset_list()
        self.get_credentials()

        self.update_asset_database()
    
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
        key = '12FCWWSQ0N28V8QV'
        self.ts = TimeSeries(key = key, output_format = 'pandas')
        self.fx = ForeignExchange(key = key, output_format = 'pandas')

    def get_asset_list(self):
        connection = connect(self.database)
        self.asset_list = read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", connection).name
        self.asset_list = list(self.asset_list)
        self.asset_list.sort()
        connection.close()

    def rename_reset(self, df):
        keys = list(df.columns)
        values = [item[3:].replace(' ', '_') for item in list(df.columns)]
        dictionary = dict(zip(keys, values))
        df.rename(columns = dictionary, inplace = True)
        df.reset_index(inplace = True)
        return df

    def update_stocks(self, dataframe, ticker, flag):
        dataframe = self.rename_reset(dataframe)
        dataframe['date'] = [elem.date().strftime('%Y-%m-%d') for elem in dataframe.date]
        connection = connect(self.database)
        if flag == 'include':
            dataframe.to_sql(ticker, connection, if_exists = 'replace', index = False)
        if flag == 'update':
            cursor = connection.cursor()
            for k in range(len(dataframe)):
                df = dataframe.iloc[k]
                lista = list(df.values)
                lista, aux = lista[1:], lista[0]
                lista.append(aux)
                del aux
                cursor.execute(
                    'UPDATE {} SET open = (?), high = (?), low = (?), close = (?), \
                    adjusted_close = (?), volume = (?), dividend_amount = (?), \
                    split_coefficient = (?) WHERE date = (?)'.format(ticker), (tuple(lista))
                )
            connection.commit()
        connection.close()
    
    def update_fx(self, from_symbol, to_symbol):
        ticker = from_symbol + to_symbol
        if ticker not in self.asset_list:
            dataframe, _ = self.fx.get_currency_exchange_daily(from_symbol = from_symbol, to_symbol = to_symbol, outputsize = 'full')
        else:
            dataframe, _ = self.fx.get_currency_exchange_daily(from_symbol = from_symbol, to_symbol = to_symbol)
        sleep(30.0)
        dataframe = self.rename_reset(dataframe)
        dataframe['date'] = [elem.date().strftime('%Y-%m-%d') for elem in dataframe.date]
        if ticker in self.asset_list:
            connection = connect('database.db')
            new = ticker + '_new'
            dataframe.to_sql(new, connection, if_exists = 'replace', index = False)
            cursor = connection.cursor()
            cursor.execute('DELETE FROM {} WHERE rowId = 0 AND rowId = 1 AND rowId = 2'.format(ticker))
            connection.commit()
            dataframe = read_sql_query('SELECT * FROM {} UNION SELECT * FROM {} ORDER BY date DESC'.format(ticker, new),connection)
            cursor.execute('DROP TABLE {}'.format(new))
            connection.commit()
            connection.close()
            lista = list()
            for elem in dataframe.date:
                if type(elem) is not str:
                    lista.append(elem.date().strftime('%Y-%m-%d'))
                else:
                    lista.append(elem)
            dataframe['date'] = lista

        connection = connect('database.db')
        dataframe.to_sql(ticker, connection, if_exists = 'replace', index = False)
        connection.close()

    def update_asset_database(self):
        count = 0
        self.update_fx(from_symbol = 'BRL', to_symbol = 'USD')
        self.update_fx(from_symbol = 'USD', to_symbol = 'BRL')
        for ticker in self.asset:
            if (count % 4 == 0) and (count != 0):
                sleep(60.0)
            if ticker in self.asset_list:
                old_asset, _ = self.ts.get_daily_adjusted(symbol = ticker)
                old_asset = old_asset[:2]
                self.update_stocks(old_asset, ticker, 'update')
            if ticker not in self.asset_list:
                new_asset, _ = self.ts.get_daily_adjusted(symbol = ticker, outputsize = 'full')
                self.update_stocks(new_asset, ticker, 'include')
            count +=1

# Update_Assets('twtr')