#%%
from os import listdir
from pandas import read_table, read_sql_query
from functions import psqlEngine

directory = '/home/renato/Desktop/dados históricos/'
db_config, databases = 'database.ini', ['domestic', 'domestic_backup']

engine = psqlEngine(db_config)
connection = engine.connect()
for filename in listdir(directory):
    if filename.endswith('.TXT'):
        print(filename.split('/')[-1].replace('COTAHIST_A', '').replace('.TXT', ''))
        df = read_table(directory + filename, encoding = 'ISO-8859-1').iloc[:-1].reset_index()
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
        for db in databases:
            print(db)
            start_date = read_sql_query("SELECT date FROM {} WHERE ticker = 'BOVA11' ORDER BY date DESC LIMIT 1".format(db), connection).values[0][0]
            df = df.sort_values('ticker').loc[df.date > start_date]
            df.to_sql('{}'.format(db), connection, if_exists = 'append', index = False)
        print('Complete')
connection.close()
engine.dispose()
