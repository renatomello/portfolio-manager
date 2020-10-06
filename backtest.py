#%%
from datetime import datetime as dt
from pandas import read_sql_query, read_csv
from functions import psqlEngine

class BackTest():
    def __init__(self, name = 'backtest', **kwargs):
        self.kwargs = kwargs
        self.hyperparameters()
        self.get_connection()
        self.get_benchmarks()
        self.get_puts()
        self.close_connection()

    def hyperparameters(self):
        self.database = self.kwargs.get('database', 'database.ini')
        self.calendar = self.kwargs.get('calendar_options', 'options_calendar.csv')
        self.start_date = self.kwargs.get('start_date', '{}-01-01'.format(dt.now().year))
        self.end_date = self.kwargs.get('end_date', dt.now().date().strftime('%Y-%m-%d'))
    
    def get_connection(self):
        self.engine = psqlEngine(self.database)
        self.connection = self.engine.connect()
    
    def close_connection(self):
        self.connection.close()
        self.engine.dispose()

    def get_benchmarks(self):
        self.calendar_options = read_csv(self.calendar)
        self.calendar_options['ticker'] = ['BOVA{}'.format(letter) for letter in self.calendar_options.put]
        self.bova = read_sql_query("SELECT date, close FROM domestic WHERE ticker = 'BOVA11' ORDER BY date", self.connection)
        self.bova = self.bova.loc[(self.bova.date >= self.start_date) & (self.bova.date <= self.end_date)]

    def get_puts(self):
        self.puts = self.bova
        self.puts['price_10'] = [0.9 * price for price in self.puts.close]
        self.puts['price_30'] = [0.7 * price for price in self.puts.close]
        # print(self.puts)
        # print(self.calendar_options)
        df = read_sql_query("SELECT date, ticker, mean, close, expiration_date FROM domestic WHERE ticker LIKE '%%{}%%' ORDER BY date".format('BOVAM'), self.connection)
        df = df.loc[df.date.str.contains('2020')]
        df['ticker_target'] = [float(ticker.replace('BOVAM', '')) for ticker in df.ticker]
        df = df.iloc[(df.ticker_target - self.puts.price_10.iloc[0]).abs().argsort()[:2]]
        print(df)

backtest = BackTest()