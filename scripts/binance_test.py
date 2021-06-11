#%%
from math import log10, floor
from decimal import Decimal
from time import sleep
from random import randint
from datetime import date, datetime as dt
from pandas import DataFrame, concat


from secret import binance_api_key, binance_api_secret
from binance.client import Client
from binance.enums import *

import matplotlib.pyplot as plt

client = Client(binance_api_key, binance_api_secret)

SECONDS_IN_HOUR = 3600

#%%
'''
columns = [
    'open_timestamp', 'open', 'high', 'low', 'close', 'volume', 
    'close_timestamp', 'quote_asset_volume', 'number_of_trades', 
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'can_be_ignored'
    ]
tickers = [
    'BUSDBRL', 'BTCBUSD', 'BTCBRL',
    # 'BTCUSDT', 'USDTBRL', 'BUSDUSDT', 
    # 'ADABRL', 'ADAUSDT', 'ADABUSD',  
    # 'ETHBUSD', 'ETHUSDT', 'ETHBRL',
    # 'BNBBUSD', 'BNBUSDT', 'BNBBRL', 
    # 'BNBETH', 'BNBBTC', 'ADABNB',
]
fees = client.get_trade_fee()
fees = DataFrame(fees['tradeFee'])
fees = fees.loc[fees.symbol.isin(tickers)]
fees

#%%
dictionary = dict()
for ticker in tickers:
    records = client.get_historical_klines(ticker, Client.KLINE_INTERVAL_4HOUR, '1 Nov, 2020')
    records = DataFrame.from_records(records, columns = columns)
    records['date_open'] = [dt.fromtimestamp(int(str(x)[:-3])) for x in records.open_timestamp]
    records['date_close'] = [dt.fromtimestamp(int(str(x)[:-3])) for x in records.close_timestamp]
    records['ticker'] = [ticker]*len(records)
    dictionary[ticker] = records
df = concat(dictionary)
del dictionary, records
df.sort_values(['ticker', 'date_open'], inplace = True)
df

#%%
busdbrl = df.loc['BUSDBRL']
btcbrl = df.loc['BTCBRL']
btcbusd = df.loc['BTCBUSD']
# busdusd = df.loc['BUSDUSDT']
# btcusd = df.loc['BTCUSDT']
# usdbrl = df.loc['USDTBRL']
# usdtbrl = df.loc['USDTBRL']
# ethusd = df.loc['ETHUSDT']
# ethbusd = df.loc['ETHBUSD']
# ethbrl = df.loc['ETHBRL']
# bnbbusd = df.loc['BNBBUSD']
# bnbbtc = df.loc['BNBBTC']
# bnbusd = df.loc['BNBUSDT']
# bnbeth = df.loc['BNBETH']
# bnbbrl = df.loc['BNBBRL']

date_min = str(max([
    # '2020-11-01'
    busdbrl.date_close.min(), btcbrl.date_close.min(), btcbusd.date_close.min(),
    # busdusd.date_close.min(), btcusd.date_close.min(), usdbrl.date_close.min(),  
    # usdtbrl.date_close.min(), ethbrl.date_close.min(), ethbusd.date_close.min(), 
    # ethusd.date_close.min(), bnbbtc.date_close.min(), bnbbusd.date_close.min(),
    # bnbeth.date_close.min(), bnbbrl.date_close.min()
]))
date_max = str(min([
    busdbrl.date_close.max(), btcbrl.date_close.max(), btcbusd.date_close.max(), 
    # busdusd.date_close.max(), btcusd.date_close.max(), usdbrl.date_close.max(),  
    # usdtbrl.date_close.max(), ethbrl.date_close.max(), ethbusd.date_close.max(), 
    # ethusd.date_close.max(), bnbbtc.date_close.max(), bnbbusd.date_close.max(),
    # bnbeth.date_close.max(), bnbbrl.date_close.max()
]))

busdbrl = busdbrl.loc[(busdbrl.date_close >= date_min) & (busdbrl.date_close <= date_max)]
btcbrl = btcbrl.loc[(btcbrl.date_close >= date_min) & (btcbrl.date_close <= date_max)]
btcbusd = btcbusd.loc[(btcbusd.date_close >= date_min) & (btcbusd.date_close <= date_max)]
# busdusd = busdusd.loc[(busdusd.date_close >= date_min) & (busdusd.date_close <= date_max)]
# btcusd = btcusd.loc[(btcusd.date_close >= date_min) & (btcusd.date_close <= date_max)]
# usdbrl = usdbrl.loc[(usdbrl.date_close >= date_min) & (usdbrl.date_close <= date_max)]
# usdtbrl = usdtbrl.loc[(usdtbrl.date_close >= date_min) & (usdtbrl.date_close <= date_max)]
# bnbbtc = bnbbtc.loc[(bnbbtc.date_close >= date_min) & (bnbbtc.date_close <= date_max)]
# bnbbusd = bnbbusd.loc[(bnbbusd.date_close >= date_min) & (bnbbusd.date_close <= date_max)]
# bnbeth = bnbeth.loc[(bnbeth.date_close >= date_min) & (bnbeth.date_close <= date_max)]
# bnbbrl = bnbbrl.loc[(bnbbrl.date_close >= date_min) & (bnbbrl.date_close <= date_max)]
busdbrl

#%%
busdbrl = busdbrl.loc[busdbrl.quote_asset_volume >= str(1e-5)]
btcbrl = btcbrl.loc[btcbrl.date_close.isin(busdbrl.date_close.to_list())]
btcbusd = btcbusd.loc[btcbusd.date_close.isin(busdbrl.date_close.to_list())]

#%%
# tax = 0.0006 / 2
fee_flag = 1.
balance_busd, balance_bnb  = 1e3, 50.
count_busd, count_usdt = 0., 0.
vol_btc_busd, vol_btc_usdt = 0., 0.
records_busd, records_usd = list(), list()
for k in range(len(busdbrl) - 1):
    initial_dollar = balance_busd# * .5
    brl_to_busd = (initial_dollar / busdbrl.open.astype(float).iloc[k]) * (1 - fee_flag * fees.loc[fees.symbol == 'BUSDBRL', 'maker'].iloc[0])
    busd_to_btc = (brl_to_busd / btcbusd.close.astype(float).iloc[k]) * (1 - fee_flag * fees.loc[fees.symbol == 'BTCBUSD', 'maker'].iloc[0])
    btc_to_brl_busd = (busd_to_btc * btcbrl.open.astype(float).iloc[k + 1]) * (1 - fee_flag * fees.loc[fees.symbol == 'BTCBRL', 'maker'].iloc[0])
    balance_busd += (btc_to_brl_busd - initial_dollar)
    vol_btc_busd += busd_to_btc
    # bnb_brl_to_busd = tax * brl_to_busd * bnbbusd.loc[bnbbusd.date_close == busdbrl.date_close.iloc[k], 'open'].astype(float).iloc[0]
    # bnb_busd_to_btc = tax * busd_to_btc * bnbbtc.loc[bnbbtc.date_close == btcbusd.date_close.iloc[k], 'close'].astype(float).iloc[0]
    # bnb_btc_to_brl_busd = tax * btc_to_brl_busd * bnbbrl.loc[bnbbrl.date_close == btcbrl.date_close.iloc[k], 'close'].astype(float).iloc[0]
    # balance_bnb = balance_bnb - (bnb_brl_to_busd + bnb_busd_to_btc + bnb_btc_to_brl_busd)
    records_busd.append([
        btcbrl.date_close.iloc[k], brl_to_busd, busd_to_btc, btc_to_brl_busd, 
        balance_busd, 1 - initial_dollar / btc_to_brl_busd, vol_btc_busd, 
        # balance_bnb, balance_bnb / bnbbrl.loc[bnbbrl.date_close == btcbrl.date_close.iloc[k], 'close'].astype(float).iloc[0]
    ])

    # initial_dollar = balance_usdt * .2
    # brl_to_usd = (initial_dollar / usdbrl.open.astype(float).iloc[k])  * (1 - 0.5 * fees.loc[fees.symbol == 'USDTBRL', 'taker'].iloc[0])
    # usd_to_btc = (brl_to_usd / btcusd.close.astype(float).iloc[k]) * (1 - 0.5 * fees.loc[fees.symbol == 'BTCUSDT', 'taker'].iloc[0])
    # btc_to_brl_usd = (usd_to_btc * btcbrl.close.astype(float).iloc[k + 1]) * (1 - 0.5 * fees.loc[fees.symbol == 'BTCBRL', 'taker'].iloc[0])
    # balance_usdt += (btc_to_brl_usd - initial_dollar)
    # vol_btc_usdt += usd_to_btc
    # records_usd.append([btcbrl.date_close.iloc[k], brl_to_usd, usd_to_btc, btc_to_brl_usd, balance_usdt, 1 - initial_dollar / btc_to_brl_usd, vol_btc_usdt])

    # initial_dollar = balance_busd# * .5
    # brl_to_busd = (initial_dollar / busdbrl.open.astype(float).iloc[k]) * (1 - fees.loc[fees.symbol == 'BUSDBRL', 'maker'].iloc[0])
    # busd_to_eth = (brl_to_busd / ethbusd.close.astype(float).iloc[k]) * (1 - fees.loc[fees.symbol == 'ETHBUSD', 'maker'].iloc[0])
    # eth_to_brl_busd = (busd_to_eth * ethbrl.close.astype(float).iloc[k + 1]) * (1 - fees.loc[fees.symbol == 'ETHBRL', 'maker'].iloc[0])
    # balance_usdt += (eth_to_brl_busd - initial_dollar)
    # vol_btc_usdt += busd_to_eth
    # records_usd.append([ethbrl.date_close.iloc[k], brl_to_busd, busd_to_eth, eth_to_brl_busd, balance_usdt, 1 - initial_dollar / eth_to_brl_busd, vol_btc_busd])


    if btc_to_brl_busd > initial_dollar: count_busd += 1
    # if eth_to_brl_busd > initial_dollar: count_usdt += 1

print('{:.3f} {:.3f} {:.3f}'.format(count_busd, len(busdbrl), count_busd / len(busdbrl)))
# print(count_usdt, len(usdbrl), count_usdt / len(usdbrl))

dictionary = dict()
dictionary['arb_busd'] = DataFrame.from_records(records_busd, columns = ['datetime', 'real_to_dollar', 'dollar_to_btc', 'btc_to_real', 'balance', 'return', 'volume_btc'])#, 'balance_bnb', 'balance_bnb_brl'])
# dictionary['arb_usdt'] = DataFrame.from_records(records_usd, columns = ['datetime', 'real_to_dollar', 'dollar_to_btc', 'btc_to_real', 'balance', 'return', 'volume_btc', 'balance_bnb', 'balance_bnb_brl'])
results = concat(dictionary)
del dictionary

#%%
# plt.plot(results.loc['arb_usdt'].datetime, 100 * (((1 + results.loc['arb_usdt']['return']).cumprod()) - 1), label = 'BRL --> USDT --> BTC --> BRL')
plt.plot(results.loc['arb_busd'].datetime, 100 * (((1 + results.loc['arb_busd']['return']).cumprod()) - 1), label = 'BRL --> BUSD --> BTC --> BRL')
# plt.plot(btcbusd.date_close.iloc[1:], 100 * ((1 + btcbusd.close.astype(float).diff() / btcbusd.close.astype(float)).cumprod() - 1).dropna(), label = 'BTC')
plt.ylabel('Cumulative Returns (%)')
plt.title('Pre-Tax Cumulative Returns')
plt.xticks(rotation = 45)
plt.legend()
plt.show()

#%%
# plt.plot(results.loc['arb_usdt'].datetime, results.loc['arb_usdt'].balance)
plt.plot(results.loc['arb_busd'].datetime, results.loc['arb_busd'].balance, label = 'BRL --> BUSD --> BTC --> BRL')
plt.title('Post-Tax Balance')
plt.ylabel('Balance')
plt.xticks(rotation = 45)
plt.legend()
plt.show()

#%%
# plt.plot(results.loc['arb_usdt'].datetime, results.loc['arb_usdt'].volume_btc + results.loc['arb_busd'].volume_btc)
plt.plot(results.loc['arb_busd'].datetime, results.loc['arb_busd'].volume_btc)
plt.ylabel('Volume (BTC)')
plt.show()
'''
#%%
round_to_n = lambda x, n: x if x == 0 else round(x, -int(floor(log10(abs(x)))) + (n - 1))

def get_balance(symbol, tickers_balance = ['BTC', 'BRL', 'BUSD']):
    balances = DataFrame(client.get_account()['balances'])
    balances = balances.loc[balances.asset.isin(tickers_balance)]
    balance = float(balances.loc[balances.asset == symbol, 'free'].iloc[0])
    return balance

def generate_order(symbol_from, symbol_to, side = SIDE_BUY):
    symbol = symbol_to + symbol_from if side == SIDE_BUY else symbol_from + symbol_to
    if symbol == 'BUSDBRL':
        rounder_quantity, rounder_price = 2, 3
        min_movement = 0.005
    elif symbol == 'BTCBUSD':
        rounder_quantity, rounder_price = 6, 2
        min_movement = 200.
    elif symbol == 'BTCBRL':
        rounder_quantity, rounder_price = 6, 4
        min_movement = 300.
    elif symbol == 'ADABRL':
        rounder_quantity, rounder_price = 2, 3
        min_movement = 0.001
    balance = get_balance(symbol_from) #* 0.98
    # balance = 10. if balance <= 0.1 else balance
    trades = client.get_ticker(symbol = symbol)
    price = float(trades['lastPrice']) - min_movement #if side == SIDE_BUY else 1 + float(trades['lastPrice'])
    quantity = balance / price if side == SIDE_BUY else balance
    quantity = round_to_n(quantity, rounder_quantity) #if quantity <= 1e-1 else round(quantity, 2)
    # price = round(price, rounder_price)
    print('{:.6f}'.format(quantity), round(price, rounder_price), quantity * price, balance)
    # print(round_to_n(quantity, 2) * round(price, 2), round(balance, 2))
    order = client.create_order(
        symbol = symbol,
        side = side,
        type = ORDER_TYPE_LIMIT_MAKER,
        quantity = '{:.6f}'.format(quantity),
        price = round(price, 2),
    )
    if side == SIDE_BUY:
        print('Order generated. BUY {} {} at price {} {}'.format(quantity, symbol_to, price, symbol_from))
    elif side == SIDE_SELL:
        print('Order generated. SELL {} {} at price {} {}'.format(quantity, symbol_from, price, symbol_to))

def generate_conditional_order(symbol_from, symbol_to, symbol_order, side = SIDE_BUY):
    reference = symbol_from + symbol_order
    order_executed = False
    print('waiting...')
    while order_executed == False:
        open_orders = client.get_open_orders(symbol = reference)
        if not open_orders:
            sleep(1.)
            order = generate_order(symbol_from, symbol_to, side)
            order_executed = True

#%%
# generate_order('BRL', 'ADA')
# generate_order('BRL', 'BTC')
# generate_order('BRL', 'BUSD')
# generate_conditional_order('BUSD', 'BTC', 'BRL')
# sleep(4 * SECONDS_IN_HOUR)
# generate_conditional_order('BTC', 'BRL', 'BUSD', side = SIDE_SELL)

#%%


#%%
symbol = 'BUSDBRL'
balance = 1e2
trades = client.get_ticker(symbol = symbol)
price = float(trades['lastPrice']) *  (1.+ 1e-5)
quantity = balance / price
order = client.create_test_order(
    symbol = symbol,
    side = SIDE_BUY,
    type = ORDER_TYPE_LIMIT_MAKER,
    # timeInForce = TIME_IN_FORCE_GTC,
    quantity = round(quantity, 2),
    price = round(price, 2),
)
# orders = client.get_open_orders(symbol = 'BUSDBRL')
# orders
# round(quantity * price, 3), round(balance, 3)

#%%
trades = DataFrame(client.get_my_trades(symbol = 'BTCBRL'))
# trades['pctg_commission'] = trades.commission.astype(float) / trades.qty.astype(float)
# trades.pctg_commission.mean()
trades

#%%
trades = client.get_ticker(symbol='BTCBUSD')
print('{:.2f} {:.2f} {:.2f}'.format(float(trades['lastPrice']) *  (1. - 1e-4), float(trades['lastPrice']), float(trades['lastPrice']) *  (1.+ 1e-4)))
