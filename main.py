from datetime import datetime
import os
import matplotlib
import numpy as np
# import threading

import pandas as pd
import backtrader as bt
import backtrader.feeds as btfeeds

account_balance = 100000
# size of each trade in native currency ie if backtesting on ETH, each trade will be 1ETH in size
trade_size = 1
take_profit = 0.1
stop_loss = 0.05
# the threshold difference between the current candle price compared to the previous one to buy
buy_trigger = 0.01


class TestStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        """
        Logging function fot this strategy
        """
        dt = dt or self.datas[0].datetime.date(0)
        print(f'Date: {dt} {txt}')

    def __init__(self):
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.dataclose = self.datas[0].close
        # keep track of pending orders
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return
        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, %.2f' % order.executed.price)
            elif order.issell():
                self.log('SELL EXECUTED, %.2f' % order.executed.price)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def next(self):
        self.log(f'Open: {self.dataopen[0]} '
                 f'High: {self.datahigh[0]} '
                 f'Low: {self.datalow[0]} '
                 f'Close: {self.dataclose[0]}')
        # check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return
        # Check if we are in the market
        if not self.position:
            # open a position if the current candle price is higher by at least 1%
            # compared to the previous candle
            if self.dataclose[0] > self.dataclose[-1] + (self.dataclose[-1] * buy_trigger):
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.buy(size=trade_size, exectype=bt.Order.Market)
                self.order = self.buy()
                print(f'EXECUTED PRICE IS:{self.order.executed.price}')

        if self.position:
            if self.dataclose[0] > self.position.price + (self.position.price * take_profit) or \
                    self.dataclose[0] < self.position.price - (self.position.price * stop_loss):
                # self.log('SELL CREATE, %.2f' % self.dataclose[0])
                # self.sell(size=trade_size, exectype=bt.Order.Market)
                # self.order = self.sell()
                self.close()
                self.order = self.close()


def start_backtesting(filename):
    # Create a cerebro entity
    cerebro = bt.Cerebro()
    # Add a strategy
    cerebro.addstrategy(TestStrategy)
    # load data from our CSV file

    data = btfeeds.GenericCSVData(
        dataname='./data/tmp.csv',
        dtformat=('%Y-%m-%d'),
        tmformat=('%H:%M:%S'),
        datetime=0,
        time=1,
        open=2,
        high=3,
        low=4,
        close=5,
        volume=6,
        openinterest=-1
    )

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    # Set our desired cash start
    cerebro.broker.setcash(account_balance)
    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # Run over everything
    cerebro.run()
    cerebro.plot()
    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())


def transform_exchange_data(filename):
    """
    Data example
    ----
    https://www.CryptoDataDownload.com
    unix,date,symbol,open,high,low,close,Volume ADA,Volume USDT,tradecount
    1647303780000,2022-03-15 00:23:00,ADA/USDT,0.79890,0.80020,0.79880,0.80010,167055,133611.06530,425
    1647303720000,2022-03-15 00:22:00,ADA/USDT,0.79910,0.79940,0.79880,0.79890,103641,82814.96900,310
    1647303660000,2022-03-15 00:21:00,ADA/USDT,0.79770,0.79920,0.79760,0.79910,150986,120532.28680,335
    """
    # ADA

    # # remove first row with dataset link
    # with open(filename, 'r') as f:
    #     with open("temp.csv", 'w') as f1:
    #         next(f)  # skip header line
    #         for line in f:
    #             f1.write(line)

    # remove unix, symbol, Volume USDT and tradecount columns
    data_df = pd.read_csv(filename)
    data_df.drop(['unix', 'symbol', 'Volume USDT', 'tradecount'], inplace=True, axis=1)
    # separate date and time
    data_df.insert(0, 'Date', pd.to_datetime(data_df['date']).dt.date)
    data_df.insert(1, 'Time', pd.to_datetime(data_df['date']).dt.time)
    data_df.drop(['date'], inplace=True, axis=1)
    # reverse to ascending order - old data first
    data_df = data_df.iloc[::-1]
    # store
    data_df.to_csv('./tmp2.csv', encoding='utf-8', index=False)


def transform_sp_data(filename):
    """
    Data example
    ----
    Date,Timestamp,Open,High,Low,Close,Volume
    20210601,00:00:00,4203.542,4203.551,4203.033,4203.548,0.0530599979683757
    20210601,00:01:00,4204.054,4204.551,4202.799,4203.099,0.110619995743036
    20210601,00:02:00,4203.048,4203.254,4202.733,4202.733,0.0714799973648041
    """
    data_df = pd.read_csv(filename)
    # change date format 20220102 ('%Y%m%d') -> 2022-01-02 ('%Y-%m-%d')
    # data_df['Date'] = pd.datetime.strptime(x, '%Y%m%d').date().isoformat()
    for idx, row in enumerate(data_df.Date):
        data_df['Date'][idx] = datetime.strptime(str(row), '%Y%m%d').date().isoformat()
        print(f'idx {idx} row {row}')
    # save csv file
    data_df.to_csv('./test.csv', encoding='utf-8', index=False)


if __name__ == '__main__':
    # transform_sp_data('./data/USA500IDXUSD_2022.csv')

    # # merge S&P 2021_1 and 2021_2
    # df_1 = pd.read_csv('./data/USA500IDXUSD_2021_1.csv')
    # df_2 = pd.read_csv('./data/USA500IDXUSD_2021_2.csv')
    # df_merge = pd.concat([df_1, df_2])
    # df_merge.to_csv('./test.csv', encoding='utf-8', index=False)

    # transform_sp_data('./data/USA500IDXUSD_2021.csv')
    # transform_exchange_data('./data/ADAUSDT_Binance_futures_data_minute.csv')

    # # segment documents based on start and end dates
    # start_date = '2021-01-04'  # '00:00:00'
    # end_date = '2021-12-30'  # '00:00:00'
    # df_1 = pd.read_csv('./data/USA500IDXUSD_2021_trans.csv')
    # df_2 = pd.read_csv('./data/ADAUSDT_Binance_futures_data_minute_trans.csv')
    # filtered_df_1 = df_1.loc[(df_1['Date'] >= start_date) & (df_1['Date'] < end_date)]
    # filtered_df_2 = df_2.loc[(df_2['Date'] >= start_date) & (df_2['Date'] < end_date)]
    # filtered_df_1.to_csv('./data/USA500IDXUSD_2021_trans_filtered.csv', encoding='utf-8', index=False)
    # filtered_df_2.to_csv('./data/ADAUSDT_Binance_futures_data_minute_trans_filtered.csv', encoding='utf-8', index=False)

    # # we have seen that in ADA there is no data on some minutes, therefore remove those timesteps in ADA too
    # df_1 = pd.read_csv('./data/USA500IDXUSD_2021_trans_filtered.csv')
    # df_2 = pd.read_csv('./data/ADAUSDT_Binance_futures_data_minute_trans_filtered.csv')
    # idx = 0
    # while idx <= 331696:
    #     if df_1.Timestamp[idx] != df_2.Time[idx]:
    #         # print(idx)
    #         df_2 = df_2.drop(idx).reset_index(drop=True)
    #     else:
    #         idx += 1
    # df_2.to_csv('./data/USA500IDXUSD_2021_trans_filtered_trimmed.csv', encoding='utf-8', index=False)

    # =========== ASSUMPTIONS ========================================

    # for each minute calculate price change as a percent: Close / Open - 1
    df_sp = pd.read_csv('./data/USA500IDXUSD_2021_trans_filtered.csv')
    df_ada = pd.read_csv('./data/ADAUSDT_Binance_futures_data_minute_trans_filtered.csv')
    df_sp['Change'] = df_sp.Close / df_sp.Open - 1
    df_ada['Change'] = df_ada.close / df_ada.open - 1
    # print(df_sp['Close'][:10])
    # print(df_sp['Open'][:10])
    # print(df_sp['Change'][:10])

    # # for S&P500 calculate last 5 minutes “change” values standard deviation (i.e. volatility)
    timestep = 5
    df_sp = df_sp.fillna(value={'Volatility': 0.0})
    for idx in range(timestep-1, len(df_sp)):
        # df_sp['Volatility'] = np.std(list(df_sp['Change'][idx-5:idx]), ddof=1)
        print(idx)
        print(list(df_sp.Open[idx-timestep+1:idx+1]))
        exit(0)
    print(df2.head())

    #
    # # calculate correlation between 5 "changes" of S&P and ADA
    # from scipy import stats
    # scipy.stats.pearsonr([1, 2, 3], [1, 2, 3])

    # TODO: save

    # =========== DONE ========================================

    # start_backtesting(filename='./FTX_Futures_BTCPERP_minute.csv')

    # transform_exchange_data('FTX_Futures_BTCPERP_minute.csv')
    # transform_exchange_data('BTCUSDT_1 Jan 2021.csv')

    # TODO: understand strategy
