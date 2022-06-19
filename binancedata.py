# needed for the binance API and websockets
from binance.client import Client
import csv
import os
import time
from datetime import date, datetime


client = Client()

def get_coins():
    with open('coins.txt', 'r') as f:
        coins = f.readlines()
        coins = [coin.strip('\n') for coin in coins]
    return coins


def get_historical_data(coin, since, kline_interval):
    """
    Args example:
    coin = 'BTCUSDT'
    since = '1 Jan 2021'
    kline_interval = Client.KLINE_INTERVAL_1MINUTE
    """
    if os.path.isfile(f'{coin}_{since}.csv'):
        print('Datafile already exists, loading file...')

    else:
        print(f'Fetching historical data for {coin}, this may take a few minutes...')

        start_time = time.perf_counter()
        data = client.get_historical_klines(coin, kline_interval, since)
        data = [item[0:5] for item in data]

        # field names
        fields = ['timstamp', 'high', 'low', 'open', 'close']

        # save the data
        with open(f'{coin}_{since}.csv', 'w', newline='') as f:

            # using csv.writer method from CSV package
            write = csv.writer(f)

            write.writerow(fields)
            write.writerows(data)

        end_time = time.perf_counter()

        # calculate how long it took to produce the file
        time_elapsed = round(end_time - start_time)

        print(f'Historical data for {coin} saved as {coin}_{since}.csv. Time elapsed: {time_elapsed} seconds')
    return f'{coin}_{since}.csv'


# Obsolete code

# def get_all_filenames():
#     for coin in coins:
#         return [get_historical_data(coin, '1 Jan 2021', Client.KLINE_INTERVAL_1MINUTE) for coin in coins]

# if __name__ == '__main__':
    # threads = []
    # coins = get_coins()
    # for coin in coins:
    #     t = threading.Thread(target=get_historical_data, args=(coin, '1 Jan 2021', Client.KLINE_INTERVAL_1MINUTE))
    #     t.start()
    #     threads.append(t)
    # [thread.join() for thread in threads]