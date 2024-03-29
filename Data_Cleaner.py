import glob
import pandas as pd
import os

symbols = ['GC', 'SI', 'PL', 'PA', 'MGC', 'QO', 'QI', 'MXP', 'ES', 'CL', 'NQ', 'RTY','YM', 'NG', 'ZS', 'MES',
                         'MNQ', 'M2K', 'MYM', 'QM', 'BRR', 'ETHUSDRR', 'MBT', 'MCL', '2YY', '5YY', '10Y', '30Y']

for symbol in symbols:
# Get a list of all the file paths that ends with wildcard
    fileList = glob.glob("Topo_Data/{}/{}_2*ticks*.csv".format(symbol, symbol))
    print('Deleting the following file for {}'.format(symbol))
    print(fileList)
    # Iterate over the list of filepaths & remove each file.
    for filePath in fileList:
        try:
            os.remove(filePath)
        except:
            print("Error while deleting file : ", filePath)

for symbol in symbols:
    print('Cleaning Master File for {}'.format(symbol))
    # Cleans the master data file
    df = pd.DataFrame(columns=['Date', 'Last', 'Volume'])
    df.set_index('Date', inplace=True)
    df.to_csv('Topo_Data/{}/{}_master.csv'.format(symbol, symbol))
    df2 = pd.DataFrame(columns=['Date', 'Bid', 'Ask', 'Bid_Volume', 'Ask_Volume'])
    df2.set_index('Date', inplace=True)
    df2.to_csv('Topo_Data/{}/{}_master_BA.csv'.format(symbol, symbol))
