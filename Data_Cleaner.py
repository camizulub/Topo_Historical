import glob
import pandas as pd
import os

symbols = ['GC', 'SI', 'PL', 'PA', 'MGC', 'QO', 'QI', 'MXP', 'ES', 'CL', 'NQ', 'RTY','YM', 'NG', 'ZS', 'MES',
                         'MNQ', 'M2K', 'MYM', 'QM', 'BRR']

for symbol in symbols:
# Get a list of all the file paths that ends with wildcard
    fileList = glob.glob("Topo_Data/{}/{}_2*ticks.csv".format(symbol, symbol))
    print('Deleting the following file for {}'.format(symbol))
    print(fileList)
    # Iterate over the list of filepaths & remove each file.
    for filePath in fileList:
        try:
            os.remove(filePath)
        except:
            print("Error while deleting file : ", filePath)

for symbol in symbols:
    print('Cleaning Master File')
    # Cleans the master data file
    df = pd.DataFrame(columns=['Date', 'Last', 'Volume'])
    df.set_index('Date', inplace=True)
    df.to_csv('Topo_Data/{}/{}_master.csv'.format(symbol, symbol))
