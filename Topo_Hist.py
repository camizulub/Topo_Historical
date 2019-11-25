#
# Python Script
# to Download data
# from Interactive Brokers API
#
# QSociety
# (c) Camilo Zuluaga
# camilo.zuluaga.trader@gmail.com
#
import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
from ib_insync import IB, util, Contract
from tqdm import tqdm

class Topo:

    def __init__(self):
        '''Initializes the topo atribuites.'''
        self.clientid = input('\tClient ID: ')
        self.ticket = input('\tTicket: ')
        self.exch = input('\tExchange: ')
        self.contract = Contract(secType='CONTFUT', exchange=self.exch, symbol=self.ticket)
        self.format = "%Y%m%d %H:%M:%S"
        self.start = '20191001 00:00:00'
        self.startdt = datetime.strptime(self.start, self.format)
        self.data_type = 'TRADES'
        self.n_seconds = ((datetime.now() - self.startdt).days*60*60*24) + (datetime.now() - self.startdt).seconds
        self.counter = 0
        self.data = []
        self.tz = pytz.timezone('US/Eastern')

    def connect(self):
        '''Connects to IB Gateway or TWS.'''
        ib.connect('127.0.0.1', 7498, clientId=self.clientid)
        ib.qualifyContracts(self.contract)
        
    def set_range(self):
        ''' Sets the objective counter to perform the loop.'''
        self.counter_range = round(self.n_seconds/2000)
        print('Counter Range= ' + str(self.counter_range))

    def looping(self):
        ''' Creates the routine for downloading the historical data.'''
        for self.counter in tqdm(range(self.counter_range)):
            if self.counter == 0: 
                hist = ib.reqHistoricalData(
                        self.contract, endDateTime='',
                        durationStr='2000 S',
                        barSizeSetting='1 secs',
                        whatToShow=self.data_type,
                        useRTH=False,
                        formatDate=1)
                df = util.df(hist)
                self.data.append(df)
            else:
                if self.counter%5==0:
                    ib.sleep(2)
                if self.counter%60==0:
                    ib.sleep(575)
                try:
                    hist = ib.reqHistoricalData(
                            self.contract,
                            endDateTime=df.iloc[0,0].strftime(self.format),
                            durationStr='2000 S',
                            barSizeSetting='1 secs',
                            whatToShow=self.data_type,
                            useRTH=False,
                            formatDate=1)
                    df = util.df(hist)
                    self.data.append(df)
                except:
                    print('Exception Ocurred')
                    break
                
    def save_data(self):
        ''' Saves the data in a CSV file, if the name is not changed it overwirtes the file
        in the destination folder.'''
        self.data.reverse()
        final = pd.concat(self.data)
        final .columns = map(lambda x: x.capitalize(), final.columns)
        time_format = '%Y-%m-%d %H:%M:%S.%f'
        final['Date'] = pd.to_datetime(final['Date'], format= time_format)
        final.set_index('Date', inplace=True)
        final = final.drop(columns= ['Barcount', 'Average'] )
        final.to_csv('C:/Users/MiloZB/Dropbox/Codigos/Data/{}(secIB).csv'.format(self.ticket))

    def digging(self):
        self.connect()
        self.set_range()
        self.looping()
        self.save_data()

if __name__ == '__main__':

    ib = IB()
    print("Starting")
    topito = Topo()
    topito.digging()
    print("Finished")
        