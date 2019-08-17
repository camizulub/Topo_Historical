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
from ib_insync import *

class Topo:

    def __init__(self):
        '''Initializes the topo atribuites.'''
        self.clientid = input('\tClient ID: ')
        self.ticket = input('\tTicket: ')
        self.exch = input('\tExchange: ')
        self.contract = Contract(secType='CONTFUT', exchange=self.exch, symbol=self.ticket)
        self.format = "%Y%m%d %H:%M:%S"
        self.start = '20190811 00:00:00'
        self.startdt = datetime.strptime(self.start, self.format)
        self.data_type = 'TRADES'
        self.n_seconds = (datetime.now() - self.startdt).seconds
        self.counter = 0
        self.data = []
        self.tz = pytz.timezone('US/Eastern')

    def connect(self):
        '''Connects to IB Gateway or TWS.'''
        ib.connect('127.0.0.1', 7497, clientId=self.clientid)
        ib.qualifyContracts(self.contract)
        
    def set_range(self):
        ''' Sets the objective counter to perform the loop.'''
        self.counter_range = round(self.n_seconds/24)
        print('Counter Range= ' + str(self.counter_range))

    def looping(self):
        ''' Creates the routine for downloading the historical data.'''
        for self.counter in range(self.counter_range):
            if self.counter == 0: 
                hist = ib.reqHistoricalTicks(
                        self.contract,
                        startDateTime='',
                        endDateTime=datetime.now(),
                        numberOfTicks=1000,
                        whatToShow=self.data_type,
                        useRth=False)
                df = util.df(hist)
                self.data.append(df)
            else:
                if df.iloc[0,0].replace(tzinfo=timezone.utc).astimezone(tz=self.tz) <= self.startdt.replace(tzinfo=timezone.utc).astimezone(tz=self.tz):
                        break
                else:
                    hist = ib.reqHistoricalTicks(
                            self.contract,
                            startDateTime='',
                            endDateTime=df.iloc[0,0].replace(tzinfo=timezone.utc).astimezone(tz=self.tz).strftime(self.format),
                            numberOfTicks=1000,
                            whatToShow=self.data_type,
                            useRth=False)
                    df = util.df(hist)
                    print(df.iloc[0,0].replace(tzinfo=timezone.utc).astimezone(tz=self.tz))
                    self.data.append(df)
                
    def save_data(self):
        ''' Saves the data in a CSV file, if the name is not changed it overwirtes the file
        in the destination folder.'''
        self.data.reverse()
        final = pd.concat(self.data)
        final.drop(columns= ['tickAttribLast', 'exchange', 'specialConditions'], inplace=True)
        final.set_index('time', inplace=True)
        final.to_csv('/home/camilo/Dropbox/Codigos/Data/{}(ticks){}.csv'.format(self.ticket, self.start))

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
        