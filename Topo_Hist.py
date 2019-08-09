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
from datetime import datetime, timedelta 
from ib_insync import *
import pytz

class Topo:

    def __init__(self):
        '''Initializes the topo atribuites.'''
        self.clientid = input('\tClient ID: ')
        self.ticket = input('\tTicket: ')
        self.exch = input('\tExchange: ')
        self.contract = Contract(secType='CONTFUT', exchange=self.exch, symbol=self.ticket)
        self.start= '20190101 00:00:00'
        self.data_type = 'BID_ASK'
        self.n_days = (datetime.now() - datetime.strptime(self.start, '%Y%m%d %H:%M:%S')).days
        self.counter = 0
        self.data = []

    def connect(self):
        '''Connects to IB Gateway or TWS.'''
        ib.connect('127.0.0.1', 7497, clientId=self.clientid)
        ib.qualifyContracts(self.contract)
        
    def set_range(self):
        ''' Sets the objective counter to perform the loop.'''
        self.counter_range = round(self.n_days*24)
        print('Counter Range= ' + str(self.counter_range))

    def looping(self):
        ''' Creates the routine for downloading the historical data.'''
        for self.counter in range(self.counter_range):
            if self.counter == 0: 
                hist = ib.reqHistoricalTicks(
                        self.contract,
                        startDateTime=self.start,
                        endDateTime='',
                        numberOfTicks=1000,
                        whatToShow=self.data_type,
                        useRth=False)
                df = util.df(hist)
                self.data.append(df)
            else:
                if df.iloc[-1,0] >= (datetime.utcnow().replace(tzinfo=pytz.UTC) - timedelta(hours=10)):
                    break
                else:
                    hist = ib.reqHistoricalTicks(
                            self.contract,
                            startDateTime=df.iloc[-1,0].strftime("%Y%m%d %H:%M:%S"),
                            endDateTime='',
                            numberOfTicks=1000,
                            whatToShow=self.data_type,
                            useRth=False)
                    df = util.df(hist)
                    self.data.append(df)
                
    def save_data(self):
        ''' Saves the data in a CSV file, if the name is not changed it overwirtes the file
        in the destination folder.'''
        final = pd.concat(self.data)
        final = final.drop(columns= ['tickAttribBidAsk'] )
        final.to_json(r'/home/camilo/Dropbox/Historical Data/{}(ticks).json'.format(self.ticket))

    def digging(self):
        self.connect()
        self.set_range()
        self.looping()
        self.save_data()

if __name__ == '__main__':

    ib = IB()
    print("Starting")
    topito = Topo()
    topito.digging
    print("Finished")
        