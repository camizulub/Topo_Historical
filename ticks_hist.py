from ib_insync import IB, util, Contract
import pytz
import pandas as pd
from datetime import datetime, timedelta
from tzlocal import get_localzone

class Topo:
    '''Download tick data from the IB API from a desired date until the current time'''
    def __init__(self):
        '''Initializes the topo atribuites.'''
        self.clientid = input('\tClient ID: ')
        self.symbols = [ 'GC', 'SI', 'PL', 'PA', 'MGC', 'QO', 'QI', 'MXP', 'ES', 'CL', 'NQ', 'RTY', 'NG', 'ZS',]
        self.exchanges = ['NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'GLOBEX', 'GLOBEX', 'NYMEX',
						    'GLOBEX', 'GLOBEX', 'NYMEX', 'ECBOT']     
        self.data_type = 'TRADES'
        self.counter = 0
        self.data = []
        self.tz = pytz.timezone('US/Eastern')
        self.local_tz = get_localzone()
        self.counter_miss = 0

    def connect(self):
        '''Connects to IB Gateway or TWS.'''
        ib.connect('127.0.0.1', 7498, clientId=self.clientid)

    def looping(self):
        ''' Creates the routine for downloading the historical data.'''
        finish = self.current_time - timedelta(minutes=1) #Initial flag
        close = ib.reqHistoricalTicks(self.contract, '', self.current_time, 1000, whatToShow=self.data_type, useRth=False)
        df_close = util.df(close)
        self.last= df_close.iloc[-1,0].tz_convert(self.tz).tz_localize(tz = None) #UTC to TZ parameter
        while finish < self.current_time and not(finish == self.last):
            if self.counter == 0:
                hist = ib.reqHistoricalTicks(self.contract,self.startdt, '', 1000, whatToShow=self.data_type, useRth=False)
                df = util.df(hist)
                if len(hist) > 0:
                    self.data.append(df)
                    self.counter = 1
                    total = (self.current_time - self.startdt).total_seconds() #Difference between the current time and the desired initial date
                else:
                    print('IB is not retreiving current data for {}'.format(self.ticket))
                    break
            else:
                finish = df.iloc[-1,0].tz_convert(self.tz).tz_localize(tz = None) #First date of the last download in our desired TZ
                end = df.iloc[-1,0].tz_convert(self.local_tz).tz_localize(tz = None) #First date of the last download in the machine TZ
                hist = ib.reqHistoricalTicks(self.contract, end, '', 1000, whatToShow=self.data_type, useRth=False)
                if len(hist) == 0:
                    while len(hist) == 0:
                        finish = finish + timedelta(minutes=1) #First date of the last download in our desired TZ
                        end = end + timedelta(minutes=1) #First date of the last download in the machine TZ
                        hist = ib.reqHistoricalTicks(self.contract, end, '', 1000, whatToShow=self.data_type, useRth=False)
                        self.counter_miss += 1
                        if len(hist) > 0:
                            df = util.df(hist)
                            self.data.append(df)
                            sec_diff = (self.current_time - finish).total_seconds() # Number of data pending to download
                            percent = (100 * ((total - sec_diff) / float(total)))  
                            print(' Progress [%d%%]\r'%percent, end="")
                        if (finish > self.current_time or finish == self.last):
                            percent = 100 
                            print(' Progress [%d%%]\r'%percent, end="")
                            break
                else:
                    df = util.df(hist)
                    self.data.append(df)
                    sec_diff = (self.current_time - finish).total_seconds() # Number of data pending to download
                    percent = (100 * ((total - sec_diff) / float(total)))
                    if (sec_diff < 0 or finish == self.last):
                        percent = 100
                    print(' Progress [%d%%]\r'%percent, end="")

    def save_data(self):
        ''' Preparate and save the data in a CSV file in the destination folder.'''
        print(' ')
        final = pd.concat(self.data) #Master DataFrame
        final.columns = map(lambda x: x.capitalize(), final.columns)
        final = final.rename(columns={'Time': 'Date'})
        final.drop(columns= ['Tickattriblast', 'Exchange', 'Specialconditions'], inplace=True)
        final.set_index('Date', inplace=True)
        final.index = final.index.tz_convert(self.tz).tz_localize(tz = None) #Convert from UTC to TZ parameter
        final = final[:self.current_time] #Filter from the begining date
        if final.iloc[0,0] > 1:
            final = final.round(2)
        else:
            final = final.round(5)
        alphanumeric = [character for character in str(self.startdt) if character.isalnum()]
        init_date = ''.join(alphanumeric)
        alphanumeric = [character for character in str(self.current_time) if character.isalnum()]
        end_date = ''.join(alphanumeric)
        final.to_csv('{}_{}-{}_ticks.csv'.format(self.ticket, init_date , end_date)) #Session
        #final.to_csv('{}/{}_master.csv'.format(self.ticket, self.ticket), mode='a', header=False) #Master
        
    def digging(self):
        '''Starts the topo'''
        self.current_time = datetime.now(self.tz).replace(day=1, hour=17, minute=0, second=0, microsecond=0, tzinfo=None) #Test Here
        while not((self.current_time.weekday() == 4) & (self.current_time.hour > 17)): #Be active during the week, finish at market close
            self.current_time = datetime.now(self.tz).replace(day=1, hour=17, minute=0, second=0, microsecond=0, tzinfo=None) #Test Here
            if ((self.current_time.weekday() == 0)|(self.current_time.weekday() == 1)|(self.current_time.weekday() == 2)\
                | (self.current_time.weekday() == 3) | (self.current_time.weekday() == 4)) & (self.current_time.hour == 17) & (self.current_time.minute <= 1):
                self.startdt = self.current_time.replace(hour=18, minute=0, second=0, microsecond=0) - timedelta(days=1) #From when download data
                self.start_run = datetime.now(self.tz) #For calculating the time for downloading the session
                self.connect()
                for symbol, exchange in zip(self.symbols, self.exchanges):
                    print('Downloading data for {}'.format(symbol))
                    self.ticket = symbol
                    self.exch = exchange
                    self.contract = Contract(secType='CONTFUT', exchange=self.exch, symbol=self.ticket)
                    ib.qualifyContracts(self.contract)
                    self.looping()
                    if self.counter > 0: self.save_data()
                    print('Failed request # {} for {}'.format(self.counter_miss, self.ticket))
                    self.counter = 0
                    self.counter_miss = 0
                    self.data = []
                time_run = 'Minutes Running per Session{}'.format(round((datetime.now(self.tz) - self.start_run).total_seconds()/60, 2))
                print(time_run)
                ib.disconnect()
                ib.sleep(1)

if __name__ == '__main__':
    ib = IB()
    print('Starting')
    juancho = Topo()
    juancho.digging()
    print('Finish')