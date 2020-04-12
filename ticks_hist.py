from ib_insync import IB, util, Contract
import pytz
import pandas as pd
from datetime import datetime
from datetime import timedelta
from tzlocal import get_localzone

class Topo:
    '''Download tick data from the IB API from a desired date until the current time'''

    def __init__(self):
        '''Initializes the topo atribuites.'''
        self.clientid = input('\tClient ID: ')
        self.symbols = ['MXP', 'ES', 'CL', 'NQ', 'RTY', 'NG', 'ZS', 'GC', 'SI', 'PL', 'PA', 'MGC', 'QO', 'QI', 'YO']
        self.exchanges = ['GLOBEX', 'GLOBEX', 'NYMEX', 'GLOBEX', 'GLOBEX', 'NYMEX', 'ECBOT', 'NYMEX', 'NYMEX',
                            'NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'GLOBEX']
        self.start = input('\tFrom Date (YYYYMMDD HH:MM): ')
        self.start_run = datetime.now()
        self.format = '%Y%m%d %H:%M'
        self.startdt = datetime.strptime(self.start, self.format)       
        self.data_type = 'TRADES'
        self.counter = 0
        self.data = []
        self.tz = pytz.timezone('US/Eastern')
        self.local_tz = get_localzone()

    def connect(self):
        '''Connects to IB Gateway or TWS.'''
        ib.connect('127.0.0.1', 7498, clientId=self.clientid)

    def printProgressBar(self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = '\r'):
        '''Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. '\r', '\r\n') (Str)'''

        percent = ('{0:.' + str(decimals) + 'f}').format(100 * ((total - iteration) / float(total)))
        filledLength = int(length * (total - iteration) // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = printEnd) # Print New Line on Complete
        if iteration == total: 
            print()

    def looping(self):
        ''' Creates the routine for downloading the historical data.'''
        finish = self.startdt + timedelta(minutes=1) #Initial flag
        while finish > self.startdt:
            if self.counter == 0:
                hist = ib.reqHistoricalTicks(
                        self.contract, '', datetime.now(self.tz),
                        1000, whatToShow=self.data_type,
                        useRth=False)
                df = util.df(hist)
                self.data.append(df)
                self.counter += 1
                self.last= df.iloc[-1,0].tz_convert(self.tz).tz_localize(tz = None) #UTC to TZ parameter
                total = (self.last - self.startdt).total_seconds() #Difference between the current time and the desired initial date
            else:
                finish = df.iloc[0,0].tz_convert(self.tz).tz_localize(tz = None) #First date of the last download in our desired TZ
                end = df.iloc[0,0].tz_convert(self.local_tz).tz_localize(tz = None) #First date of the last download in the machine TZ
                hist = ib.reqHistoricalTicks(
                        self.contract, '', end,
                        1000, whatToShow=self.data_type,
                        useRth=False)
                df = util.df(hist)
                self.data.append(df)
                sec_diff = (finish - self.startdt).total_seconds() # Number of data pending to download
                if sec_diff > 0:
                    self.printProgressBar(sec_diff, total)

    def save_data(self):
        ''' Prepate and save the data in a CSV file in the destination folder.'''
        print(' ')
        self.data.reverse()
        final = pd.concat(self.data) #Master DataFrame
        final.columns = map(lambda x: x.capitalize(), final.columns)
        final = final.rename(columns={'Time': 'Date'})
        final.drop(columns= ['Tickattriblast', 'Exchange', 'Specialconditions'], inplace=True)
        final.set_index('Date', inplace=True)
        final.index = final.index.tz_convert(self.tz).tz_localize(tz = None) #Convert from UTC to TZ parameter
        final = final[self.startdt:] #Filter from the begining date
        if final.iloc[0,0] > 1:
            final = final.round(2)
        else:
            final = final.round(5)
        final = final.groupby(['Date','Price'], sort=False).sum()[['Size']].reset_index() #Compress data for the same time and price
        final.set_index('Date', inplace=True)
        alphanumeric = [character for character in str(self.startdt) if character.isalnum()]
        init_date = ''.join(alphanumeric)
        alphanumeric = [character for character in str(self.last) if character.isalnum()]
        end_date = ''.join(alphanumeric)
        final.to_csv('{}_{}-{}_ticks.csv'.format(self.ticket, init_date , end_date))
        time_run = 'Minutes Running {}'.format(round((datetime.now() - self.start_run).total_seconds()/60, 2))
        print(time_run)

    def digging(self):
        '''Start the topo'''
        self.connect()
        for symbol, exchange in zip(self.symbols, self.exchanges):
            print('Downloading data for {}'.format(symbol))
            self.ticket = symbol
            self.exch = exchange
            self.contract = Contract(secType='CONTFUT', exchange=self.exch, symbol=self.ticket)
            ib.qualifyContracts(self.contract)
            self.looping()
            self.save_data()
            self.counter = 0
            self.data = []

if __name__ == '__main__':

    ib = IB()
    print('Starting')
    juancho = Topo()
    juancho.digging()
    print('Finish')