#
# Python Script
# to Download data
# from Interactive Brokers API
#
# QSociety
# (c) Camilo Zuluaga
# camilo.zuluaga.trader@gmail.com
#
from arctic import Arctic
from arctic.auth import Credential
from arctic.hooks import register_get_auth_hook
from ib_insync import IB, util, Contract, Future
import json
import pytz
import pandas as pd
from datetime import datetime, timedelta
import requests

class Topo:
    '''Download tick data from the IB API for the session of each day of the week'''
    def __init__(self):
        '''Initializes the topo atribuites.'''
        self.clientid = input('\tClient ID: ')
        self.symbols = ['GC', 'SI', 'PL', 'PA', 'MGC', 'QO', 'QI', 'MXP', 'ES', 'CL', 'NQ', 'RTY', 'YM', 'NG', 'ZS', 'MES', 
                         'MNQ','M2K', 'MYM', 'QM', 'BRR']
        self.exchanges = ['NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'NYMEX', 'GLOBEX', 'GLOBEX', 'NYMEX',
	                  'GLOBEX', 'GLOBEX','ECBOT', 'NYMEX', 'ECBOT', 'GLOBEX', 'GLOBEX', 'GLOBEX', 'ECBOT', 'NYMEX', 'CMECRYPTO']   
        self.data_type = 'TRADES'
        self.counter = 0
        self.data = []
        self.re_run = False    # False Default
        self.run = 0
        self.tz = pytz.timezone('US/Eastern')
        self.counter_miss = 0
        self.days = {0: 'MONDAY', 1: 'TUESDAY', 2: 'WEDNESDAY', 3: 'THURSDAY', 4: 'FRIDAY'}

    def credentials(self):
        '''Reads the credentials for authentications'''
        credentials_json = json.load(open('credentials.json'))
        self.mongo_user = credentials_json['mongo']['user']
        self.mongo_user_pass = credentials_json['mongo']['password']
        self.telegram_token = credentials_json['telegram']['bot_token']
        self.telegram_chat = credentials_json['telegram']['bot_chatID']

    def arctic_auth_hook(self, mongo_host, app, database):
        '''Auntenticates to mongodb database'''
        return Credential(
        database="arctic",
        user=self.mongo_user,
        password=self.mongo_user_pass,
        )   
    def db_connect(self):
        '''Connects to Arctic database.'''
        register_get_auth_hook(self.arctic_auth_hook)
        store = Arctic('db')
        self.library = store['Futures_Historical_Ticks']

    def connect(self):
        '''Connects to IB Gateway or TWS.'''
        ib.connect('127.0.0.1', 7497, clientId=self.clientid)

    def send_telegram_message(self, message):
        '''Send telegram message to specific group'''
        bot_token = self.telegram_token
        bot_chatID = self.telegram_chat
        url = 'https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s'%(bot_token, bot_chatID, message)
        requests.get(url)

    def local_symbol(self, symbol):
        if symbol in ['ES', 'RTY', 'NQ', 'MES', 'MNQ', 'M2K']:
            contract_dates = pd.read_csv('contract_dates/indexes_globex.txt', parse_dates=True)
        elif symbol in ['YM', 'MYM', 'DAX']:
            contract_dates = pd.read_csv('contract_dates/indexes_ecbot_dtb.txt', parse_dates=True)
        elif symbol in ['QO', 'MGC']:
            contract_dates = pd.read_csv('contract_dates/QO_MGC.txt', parse_dates=True)
        elif symbol in ['CL', 'QM']: contract_dates = pd.read_csv('contract_dates/CL_QM.txt')
        else: contract_dates = pd.read_csv('contract_dates/%s.txt'%symbol, parse_dates=True)
        
        for i in range(len(contract_dates)):
            initial_date = pd.to_datetime(contract_dates.iloc[i].initial_date).date()
            final_date = pd.to_datetime(contract_dates.iloc[i].final_date).date()
            if initial_date <= self.current_time.date() <= final_date:
                current_contract = contract_dates.iloc[i].contract
                break
        
        self.local = current_contract
        if symbol in ['ES', 'RTY', 'NQ', 'MES', 'MNQ', 'M2K', 'QO', 'CL', 'MGC', 'QM']:
            self.local = '%s%s'%(symbol, current_contract)
        if symbol in ['YM', 'ZS']: self.local = '%s   %s'%(symbol, current_contract)
        if symbol == 'MYM': self.local = '%s  %s'%(symbol, current_contract)
        if symbol == 'DAX': self.local = 'FDAX %s'%current_contract

    def looping(self):
        ''' Creates the routine for downloading the historical data.'''
        curr = self.current_time.astimezone(pytz.utc).replace(tzinfo=None) + timedelta(minutes=1) #Last date of the last closing in the machine TZ
        close = ib.reqHistoricalTicks(self.contract, '', curr, 1000, whatToShow=self.data_type, useRth=False)
        df_close = util.df(close)
        last= df_close.iloc[-1,0].replace(tzinfo=None) #UTC to TZ parameter
        start = self.startdt.astimezone(pytz.utc).replace(tzinfo=None)
        end = last - timedelta(minutes=1)
        while not(end >= last):
            if self.counter == 0:
                hist = ib.reqHistoricalTicks(self.contract, start, '', 1000, whatToShow=self.data_type, useRth=False)
                df = util.df(hist)
                if len(hist) > 0:
                    self.data.append(df)
                    self.counter = 1
                    total = (last - start).total_seconds() #Difference between the current time and the desired initial date
                    end = df.iloc[-1,0].replace(tzinfo=None)
                    if (end >= last):
                        percent = 100
                        print(' Progress [%d%%]\r'%percent, end="")
                else:
                    print('IB is not retreiving current data for {}'.format(self.ticket))
                    break
            else:
                hist = ib.reqHistoricalTicks(self.contract, end, '', 1000, whatToShow=self.data_type, useRth=False)
                if len(hist) == 0:
                    while len(hist) == 0:
                        end = end + timedelta(minutes=1) #First date of the last download in the machine TZ
                        hist = ib.reqHistoricalTicks(self.contract, end, '', 1000, whatToShow=self.data_type, useRth=False)
                        self.counter_miss += 1
                        if len(hist) > 0:
                            df = util.df(hist)
                            self.data.append(df)
                            sec_diff = (last - end).total_seconds() # Number of data pending to download
                            percent = (100 * ((total - sec_diff) / float(total)))  
                            print(' Progress [%d%%]\r'%percent, end="")
                        if (end >= last):
                            percent = 100
                            print(' Progress [%d%%]\r'%percent, end="")
                            break
                else:
                    df = util.df(hist)
                    if end == df.iloc[-1,0].replace(tzinfo=None):
                        new_end = end + timedelta(seconds=1)
                        hist = ib.reqHistoricalTicks(self.contract, new_end, '', 1000, whatToShow=self.data_type, useRth=False)
                        df = util.df(hist)
                        end = df.iloc[-1,0].replace(tzinfo=None)
                    self.data.append(df)
                    end = df.iloc[-1,0].replace(tzinfo=None)
                    sec_diff = (last- end).total_seconds() # Number of data pending to download
                    percent = (100 * ((total - sec_diff) / float(total)))
                    if (sec_diff < 0 or end >= last):
                        percent = 100
                        print(' Progress [%d%%]\r'%percent, end="")
                        break
                    print(' Progress [%d%%]\r'%percent, end="")

    def save_data(self):
        ''' Preparate and save the data in a CSV file in the destination folder.'''
        print(' ')
        final = pd.concat(self.data) #Master DataFrame
        final.columns = map(lambda x: x.capitalize(), final.columns)
        final = final.rename(columns={'Time': 'Date', 'Price': 'Last', 'Size': 'Volume'})
        final.drop(columns= ['Tickattriblast', 'Exchange', 'Specialconditions'], inplace=True)
        final['consec'] = (final.Date != final.Date.shift()).cumsum() + (final.Last != final.Last.shift()).cumsum() #Calculates consecutive values
        final = final.groupby(['consec', 'Date', 'Last']).sum().reset_index().drop('consec', axis=1) #Compressor
        final.set_index('Date', inplace=True)
        final.index = final.index.tz_convert(self.tz).tz_localize(tz=None) #Convert from UTC to TZ parameter
        if self.ticket in ['QI', 'ZS']:
            final = final.round(4)
        elif self.ticket in ['MXP']:
            final = final.round(5)
        elif self.ticket in ['NG', 'QM']:
            final = final.round(3)
        else:
            final = final.round(2)
        final = final[:self.current_time.replace(tzinfo=None)]
        alphanumeric = [character for character in str(self.startdt.replace(tzinfo=None)) if character.isalnum()]
        init_date = ''.join(alphanumeric)
        alphanumeric = [character for character in str(self.current_time.replace(tzinfo=None)) if character.isalnum()]
        end_date = ''.join(alphanumeric)
        final.to_csv('Topo_Data/{}/{}_{}-{}_ticks.csv'.format(self.ticket, self.ticket, init_date , end_date)) #Session
        final.to_csv('Topo_Data/{}/{}_master.csv'.format(self.ticket, self.ticket), mode='a', header=False) #Master
        self.library.append(self.ticket, final, metadata={'source': 'Qs'})
        
    def digging(self):
        '''Starts the topo'''
        self.current_time = datetime.now(self.tz)
        while not((self.current_time.weekday() == 4) & (self.current_time.hour > 17)): #Be active during the week, finish at market close
            if self.run >= 1: #If the code downloaded the data for the selected day do not do it again
                print('Pending Session Downloaded')
                break
            self.current_time = datetime.now(self.tz).replace(second=0, microsecond=0) #Modify if you need to download an specific date
            if (self.current_time.weekday() <= 4) & (self.current_time.hour == 17) & (self.current_time.minute <= 2):
                self.startdt = self.current_time.replace(hour=18, minute=0) - timedelta(days=1) #From when download data
                self.start_run = datetime.now(self.tz) #For calculating the time for downloading the session
                self.connect()
                if self.re_run: #Flag when a specific session is requiered to download
                    self.run += 1
                self.send_telegram_message('DOWNLOADING SESSION OF {}'.format(self.days[self.current_time.weekday()]))  # Download start Message to telegram
                print('DOWNLOADING SESSION OF {}'.format(self.days[self.current_time.weekday()]))
                for symbol, exchange in zip(self.symbols, self.exchanges):
                    print('Downloading data for {}'.format(symbol))
                    self.ticket = symbol
                    self.exch = exchange
                    self.local_symbol(symbol)
                    print('Local Symbol {}'.format(self.local))
                    self.contract = Contract(secType='FUT', exchange=self.exch, localSymbol=self.local)
                    ib.qualifyContracts(self.contract)
                    self.looping()
                    if self.counter > 0: self.save_data()
                    print('Failed request # {} for {}'.format(self.counter_miss, self.ticket))
                    self.send_telegram_message('Download of {} complete'.format(symbol))    # Symbol download end to Telegram
                    self.counter = 0
                    self.counter_miss = 0
                    self.data = []
                time_run = 'Minutes Running per Session {}'.format(round((datetime.now(self.tz) - self.start_run).total_seconds()/60, 2))
                self.send_telegram_message(time_run)    # Download end session message to telegram
                print(time_run)
                ib.disconnect()
                ib.sleep(1)

if __name__ == '__main__':
    ib = IB()
    print('Starting')
    juancho = Topo()
    juancho.credentials()
    juancho.db_connect()
    juancho.digging()
    print('Finish')      
