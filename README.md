# Topo_Historical

Downloads historical data from IB API

## Instructions

1. When Log In in the gateway or TWS specify the timezone "UTC".
2. Check the IB port that the API is using (eg: 7897). The code is using the 7497 port, so if you have it different you can change the number in the code or in the API Settings.
3. Set the path in the line 153 of the code if you want to save the information in an specific folder, if not it will be saved in the current path.
4. Create a Master file (empty Data Frame) with the columns Date, Price and Size. Otherwise comment the line 154.
5. If you want to download more than one session, change the parameters for the variable **self.startdt** the line 166. For days in (timedelta(days=**1**)). You can also change the hour and minutes for the beginning date in self.current_time.replace(hour=**18**, minute=**0,** second=0, microsecond=0)
6. Run the code with python interpreter.
7. Specify the Client ID (int): Whatever number between 1 to 999.
