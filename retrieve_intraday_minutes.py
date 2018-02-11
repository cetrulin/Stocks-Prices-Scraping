# Scraping stocks from Google and Alpha Vantage
# @author: Andres L. Suarez-Cetrulo
import pandas as pd
import numpy as np
import datetime

"""
Global values
"""

PATH = "/home/YOUR_USER/DATA_REPO"
RAW_DATA_PATH = PATH+"/WAREHOUSE/"
MAX_TRIES = 10 # downloading a symbol

"""
Retrieve intraday stock data from Alpha Vantage API.
"""

#Alpha Vantage API to download 15 days of minute data (only if required)
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.cryptocurrencies import CryptoCurrencies

apikey='YOUR_ALPHA_VANTAGE_API_KEY'

# Get pandas object with the intraday data and another with the call's metadata
# Get the same for crypto
ts = TimeSeries(key=apikey, output_format='pandas')
cc = CryptoCurrencies(key=apikey, output_format='pandas')


"""
Retrieve intraday stock data from Google Finance.
"""

import csv
import datetime
import re

import pandas as pd
import requests

def get_google_finance_intraday(ticker, period=60, days=1, exchange='USD', debug=False):
    """
    Retrieve intraday stock data from Google Finance.
    Parameters
    ----------
    ticker : str
        Company ticker symbol.
    period : int
        Interval between stock values in seconds.
    days : int
        Number of days of data to retrieve.
    Returns
    -------
    df : pandas.DataFrame
        DataFrame containing the opening price, high price, low price,
        closing price, and volume. The index contains the times associated with
        the retrieved price values.
    """
    
    uri = 'https://finance.google.com/finance/getprices' \
          '?&p={days}d&f=d,o,h,l,c,v&q={ticker}&i={period}?x={exchange}'.format(ticker=ticker, period=period,  days=days, exchange=exchange)
    
    if(debug): 
        print (uri)
        
    page = requests.get(uri)
    reader = csv.reader(page.content.splitlines())
    columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    rows = []
    times = []
    
    for row in reader:
        
        if re.match('^[a\d]', row[0]):
            
            if row[0].startswith('a'):
                start = datetime.datetime.fromtimestamp(int(row[0][1:]))
                times.append(start)
                
            else:
                times.append(start+datetime.timedelta(seconds=period*int(row[0])))
                
            rows.append(map(float, row[1:]))
            
    if len(rows):
        return pd.DataFrame(rows, index=pd.DatetimeIndex(times, name='Date'),
                            columns=columns)
    else:
        return pd.DataFrame(rows, index=pd.DatetimeIndex(times, name='Date'))


"""
Download price for a given symbol using either Google Finance or Alpha Vantage
"""

def download_single_price_from(symbol,period=60,days=20,exchange='USD',site="google",debug=True, \
                               path="default" ,name="default"): #real max days at 1min level is 15...
    df = pd.DataFrame({'A' : []})
    site_option = ""
    
    # Download index price
    if site=="google_finance":
        df = get_google_finance_intraday(symbol,period,days,exchange,debug) 
    elif site=="alpha_vantage":        
        df, meta_data = ts.get_intraday(symbol, interval='1min', outputsize='full')
    elif site=="avantage_crypto":
        df, meta_data = cc.get_digital_currency_intraday(symbol, exchange)   
        
    # Save index prices
    output_file=check_or_create_path(path)+"/"+name+"_"+str(datetime.date.today())+".csv.gz"
    # df.to_csv(output_file, sep=';', encoding='utf-8', compression='gzip')
    df.to_csv(output_file, sep=';', compression='gzip') # encoding='utf-8', 
    # each compressed file can be read after as:
    #  df = pd.read_csv(output_file, compression='gzip')


"""
Trying to download the symbols '$MAX_TRIES' times. 
After, we assume that the symbol is not available in the given API/Provider.
"""

def try_download(symbol,period,days,exchange,site,debug,path, name, tries_count):
    try: download_single_price_from(symbol=symbol,period=period,days=days,exchange=exchange,\
                                    site=site,debug=debug,path=path,name=symbol)
    except: # catch *all* exceptions
        e = sys.exc_info()[0]
        print( "<p>Error: %s</p>" % e )
        
        # Recursive function that tries again from pointer when crashing (awaits 5 seconds to retry)
        if (tries_count<MAX_TRIES):
            time.sleep(5)
            print ("Trying again... try #"+str(tries_count+1)+" of a maximum of "+str(MAX_TRIES))
            try_download(symbol,period,days,exchange,site,debug,path, name, tries_count=(tries_count+1))

        else: 
            print ("Maximum number of attemps for symbol: "+symbol+" from "+site+ \
                   ". Check in path '"+path+"' required.")
                   
"""
Download price for all the symbols of a given list
"""

import os
import sys
import time

def download_list_of_prices(root_path, list_file, symbols_subpath, \
                            period=60,days=20,exchange='USD',site="google",\
                            debug=True, from_symbol=''):
    # 1 Initialize pointer
    download_symbol=False if from_symbol != '' else True
    
    # 2 Load list of stocks
    symbols=pd.read_csv(root_path+"/"+list_file, sep=';', parse_dates=True, infer_datetime_format=True)
    
    # 3 Check output root paths (and create them if needed)
    symbols_full_path=\
        check_or_create_path(str(check_or_create_path(root_path+"/"+site+"/"+symbols_subpath))+"/symbols")
       
    counter=0
    # 4 Go through list of prices
    for symbol in symbols['symbol'].tolist():
        counter=counter+1
        
        # Looking for start symbol to start downloading
        # An start symbol should be provided for instance in case of a failed download 
        # This is only to avoid starting from scratch in those cases.
        if (download_symbol or symbol==from_symbol):
            download_symbol=True
            
            # Print current symbol to be downloaded as feedback.
            # It also prints the symbol index in the whole list of symbols to download. 
            print ""+str(counter)+"/"+str(len(symbols['symbol'].tolist()))+" = "+symbol
            
            # Trying to download the symbols 10 times.
            try_download(symbol=symbol,period=period,days=days,exchange=exchange,\
                         site=site,debug=debug,path=symbols_full_path+"/"+symbol+"/",\
                         name=symbol, tries_count=0)

    print("Done!")


"""
Check if the path does exist. If it doesnt, create a folder for the given symbol
""" 

def check_or_create_path(path):
    directory = os.path.dirname(path)
    try: 
        os.stat(directory)
    except: 
        os.mkdir(directory)
    return path


if __name__ == "__main__":

    # 1 Download SPX index
    for site in ["google_finance","alpha_vantage"]:
        #download_single_price_from("SPX", 60, 20, "USD", site ,True, RAW_DATA_PATH+site+"/"+"S&P500/index","S&P500")
        try_download("SPX", 60, 20, "USD", site ,True, RAW_DATA_PATH+site+"/"+"S&P500/index","S&P500", 0)

    # 2 Download all SPX stocks
    from_symbol="" #"LUK" #DAL" 
    for site in ["google_finance","alpha_vantage"]:
        download_list_of_prices(root_path=PATH+"/raw",list_file="SPX_list.csv",symbols_subpath="S&P500", \
                                period=60,days=20,exchange='USD',site=site,debug=False,from_symbol=from_symbol)

    # 3 Download SPY ETF
    for site in ["google_finance","alpha_vantage"]:
        try_download("SPY",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"S&P500/spy_eft","SPY",0)

    # 4 Download EURUSD
    for site in ["google_finance","alpha_vantage"]:
        try_download("EURUSD",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"EURUSD","EURUSD",0)

    # Extra: Download Bitcoin-USD
    for site in ["google_finance","alpha_vantage"]:
        try_download("BTCUSD",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"BITCOIN","BTCUSD",0)

    # Extra: Download NASDAQ
    for site in ["google_finance","alpha_vantage"]:
        # NASDAQ EFTs to track volumes
        try_download("QQQ",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"NASDAQ/qqq_eft","QQQ",0)

    # Extra: Downloading crypto
    for site in ["avantage_crypto"]:
        try_download("BTC",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"BITCOIN/","BTCUSD",0)
        try_download("ETH",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"ETHEREUM","ETHUSD",0)
        try_download("XRP",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"RIPPLE","XRPUSD",0)
        try_download("LTC",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"LITECOIN","LTCUSD",0)
        try_download("IOT",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"IOTA","IOTUSD",0)
        try_download("XMR",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"MONERO","XMRUSD",0)
        try_download("DASH",60,20,"USD",site,True,RAW_DATA_PATH+site+"/"+"DASH","DASHUSD",0)


