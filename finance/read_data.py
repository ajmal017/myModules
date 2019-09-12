"""

#############################
#       read_data.py
#       Version 1.1
#       2019-08-24
#       Tom Nordal
#############################
"""

import os
import pandas as pd
import pandas_datareader.data as pdr
import datetime
import sys


def write_to_csv(DF, dir_csv, filename):
    """Save a dataframe to a csv file."""
    
    file_path = os.path.join(dir_csv, filename + '.csv')
    DF.to_csv(file_path)

def from_yahoo_fetch_tickers(ticker_list, StartDate, EndDate, interval, dir_to_save_csv):
    """Get stock data from Yahoo

    Parameter
    ---------
    ticker_list : list
        A list of tickers to download
    StartDate : Datetime
        Oldes date to download
    EndDate : Datetime
        Newes date to download
    interval : str
        'd' = dayly data, 'w' = weekly data, 'm' = monthtly data
    dir_to_save_scv : str
        Folder to save downloaded data files
    
    Returns
    -------
    dictionary
        a dict with all downloaded data
    
    """

    tickers = ticker_list
    ohlc_dict = {} 
    temp = pd.DataFrame()   
    attempt = 0 # initializing passthrough variable
    drop = [] # initializing list to store tickers whose close price was successfully extracted
    while len(tickers) != 0 and attempt <= 5:
        print("-----------------")
        print("attempt number ",attempt) 
        print("-----------------")    
        tickers = [j for j in tickers if j not in drop] # removing stocks whose data has been extracted from the ticker list
        for i in range(len(tickers)):
            try:
                temp = pdr.get_data_yahoo(tickers[i], StartDate.strftime('%Y-%m-%d'), EndDate.strftime('%Y-%m-%d'), interval=interval)
                temp.dropna(inplace = True)
                write_to_csv(temp,dir_to_save_csv, tickers[i])
                ohlc_dict[tickers[i]] = temp
                drop.append(tickers[i])       
            except:
                print(tickers[i]," :failed to fetch data...retrying")
                continue
        attempt+=1
    return ohlc_dict

def from_yahoo_fetch_one_ticker(ticker, StartDate, EndDate, interval, dir_to_save_csv=None):
    """Get stock data from Yahoo for one ticker."""

    ticker_data = pd.DataFrame()
    ticker_data = pdr.get_data_yahoo(ticker, StartDate.strftime('%Y-%m-%d'), EndDate.strftime('%Y-%m-%d'), interval=interval)
    if dir_to_save_csv != None:
        write_to_csv(ticker_data,dir_to_save_csv,ticker)

    return ticker_data


# Fetch from files
def from_csv_in_dir_fetch_tickers(ticker_list, file_path):
    """Read stock data from csv files."""

    ohlc_dict = {}
    temp = pd.DataFrame()
    tickers = ticker_list
    drop = []
    for i in range(len(tickers)):
        for file in os.listdir(file_path):
            if tickers[i] in file:
                # print('----- Reading from: ' + tickers[i] + ' -----')
                file_to_read = os.path.join(file_path, file)
                temp = pd.read_csv(file_to_read, index_col='Date')
                ohlc_dict[tickers[i]] = temp
                drop.append(tickers[i])
    return ohlc_dict, drop

# Fetch from files
def fetch_all_tickers_in_dir(file_path):
    """Read stock data from all csv files in a directory."""

    ohlc_dict = {}
    temp = pd.DataFrame()
    drop = []
    for file in os.listdir(file_path):
        print('----- Reading from: ' + file + ' -----')
        file_to_read = os.path.join(file_path, file)
        temp = pd.read_csv(file_to_read, index_col='Date')
        ohlc_dict[file[:-4]] = temp
        drop.append(file[:-4])
    return ohlc_dict, drop


def fetch_ticker_from_csv(file_to_read):
    """Read stock data from a csv file."""

    ticker_data = pd.DataFrame()
    ticker_data = pd.read_csv(file_to_read, index_col='Date')
    return ticker_data


def update_tickers(ticker_list, file_path, retries=5):
    """Append new stock data from Yahoo to existing data files.

    Parameters
    ----------
    ticker_list: list
        list of tickers to update
    file_path : str
        folder to data files
    retries : int, optional
        number of retries when downloading data from Yahoo

    Returns
    -------
        list of tickers succesfulyy downloaded
    
    """

    tickers_success = []
    for i in range(len(ticker_list)):
        print('---------- Update ticker: {0} ----------'.format(ticker_list[i]))
        file_name = os.path.join(file_path, ticker_list[i] + '.csv')
        df_old_values = fetch_ticker_from_csv(file_name)
        last_date = df_old_values.tail(1).index[0]
        startdate = datetime.datetime.strptime(last_date, '%Y-%m-%d') + datetime.timedelta(2)
        enddate = datetime.date.today()
        enddate2 = datetime.datetime.combine(enddate, datetime.time(0,0))

        if enddate2 > startdate:
            df_new_values = pd.DataFrame()
            attempts = 0
            while attempts <= retries and df_new_values.shape[0] < 1:
                try:
                    df_new_values =from_yahoo_fetch_one_ticker(ticker_list[i], startdate, enddate, 'd')
                    tickers_success.append(ticker_list[i])
                    df_old_values = df_old_values.append(df_new_values, ignore_index=False)
                    df_old_values.reset_index(inplace=True)
                    df_old_values['Date'] = pd.to_datetime(df_old_values.Date)
                    df_old_values.set_index('Date', inplace=True)
                    write_to_csv(df_old_values, file_path, ticker_list[i])
                except:
                    print(ticker_list[i], ' :failed to fetch data...retrying')
                    attempts += 1
                    continue
    return tickers_success
        



if __name__ == '__main__':
    ticker = ['TEL.OL', 'TOM.OL', 'NHY.OL']
    fpath = 'C:\\Users\\tnord\\Documents\\marked_data\\data\\NO\\days'

    ts = update_tickers(ticker, fpath)
    print(ts)