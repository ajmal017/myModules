import os
import pandas as pd
import pandas_datareader.data as pdr
import datetime

#############################
#       read_data.py
#       Version 1.0
#       2019-08-10
#       Tom Nordal
#############################


def write_to_csv(DF, dir_csv, filename):
    file_path = os.path.join(dir_csv, filename + '.csv')
    DF.to_csv(file_path)

def from_yahoo_fetch_tickers(ticker_list, StartDate, EndDate, interval, dir_to_save_csv):
    # Fetch tickers
    # Interval = 'd', 'w' or 'm'
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

def from_yahoo_fetch_one_ticker(ticker, StartDate, EndDate, interval, dir_to_save_csv):
    tickers = [ticker]
    return from_yahoo_fetch_tickers(tickers, StartDate, EndDate, interval, dir_to_save_csv)

# Fetch from files
def from_csv_in_dir_fetch_tickers(ticker_list, file_path):
    ohlc_dict = {}
    temp = pd.DataFrame()
    tickers = ticker_list
    drop = []
    for i in range(len(tickers)):
        for file in os.listdir(file_path):
            if tickers[i] in file:
                print('----- Reading from: ' + tickers[i] + ' -----')
                file_to_read = os.path.join(file_path, file)
                temp = pd.read_csv(file_to_read, index_col='Date')
                ohlc_dict[tickers[i]] = temp
                drop.append(tickers[i])
    return ohlc_dict, drop

# Fetch from files
def fetch_all_tickers_in_dir(file_path):
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

