"""

#############################
#       marked_data_manager.py
#       Version 1.0
#       2019-08-24
#       Tom Nordal
#############################

"""

import os


MARKED_DATA_DIR = 'C:\\Users\\tnord\\Documents\\marked_data'

def get_marked_dir(marked, interval):
    """Get path for a marked

    Parameters
    ----------
    marked : str     
        marked = 'NO', 'SE' or 'US'
    interval : str
        interval = 'days', 'weeks' or 'months'
    
    Returns
    -------
    str
        Full path to folder with matked data
    """
    return os.path.join(MARKED_DATA_DIR,'data', marked, interval)

def get_ticker_list_dir():
    return os.path.join(MARKED_DATA_DIR, 'ticker_lists')

def get_ticker_list_file(filename):  
    return os.path.join(get_ticker_list_dir(), filename)

def make_ticker_list_from_dir(ticer_dir):
    """Extract all ticker from ticker_dir."""

    ticers = []
    for file in os.listdir(ticer_dir):
        ticers.append(file[:-4])
    return ticers


if __name__ == '__main__':
   print('this.__doc__')