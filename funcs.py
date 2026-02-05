#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import datetime as dt
import logging
from config import *
from sqlalchemy import create_engine, text


# nastavenie logovania
# logovanie do log.log - chyby/debug
# logovanie do inf.log - informacie


def get_data_from_parquets(directory, filter_query=None):
    '''
    Read and concatenate parquet files from a directory, with optional filtering.
    Returns an empty DataFrame if no parquet files are found.
    '''
    p = Path(directory)
    files = sorted([str(f) for f in p.glob('*.parquet')])
    # Process files one by one to avoid high memory usage
    df_list = []
    for f in files:
        df_part = pd.read_parquet(f)
        if filter_query:
            df_part = df_part.query(filter_query)
        df_list.append(df_part)
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
    else:
        df = pd.DataFrame()
    return df
 
def pack_to_zip(filelist, zipfilename):
    '''zabali subory filelist do zip suboru zipfilename'''
    import zipfile
    with zipfile.ZipFile(zipfilename, 'w') as zipf:
        for file in filelist:
            zipf.write(file, arcname=Path(file).name)
    logger.info(f'Vytvorený ZIP súbor: {zipfilename}')

def log_elapsed_time(func):
    start = dt.datetime.now()
    func()
    elapsed = dt.datetime.now() - start
    logger.info(f"{func.__name__}: Celkový čas spracovania: {elapsed}")


def remove_duplicates(df):
    nr_records = df.Cas_CET.count()
    if 'file' in (l := list(df)):  #napodiv vracia stlpce do listu
        l.remove('file')
    df =df.drop_duplicates(l, keep='first').sort_values(by='Cas_CET')
    logger.info(f"zaznamov: {nr_records}, duplikatov:  {nr_records - df.Cas_CET.count()}")
    print(f"zaznamov: {nr_records}, duplikatov:  {nr_records - df.Cas_CET.count()}")
    return df

def create_logger(logname):
    '''Create and return a logger with the specified name'''
    logger = logging.getLogger(logname)
    logger.addHandler(logging.FileHandler(TOPDIR + f"{logname}{dt.datetime.now().strftime('%Y%m%d%H%M%S')}.log", mode='w'))
    logger.setLevel(logging.DEBUG)
    return logger

logger = create_logger('log')
logger_inf = create_logger('inf')


# LOGFILE = "log1.log"
# LOGFILE_INF = "inf.log" 
# logger=logging.getLogger('log')
# logger.addHandler(logging.FileHandler(TOPDIR + LOGFILE, mode='w'))
# logger_inf = logging.getLogger('inf')
# logger_inf.addHandler(logging.FileHandler(TOPDIR + LOGFILE_INF, mode='w'))
# logger.setLevel(logging.DEBUG)
# logger_inf.setLevel(logging.DEBUG)

#------------------
# not used - automatic selection of connection string and data directory
#------------------
# def test_postgresql_connection(connstr):
#     '''Test connection to PostgreSQL database'''
#     try:
#         engine = create_engine(connstr)
#         with engine.connect() as conn:
#             result = conn.execute(text("SELECT 1")).first()
#             if result[0] == 1:
#                 return True
#             else:
#                 return False 
#     except Exception as e:
#         return False

# CONNSTR = None
# for connstr in CONNSTRS:
#     if test_postgresql_connection(connstr):
#         print(f'Connection successful: {connstr}')
#         CONNSTR = connstr
#         break
# print (CONNSTR)

# SHMUDIR = ''
# for shmudir in SHMUDIRS:
#     if Path(shmudir).exists():
#         print(f'SHMU data directory found: {shmudir}')
#         SHMUDIR = shmudir
#         break
# print(SHMUDIR)    
