#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases processing

Modul pre spracovanie dat SHMU a databaz
Created on Mon Aug  5 10:20:00 2024
'''

import pandas as pd
import re
import openpyxl
import datetime as dt
import sqlite3
import pyarrow as pa
import logging

from pathlib import Path
from config import *
import logging
from sqlalchemy import create_engine

# nastavenie logovania
# logovanie do log.log - chyby/debug
# logovanie do inf.log - informacie

LOGFILE = "log2.log"
LOGFILE_INF = "inf2.log" 
logger=logging.getLogger('log')
logger.addHandler(logging.FileHandler(TOPDIR + LOGFILE, mode='w'))
logger_inf = logging.getLogger('inf')
logger_inf.addHandler(logging.FileHandler(TOPDIR + LOGFILE_INF, mode='w'))
logger.setLevel(logging.DEBUG)
logger_inf.setLevel(logging.DEBUG)

def list_parquet_files(directory):
    '''List all parquet files in the given directory'''
    p = Path(directory)
    return sorted([str(f) for f in p.glob('*.parquet')])

def get_data_from_parquets(directory, filter_query=None):
    '''Read and concatenate parquet files from a directory, with optional filtering'''
    files = list_parquet_files(directory)
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    if filter_query:
        df = df.query(filter_query)
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

def main():
    
    
    workflow = [RES_TEPLOTY_SK_DIR, RES_ZRAZKY_BREZNO_DIR, RES_ZRAZKY_SK_DIR, 
                RES_HLADINY_SK_DIR, RES_PRIETOKY_SK_DIR, RES_PODZEMNE_VODY_SK_DIR]
    for dir in workflow:
        df = get_data_from_parquets(dir)
        logger_inf.info(f'Načítané dáta z {dir}: {df.shape[0]} riadkov, {df.shape[1]} stĺpcov')
        df.info()
    print('done')

if __name__ == "__main__":
    main()  
