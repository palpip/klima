#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases tesing

Modul preskenuje adresare s parquet subormi SHMU
rozdeli zisti, ktore tabulky uz mozno ulozit a uzavriet
podla mesiacov ako neaktualne a ktore treba este spracovat

Created on 

'''

import pandas as pd
import re
import openpyxl
import datetime as dt
import sqlite3
import pyarrow
import logging
from pathlib import Path
import sys
import os

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SRC_DIR))

from config import *

LOGFILE = "log.log"
LOGFILE_INF = "inf.log"

logger=logging.getLogger('log')
logger.addHandler(logging.FileHandler(TOPDIR + LOGFILE, mode='w'))
logger_inf = logging.getLogger('inf')
logger_inf.addHandler(logging.FileHandler(TOPDIR + LOGFILE_INF, mode='w'))


def check_month_file_measure(fn):
    '''kontrola suladu mesiaca v stlpci Cas_CET a v nazve suboru
    fn - nazov suboru
    '''
    df = pd.read_parquet(fn)
    #vytvor pomocne stlpce s mesiacmi
    df['M1'] = df.Cas_CET.dt.month.astype(int)
    df['M2'] = df.file.str[5:7].astype(int)


    dr = df[df.M1 != df.M2]
    print(fn, len(dr))
    if len(dr) > 0:
        print(dr.head(30))
        logger.error(f'File {fn} has wrong month in {len(dr)} rows')
    else:
        logger.info(f'File {fn} OK')
    # dfresult = df[df.Cas_CET.dt.month == 9]
    # print(dfresult.head())
    
          

def check_month():
    check_month_file_measure(TEPLOTY_SK_DIR +  r'teploty_sk.parquet')
    check_month_file_measure(HLADINY_SK_DIR +  r'hladiny_sk.parquet')
    check_month_file_measure(PRIETOKY_SK_DIR +  r'prietoky_sk.parquet')
    check_month_file_measure(ZRAZKY_SK_DIR +  r'zrazky_sk.parquet')
    check_month_file_measure(ZRAZKY_BREZNO_DIR +  r'zrazky_brezno.parquet')


    # check_month_file_measure(TOPDATADIR +  'teploty_sk.parquet')    #correct

def test_parquet_append():
    '''test pripojenia dat do parquet suboru'''
    df1 = pd.read_parquet(TEPLOTY_SK_DIR +  r'teploty_sk.parquet')
    print(df1.head())
    print('df1 len:{}'.format(len(df1)))
    df2 = pd.read_parquet(TEPLOTY_SK_DIR +  r'teploty_sk.parquet')
    print(df2.head())
    print('df2 len:{}'.format(len(df2)))
    df3 = pd.concat([df1, df2], ignore_index=True)
    print('df3 len:{}'.format(len(df3)))
    df3.to_parquet(TEPLOTY_SK_DIR +  r'teploty_skTmp.parquet', compression='snappy')
    df4 = pd.read_parquet(TEPLOTY_SK_DIR +  r'teploty_skTmp.parquet')
    print(f'df4 len:{len(df4)}')
    df4 = df4.drop_duplicates()
    print(f'df4 len after drop_duplicates:{len(df4)}')
    logger_inf.info(f'test_parquet_append: df1 len:{len(df1)}, df2 len:{len(df2)}, df4 len after drop_duplicates:{len(df4)}')
    print(df4.head(20))
    print(df4.tail(20))
    
def parquet_infos():
    print("Parquet file infos:")
    for fn in DATABASES:
        fn = fn + '.parquet'
        if Path(fn).exists():
            df = pd.read_parquet(fn)
            print('\n\n-------------------\n')
            print(f"File: {fn}, Rows: {len(df)}, Columns: {df.columns.tolist()}")
            print(df.head(5))
            print(df.dtypes, df.info(), df.describe(include='all'))
        else:
            print(f"File: {fn} does not exist.")





def sqlite_infos():
    print("Database record counts:")
    for db in DATABASES:
        conn = sqlite3.connect(db + '.sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"Database: {db}, Table: {table_name}, Record count: {count}")
        conn.close()
    
    '''zisti informacie o databazach'''

    for db in DATABASES:
        conn = sqlite3.connect(db + '.sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("Tables in the database:")
        for table in tables:
            print(table[0])
            cursor.execute(f"PRAGMA table_info({table[0]});")
            columns = cursor.fetchall()
            print("Columns:")
            for column in columns:
                print(column)
        conn.close()

def main():
    print(DATABASES)
    parquet_infos()
    # sqlite_infos()
    # test_parquet_append()
 

if __name__ == "__main__":
    main()  
