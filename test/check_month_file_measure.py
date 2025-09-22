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
    
          

def main():
    check_month_file_measure(TEPLOTY_SK_DIR +  r'teploty_sk.parquet')
    check_month_file_measure(HLADINY_SK_DIR +  r'hladiny_sk.parquet')
    check_month_file_measure(PRIETOKY_SK_DIR +  r'prietoky_sk.parquet')
    check_month_file_measure(ZRAZKY_SK_DIR +  r'zrazky_sk.parquet')
    check_month_file_measure(ZRAZKY_BREZNO_DIR +  r'zrazky_brezno.parquet')


    # check_month_file_measure(TOPDATADIR +  'teploty_sk.parquet')    #correct
    
if __name__ == "__main__":
    main()  
