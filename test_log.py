#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases processing

Modul preskenuje adresare s html subormi SHMU
rozdeli zisti, ktore tabulky uz mozno ulozit a uzavriet
podla mesiacov ako neaktualne a ktore treba este spracovat

Created on Mon Aug  5 10:20:00 2024

'''

import pandas as pd
import re
import openpyxl
import datetime as dt
import sqlite3
import pyarrow
import logging
from pathlib import Path

from main import extract_tables_from_html, extract_date_from_html
from config import *

LOGFILE = "log.log"
LOGFILE_INF = "inf.log"

logger=logging.getLogger('log')
logger.addHandler(logging.FileHandler(TOPDIR + LOGFILE, mode='w'))
logger_inf = logging.getLogger('inf')
logger_inf.addHandler(logging.FileHandler(TOPDIR + LOGFILE_INF, mode='w'))
# resultfile = open(VRTLOZCSVQGIS, 'w') # do resultfile zapisuje len CVSReader a main a uzatvara ho main 



def to_num(df, cols):
    '''prevedie stlpce cols na numeric'''
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def set_logging(logdir):
    '''nastavi logging'''
    logger.handlers.clear()
    logger_inf.handlers.clear() 
    logger.addHandler(logging.FileHandler(logdir + LOGFILE, mode='a'))
    logger_inf.addHandler(logging.FileHandler(logdir + LOGFILE_INF, mode='a'))

    logger.setLevel(logging.DEBUG)
    logger_inf.setLevel(logging.DEBUG)

    logger.info(f'Start err logging to {LOGFILE}')
    logger_inf.info(f'Start inf logging to {LOGFILE_INF}'          )
    
def teploty():
    '''spracuje vsetky html subory v adresari TEPLOTY_SK_DIR
    extrahuje tabulky, prida stlpec datetime a ulozi do parquet
    '''
    set_logging(TEPLOTY_SK_DIR)
  
    df = pd.DataFrame()
    htmlfiles = Path(TEPLOTY_SK_DIR).glob('*00-45.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            month_from_filename = int(re.findall(r'(\d{4})-(\d{2})-\d{2}.*', file_path.name)[0][1])
            print(file_path, month_from_filename)
            tables = pd.read_html(file_path)
            table = tables[0]
            table.columns = table.columns.droplevel(0)
            table.Teplota = table.Teplota.str.replace(' °C','')
            table['Rýchlosť'] = table['Rýchlosť'].str.replace(' m/s','')
            table.Tlak = table.Tlak.str.replace(' hPa','')
            regex_pattern = r'sia - (.*) - (.*) LSE'
            [datum,cas] = extract_date_from_html(file_path, regex_pattern)
            if month_from_filename != int(datum.split('.')[1]):     
                print(f"Chybný mesiac v názve súboru {file_path} - {month_from_filename} != {datum.split('.')[1]}")
            table['datetime'] = dt.datetime.strptime(f'{datum} {cas}','%d.%m.%Y %H:%M')
            df = pd.concat([df, table])
            # print(df)
        else:
            logger.info(f"!!!Empty file!!!: {file_path}")
    df.drop_duplicates(inplace=True)
    df.sort_values(by=['datetime'], inplace=True)
    logger.info(f"Teploty: {len(df)} rows")
    logger_inf.info(f"Teploty: {len(df)} rows")
    logger_inf.info(f"min: {df.datetime.min()} - max: {df.datetime.max()}")
    
    brezno = df[df['Stanica'] == 'Brezno']
    # save_frame(df, TEPLOTY_SK_DIR, 'teploty')
    # save_frame(brezno, TEPLOTY_SK_DIR, 'teploty_brezno')
    print('done')

def uhrnycelk():   
    df = pd.DataFrame()
    htmlfiles = Path(ZRAZKY_SK_DIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = extract_tables_from_html(file_path,100)
            for tbl in tables:
                df = pd.concat([df, tbl])
        else:
            logger.error(f"Súbor {file_path} je prázdny")
    df = df[df['Čas merania'] != 'Priemery:']
    month_from_filename = int(re.findall(r'(\d{4})-(\d{2})-\d{2}.*', file_path.name)[0][1])

    df['datetime'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
    df = df.drop_duplicates(df, keep='first').sort_values(by='Čas merania')
    # save_frame(df, ZRAZKY_SK_DIR, 'zrazky_sk')    
    logger.info(f"ZRAZKY_SK - {len(df)} riadkov")
    
def hydrometricke_stanice():   
    df = pd.DataFrame()
    htmlfiles = Path(PRIETOKY_SK_DIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = extract_tables_from_html(file_path,100)
            regex_pattern = r'(?:<.*?>)?\s?(\d{1,2}\.\d{1,2}\.\d{4}) o (\d{1,2}:\d\d)'
            [datum,cas] = extract_date_from_html(file_path, regex_pattern)
            table=tables[0]
            table['datetime'] = (dtime := dt.datetime.strptime(f'{datum} {cas}','%d.%m.%Y %H:%M'))

            month_from_filename = int(re.findall(r'(\d{4})-(\d{2})-\d{2}.*', file_path.name)[0][1])
            if month_from_filename != int(dtime.month):      
                print(f"Chybný mesiac v názve súboru {file_path} - {month_from_filename} != {dtime.month}")

            df = pd.concat([df, table])
        else:
            print(f"!!!Empty file!!!: {file_path}")
    #cistenie dat
    df.columns = df.columns.droplevel(1) # odstranenie viacriadkovych hlaviciek 
    df = df.rename(columns={'∆H': 'dH', 'QM,N' : 'QMN'}) # premenovanie stlpca
    df.Z = df.Z.replace('-', pd.NA)
    df.Z = df.Z.replace('//', 0)
    df = to_num(df, ['H','dH','Q','Tvo','Tvz','Z','QMN'])
    df = df.drop_duplicates(df, keep='first').sort_values(by='datetime')
    # save_frame(df, PRIETOKY_SK_DIR, 'hydrometricke_stanice')

def create_data_dirs(year = 2025, month = 6, topdir = TOPDATADIR, datadirs=DATADIRS):
    '''vytvori adresare pre ukladanie dat'''
    year_month = f'{year:04d}-{month:02d}'
    topdir = topdir + year_month + '/'
    Path(topdir).mkdir(parents=True, exist_ok=True)
    for dirname in datadirs:
        Path(topdir + dirname).mkdir(parents=True, exist_ok=True)   

# create_data_dirs(2025, 7)
# create_data_dirs(2025, 8)
# create_data_dirs(2025, 9)
# create_data_dirs(2025, 10)
set_logging(TOPDIR)
logger_inf.info('Start teploty()')
logger.info('Start  teploty()')
# hydrometricke_stanice()   
teploty()
set_logging(TOPDIR)
logger_inf.info('End   teploty()')
logger.info('End    teploty()')

