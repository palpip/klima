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

# nastavenie logovania
# logovanie do log.log - chyby/debug
# logovanie do inf.log - informacie

LOGFILE = "log1.log"
LOGFILE_INF = "inf.log" 
logger=logging.getLogger('log')
logger.addHandler(logging.FileHandler(TOPDIR + LOGFILE, mode='w'))
logger_inf = logging.getLogger('inf')
logger_inf.addHandler(logging.FileHandler(TOPDIR + LOGFILE_INF, mode='w'))
logger.setLevel(logging.DEBUG)
logger_inf.setLevel(logging.DEBUG)



def extract_tables_from_html(html_content, tableno = 0):
    """Extract all tables from an HTML content string.
    :param html_content: str, HTML content containing tables
    :param tableno: int, index of the table to extract 
                    (0) for first table with multilevel header
                    (1) for second table with single level header
                    (100) for all tables as list of DataFrames
    :return: list of DataFrames
    """
    tables = pd.read_html(html_content)        # for i, table in enumerate(tables):
    if tableno == 0:
        tbl = tables[tableno]
        tbl.columns = tbl.columns.droplevel(0)
    elif tableno == 1:
        tbl = tables[tableno]
    else:    
        tbl = tables
    return tbl


def extract_date_from_html(htmlfile, regex_pattern):
    """Extract date from HTML content.
    :param html_content: str, HTML content containing date information
    :param regex_pattern: str, regex pattern to match the date
    :return: extracted date and time as a list    
    """
    with open(htmlfile, 'r') as file:
        retval = re.findall(regex_pattern, file.read())
    return [retval[0][0], retval[0][1]] 
    
def find_files_with_extension(directory, extension):
    """
    Recursively find all files with the given extension in the directory.
    :param directory: str or Path, the root directory to search
    :param extension: str, file extension (e.g., '.html')
    :return: generator of Path objects
    """
    return Path(directory).rglob(f'*{extension}')


def save_frame(df, dirname, dfname):
    '''ulozi dataframes v adresari dirname vo formate
    dfname.csv, dfname.xlsx, dfname.sqlite3'''
    df.to_csv(dirname + dfname + '.csv')
    df.to_excel(dirname + dfname + '.xlsx')
    conn = sqlite3.connect(dirname + dfname + '.sqlite')
    df.to_sql(dfname, conn, if_exists='replace', index=False)    
    df.to_parquet(dirname + dfname + '.parquet', engine='auto') 

def to_num(df, cols):
    '''prevedie stlpce cols na numeric'''
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce', dtype_backend="pyarrow")
    return df

def to_decimal(df, cols):
    '''prevedie stlpce cols na decimal'''
    for col in cols:
        df[col] = df[col].astype(pd.ArrowDtype(pa.decimal128(21, 2)))
    return df

def teploty():
    df = pd.DataFrame()
    htmlfiles = Path(TEPLOTY_SK_DIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = pd.read_html(file_path)
            table = tables[0]
            table.columns = table.columns.droplevel(0)
            table.Teplota = table.Teplota.str.replace(' °C','').apply(pd.to_numeric, errors='coerce').astype(float)
            table['Rýchlosť'] = table['Rýchlosť'].str.replace(' m/s','').apply(pd.to_numeric, errors='coerce').astype(float)
            table.Tlak = table.Tlak.str.replace(' hPa','').apply(pd.to_numeric, errors='coerce').astype(float)
            regex_pattern = r'sia - (.*) - (.*) LSE'
            [datum,cas] = extract_date_from_html(file_path, regex_pattern)
            table['Cas_CET'] = dt.datetime.strptime(f'{datum} {cas}','%d.%m.%Y %H:%M')
            table['file'] = file_path.name.split('.')[0]
            df = pd.concat([df, table])
            # print(df)
        else:
            logger.error(f"Súbor {file_path} je prázdny")
    df.drop_duplicates(inplace=True)
    df.sort_values(by=['Cas_CET'], inplace=True)
    save_frame(df, TEPLOTY_SK_DIR, 'teploty_sk')
    
    brezno = df[df['Stanica'] == 'Brezno']
    save_frame(brezno, TEPLOTY_SK_DIR, 'teploty_brezno')
    logger.info(f"TEPLOTY_SK - {len(df)} riadkov")
    logger.info(f"TEPLOTY_BREZNO - {len(brezno)} riadkov")
    
def zrazky_brezno():   
    df = pd.DataFrame()
    htmlfiles = Path(ZRAZKY_BREZNO_DIR).glob('*.html')
    for file_path in htmlfiles:
        print(file_path)
        table = extract_tables_from_html(file_path,1)
        table['file'] = file_path.name.split('.')[0]
        df = pd.concat([df, table])
    
    df['Cas_CET'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
    df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET')
    save_frame(df, ZRAZKY_BREZNO_DIR, 'zrazky_brezno')    
    logger.info(f"ZRAZKY_BREZNO - {len(df)} riadkov")
    
    
def zrazky_sk():   
    df = pd.DataFrame()
    htmlfiles = Path(ZRAZKY_SK_DIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = extract_tables_from_html(file_path,100)
            for table in tables:
                table['file'] = file_path.name.split('.')[0]
                df = pd.concat([df, table])
        else:
            logger.error(f"Súbor {file_path} je prázdny")
    df = df[df['Čas merania'] != 'Priemery:']
    df['Cas_CET'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
    df = df.drop_duplicates(df, keep='first').sort_values(by='Čas merania')
    save_frame(df, ZRAZKY_SK_DIR, 'zrazky_sk')    
    logger.info(f"ZRAZKY_SK - {len(df)} riadkov")
    
def hladiny_sk():   
    df = pd.DataFrame()
    htmlfiles = Path(HLADINY_SK_DIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = extract_tables_from_html(file_path,100)
            for table in tables:
                table['file'] = file_path.name.split('.')[0]
                df = pd.concat([df, table])
        else:
            logger.error(f"Súbor {file_path} je prázdny")
    df['Cas_CET'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
    df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET')
    save_frame(df, HLADINY_SK_DIR, 'hladiny_sk')    
    logger.info(f"HLADINY_SK - {len(df)} riadkov")
    

def prietoky_sk():   
    df = pd.DataFrame()
    htmlfiles = Path(PRIETOKY_SK_DIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = extract_tables_from_html(file_path,100)
            #pattern pre datum a cas
            regex_pattern = r'(?:<.*?>)?\s?(\d{1,2}\.\d{1,2}\.\d{4}) o (\d{1,2}:\d\d)'
            [datum,cas] = extract_date_from_html(file_path, regex_pattern)
            table=tables[0]
            table['Cas_CET'] = dt.datetime.strptime(f'{datum} {cas}','%d.%m.%Y %H:%M')
            table['file'] = file_path.name.split('.')[0]
            df = pd.concat([df, table])
        else:
            logger.error(f"Súbor {file_path} je prázdny")
    #cistenie dat
    df.columns = df.columns.droplevel(1) # odstranenie viacriadkovych hlaviciek 
    df = df.rename(columns={'∆H': 'dH', 'QM,N' : 'QMN'}) # premenovanie stlpca
    df.Z = df.Z.replace('-', pd.NA).replace('//', 0)    # nahradenie hodnoty '-' na NaN  a '//' na 0, OVERENE
    df = to_num(df, ['H','dH','Q','Tvo','Tvz','Z','QMN']) # prevedenie na numeric
    df = to_decimal(df, ['H','dH','Q','Tvo','Tvz','Z','QMN']) # prevedenie na decimal
    
    df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET')
    save_frame(df, PRIETOKY_SK_DIR, 'prietoky_sk')
    logger.info(f"PRIETOKY_SK - {len(df)} riadkov")
    
def main():
    start = dt.datetime.now()
    prietoky_sk()
    logger.info(f"{prietoky_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    start = dt.datetime.now()
    hladiny_sk()
    logger.info(f"{hladiny_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    start = dt.datetime.now()
    zrazky_sk()    
    logger.info(f"{zrazky_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    start = dt.datetime.now()
    zrazky_brezno()        
    logger.info(f"{zrazky_brezno.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    start = dt.datetime.now()
    teploty()
    logger.info(f"{teploty.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    print('done')

if __name__ == "__main__":
    main()  
