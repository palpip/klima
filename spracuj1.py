#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases processing
Modul pre spracovanie dat SHMU a databaz
Modul je druhým krokom, načítava hotové databázy, spracúva denné
dáta/sumáre a oddelí Breznianske dáta
'''

import pandas as pd
import re
import openpyxl
import datetime as dt
import sqlite3
import pyarrow
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
logger.addHandler(logging.FileHandler(TOPDIR + LOGFILE, mode='a'))
logger_inf = logging.getLogger('inf')
logger_inf.addHandler(logging.FileHandler(TOPDIR + LOGFILE_INF, mode='a'))
logger.setLevel(logging.DEBUG)
logger_inf.setLevel(logging.DEBUG)

pd.DataFrame().to_excel(TOPDATADIR + 'vystup.xlsx', sheet_name = 'info', index=False)  # vytvorenie prazdneho xlsx suboru
# with pd.ExcelWriter(TOPDIR + 'vystup.xlsx', mode = 'w', engine='openpyxl') as EXCELWRITER:
#     pd.DataFrame.to_excel(EXCELWRITER, sheet_name='info', index=False)

def save_frame(df, dirname, dfname):
    '''ulozi dataframes v adresari dirname vo formate
    dfname.csv, dfname.xlsx, dfname.sqlite3'''
    # len breznovskych dat
    # df.to_csv(dirname + dfname + '.csv')
    # df.to_excel(dirname + dfname + '.xlsx', sheet_name=dfname, index=False)
    
    print('-----', dirname + dfname + '.xlsx')
    with pd.ExcelWriter(TOPDATADIR + 'vystup.xlsx', mode = 'a', engine='openpyxl', if_sheet_exists='replace') as EXCELWRITER:
        df.to_excel(EXCELWRITER, sheet_name=dfname, index=False)
    
    # conn = sqlite3.connect(dirname + dfname + '.sqlite')
    # df.to_sql(dfname, conn, if_exists='replace', index=False)    
    df.to_parquet(TOPDATADIR + dfname + '.parquet', engine='auto') 



def teploty(infile, lokalita='Brezno'):
    #read parquet file
    df = pd.read_parquet(infile + '.parquet')
    print('starting teploty')
    print(df.info())
    # make daily temperature averages
    # whole dataframe SK - not grouped by station
    # df_agg = df
    # df_agg = df_agg.set_index('Cas_CET').resample('D').Teplota.agg(['min', 'max', 'mean'])
    # df_agg.reset_index(inplace=True)
    # df_agg.sort_values(by='Cas_CET', inplace=True)
    # df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.set_index('Cas_CET').groupby('Stanica').resample('D').Teplota.agg(['min', 'max', 'mean'])
    df_agg.reset_index(inplace=True)
    df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    df_agg.sort_values(by=['Stanica','Cas_CET'], inplace=True)
    # save whole SK
    # save_frame(df_agg,TOPDATADIR+DATADIRS[0], 'teploty_denne_sk')
    
    # filter lokalita
    # surove data pre lokalitu 
    df_lokalita_raw = df[df['Stanica'] == lokalita]
    save_frame(df_lokalita_raw,TOPDATADIR+DATADIRS[0], 'teploty_'+ lokalita.replace(' - ', '_').lower())
    print(TOPDATADIR+DATADIRS[0], f"teploty_{lokalita}", ' Saved')
    
    df_lokalita_agg = df_agg[df_agg['Stanica'] == lokalita]
    save_frame(df_lokalita_agg,TOPDATADIR+DATADIRS[0], 'teploty_denne_'+lokalita.replace(' - ', '_').lower())
    print(TOPDATADIR+DATADIRS[0], f"teploty_denne_{lokalita}", ' Saved')
    


 
    
    
def zrazky_sk(infile, lokalita = 'Brezno'):
    df = pd.read_parquet(ZRAZKY_SK_DIR + 'zrazky_sk.parquet'   )
    # make daily averages
    
    # whole dataframe SK - not grouped by station
    # df_agg = df
    # df_agg = df_agg.set_index('Cas_CET').resample('D')['Zrážky 1h'].agg(['sum'])
    # df_agg.reset_index(inplace=True)
    # df_agg.sort_values(by='Cas_CET', inplace=True)
    # df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.set_index('Cas_CET').groupby('Stanica').resample('D')
    df_agg = df_agg['Zrážky 1h'].agg(['sum'])
    df_agg.reset_index(inplace=True)
    df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    df_agg.sort_values(by=['Stanica','Cas_CET'], inplace=True)

    # save whole SK
    # save_frame(df_agg,TOPDATADIR+DATADIRS[2], 'zrazky_denne_sk')
    
    
    # filter lokalita
    # surove data pre lokalitu 
    df_lokalita_raw = df[df['Stanica'] == lokalita]
    df_lokalita_raw = df_lokalita_raw.sort_values(by='Cas_CET')
    save_frame(df_lokalita_raw,TOPDATADIR+DATADIRS[2], 'zrazky_'+ lokalita.replace(' - ', '_').lower())
    print(TOPDATADIR+DATADIRS[2], f"zrazky_{lokalita}", ' Saved')
    
    df_lokalita_agg = df_agg[df_agg['Stanica'] == lokalita]
    df_lokalita_agg = df_lokalita_agg.sort_values(by='Cas_CET')
    save_frame(df_lokalita_agg,TOPDATADIR+DATADIRS[2], 'zrazky_denne_'+lokalita.replace(' - ', '_').lower())
    print(TOPDATADIR+DATADIRS[2], f"zrazky_denne_{lokalita}", ' Saved')
    
    # save_frame(df, ZRAZKY_SK_DIR, 'zrazky_sk')    
    logger.info(f"ZRAZKY_SK - {len(df)} riadkov")


    
def hladiny_sk(lokalita='Brezno', tok='Hron'):   
    df = pd.read_parquet(HLADINY_SK_DIR + 'hladiny_sk.parquet'   )
    # make daily averages
    # whole dataframe SK - not grouped by station nor Tok
    df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET')
    # df_agg = df
    # df_agg = df_agg.set_index('Cas_CET').resample('D')['Vodný stav'].agg(['min', 'max', 'mean'])
    # df_agg.reset_index(inplace=True)
    # df_agg.sort_values(by='Cas_CET', inplace=True)
    # df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    # # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.set_index('Cas_CET').groupby(['Stanica', 'Tok']).resample('D')
    df_agg = df_agg['Vodný stav'].agg(['min', 'max', 'mean'])
    df_agg.reset_index(inplace=True)
    df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    df_agg.sort_values(by=['Stanica', 'Tok', 'Cas_CET'], inplace=True)  
    
    # save whole SK
    #save_frame(df, HLADINY_SK_DIR, 'hladiny_sk')    
    
    # filter lokalita
    # surove data pre lokalitu
    df_lokalita_raw = df[(df['Stanica'] == lokalita) & (df['Tok'] == tok)]
    df_lokalita_raw = df_lokalita_raw.sort_values(by='Cas_CET')
    save_frame(df_lokalita_raw,TOPDATADIR+DATADIRS[3], 'hladiny_'+ lokalita.replace(' - ', '_').lower())
    print(TOPDATADIR+DATADIRS[3], f"hladiny_{lokalita}", ' Saved')
    # denné agregované data pre lokalitu
    df_lokalita_agg = df_agg[(df_agg['Stanica'] == lokalita) & (df_agg['Tok'] == tok)]
    df_lokalita_agg = df_lokalita_agg.sort_values(by='Cas_CET')
    save_frame(df_lokalita_agg,TOPDATADIR+DATADIRS[3], 'hladiny_denne_'+lokalita.replace(' - ', '_').lower())
    print(TOPDATADIR+DATADIRS[3], f"hladiny denne_{lokalita}", ' Saved')
    # filter by station and tok    
    logger.info(f"HLADINY_SK - {len(df)} riadkov")  
    


def prietoky_sk(lokalita='Brezno - Hron'):   
    df = pd.read_parquet(PRIETOKY_SK_DIR + 'prietoky_sk.parquet'   )
    df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET')
    dfb = df[df['Stanica - tok'] == lokalita]
    dfb.loc[:,['Cas_CET']] = pd.to_datetime(dfb['Cas_CET'].dt.date, errors='coerce')
    dfb = dfb.sort_values(by='Cas_CET')    
    save_frame(dfb, PRIETOKY_SK_DIR, 'prietoky_'+ lokalita.replace(' - ', '_').lower())    
    logger.info(f"PRIETOKY_BREZNO - {len(dfb)} riadkov")
    # save_frame(df, PRIETOKY_SK_DIR, 'prietoky_sk')
    # logger.info(f"PRIETOKY_SK - {len(df)} riadkov")
    
def main():
    start = dt.datetime.now()
    teploty(DATABASES[0])
    logger.info(f"{teploty.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    start = dt.datetime.now()
    prietoky_sk()
    logger.info(f"{prietoky_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    start = dt.datetime.now()
    hladiny_sk()
    logger.info(f"{hladiny_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    # start = dt.datetime.now()
    zrazky_sk(DATABASES[2])    
    logger.info(f"{zrazky_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    start = dt.datetime.now()
    # zrazky_brezno()        
    # logger.info(f"{zrazky_brezno.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    print('done')
    
if __name__ == "__main__":
    main()  
