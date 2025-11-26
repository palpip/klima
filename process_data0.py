#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases processing
Modul pre spracovanie dat SHMU a databaz
Modul je druhým krokom, načítava hotové databázy, spracúva denné
dáta/sumáre a oddelí Breznianske dáta
'''

import pandas as pd
import datetime as dt
import re
import openpyxl
import sqlite3
import pyarrow
import logging

from pathlib import Path
from config import *
from funcs import *

pd.DataFrame().to_excel(TOPRESDIR + 'vystupxx.xlsx', sheet_name = 'info', index=False)  # vytvorenie prazdneho xlsx suboru
# with pd.ExcelWriter(TOPDIR + 'vystup.xlsx', mode = 'w', engine='openpyxl') as EXCELWRITER:
#     pd.DataFrame.to_excel(EXCELWRITER, sheet_name='info', index=False)

def save_frame(df, dirname, dfname):
    '''ulozi dataframes v adresari dirname vo formate
    dfname.csv, dfname.xlsx, dfname.sqlite3'''
    # len breznovskych dat
    # df.to_csv(dirname + dfname + '.csv')
    # df.to_excel(dirname + dfname + '.xlsx', sheet_name=dfname, index=False)
    
    print('-----', dirname + dfname + '.xlsx')
    with pd.ExcelWriter(TOPRESDIR + 'vystupxx.xlsx', mode = 'a', engine='openpyxl', if_sheet_exists='replace') as EXCELWRITER:
        df.to_excel(EXCELWRITER, sheet_name=dfname, index=False)
    
    # conn = sqlite3.connect(dirname + dfname + '.sqlite')
    # df.to_sql(dfname, conn, if_exists='replace', index=False)    
    df.to_parquet(TOPRESDIR + dfname + '.parquet', engine='auto') 



def teploty(infile=TEPLOTY_SK_DIR+'teploty_sk', lokalita='Brezno'):
    #read parquet file
    df = pd.read_parquet(infile + '.parquet')
    print('starting teploty')
    print(df.info())
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.set_index('Cas_CET').groupby('Stanica').resample('D').Teplota.agg(['min', 'max', 'mean'])
    df_agg.reset_index(inplace=True)
    df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    df_agg.sort_values(by=['Stanica','Cas_CET'], inplace=True)
    # save whole SK
    # save_frame(df_agg,TOPRESDIR+DATADIRS[0], 'teploty_denne_sk')
    
    # filter lokalita
    # surove data pre lokalitu 
    df_lokalita_raw = df[df['Stanica'] == lokalita]
    save_frame(df_lokalita_raw,TOPRESDIR+DATADIRS[0], 'teploty_'+ lokalita.replace(' - ', '_').lower())
    print(TOPRESDIR+DATADIRS[0], f"teploty_{lokalita}", ' Saved')
    
    df_lokalita_agg = df_agg[df_agg['Stanica'] == lokalita]
    save_frame(df_lokalita_agg,TOPRESDIR+DATADIRS[0], 'teploty_denne_'+lokalita.replace(' - ', '_').lower())
    print(TOPRESDIR+DATADIRS[0], f"teploty_denne_{lokalita}", ' Saved')
        
def zrazky_sk(infile=ZRAZKY_SK_DIR + 'zrazky_sk', lokalita = 'Brezno'):
    df = pd.read_parquet(infile + '.parquet'   )
    # make daily averages
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.groupby(['Stanica', pd.Grouper(key='Cas_CET', freq='D')], observed=True)['Zrážky 1h'].agg(['sum', 'count']).reset_index()
    
    # save whole SK
    save_frame(df_agg,TOPRESDIR+DATADIRS[2], 'zrazky_denne_sk')
       
    # filter lokalita
    # surove data pre lokalitu 
    df_lokalita_raw = df[df['Stanica'] == lokalita]
    df_lokalita_raw = df_lokalita_raw.sort_values(by='Cas_CET')
    save_frame(df_lokalita_raw,TOPRESDIR+DATADIRS[2], 'zrazky_'+ lokalita.replace(' - ', '_').lower())
    print(TOPRESDIR+DATADIRS[2], f"zrazky_{lokalita}", ' Saved')
    
    df_lokalita_agg = df_agg[df_agg['Stanica'] == lokalita]
    # df_lokalita_agg = df_lokalita_agg.sort_values(by='Cas_CET')
    save_frame(df_lokalita_agg,TOPRESDIR+DATADIRS[2], 'zrazky_denne_'+lokalita.replace(' - ', '_').lower())
    print(TOPRESDIR+DATADIRS[2], f"zrazky_denne_{lokalita}", ' Saved')
    
    # save_frame(df, ZRAZKY_SK_DIR, 'zrazky_sk')    
    logger.info(f"ZRAZKY_SK - {len(df)} riadkov")

def hladiny_sk(lokalita='Brezno', tok='Hron'):   
    df = pd.read_parquet(HLADINY_SK_DIR + 'hladiny_sk.parquet'   )
    df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET')
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.groupby(['Stanica','Tok', pd.Grouper(key='Cas_CET', freq='W')], observed=True)['Vodný stav'].agg(['min', 'max', 'mean']).reset_index()
    # df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    # df_agg.sort_values(by=['Stanica', 'Tok', 'Cas_CET'], inplace=True)  
    
    # save whole SK
    save_frame(df_agg, HLADINY_SK_DIR, 'hladiny_sk')    
    
    # filter lokalita
    # surove data pre lokalitu
    df_lokalita_raw = df[(df['Stanica'] == lokalita) & (df['Tok'] == tok)]
    df_lokalita_raw = df_lokalita_raw.sort_values(by='Cas_CET')
    save_frame(df_lokalita_raw,TOPRESDIR+DATADIRS[3], 'hladiny_'+ lokalita.replace(' - ', '_').lower())
    print(TOPRESDIR+DATADIRS[3], f"hladiny_{lokalita}", ' Saved')
    # denné agregované data pre lokalitu
    df_lokalita_agg = df_agg[(df_agg['Stanica'] == lokalita) & (df_agg['Tok'] == tok)]
    df_lokalita_agg = df_lokalita_agg.sort_values(by='level_2')
    save_frame(df_lokalita_agg,TOPRESDIR+DATADIRS[3], 'hladiny_denne_'+lokalita.replace(' - ', '_').lower())
    print(TOPRESDIR+DATADIRS[3], f"hladiny denne_{lokalita}", ' Saved')
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


def podzemne_vody_sk():
    return   
    df = pd.read_parquet(PODZEMNE_VODY_SK_DIR + 'podzemne_vody_sk.parquet'   )
    df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET')
    save_frame(df, PODZEMNE_VODY_SK_DIR, 'podzemne_vody_sk')    
    logger.info(f"PODZEMNE_VODY_SK - {len(df)} riadkov")

def log_elapsed_time(func):
    start = dt.datetime.now()
    func()
    elapsed = dt.datetime.now() - start
    logger.info(f"{func.__name__}: Celkový čas spracovania: {elapsed}")

def main():


    # start = dt.datetime.now()
    # teploty(DATABASES[0])
    # logger.info(f"{teploty.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    # start = dt.datetime.now()
    # prietoky_sk()
    # logger.info(f"{prietoky_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    # start = dt.datetime.now()
    # hladiny_sk()
    # logger.info(f"{hladiny_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    # start = dt.datetime.now()
    # zrazky_sk(DATABASES[2])    
    # logger.info(f"{zrazky_sk.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    # start = dt.datetime.now()
    # zrazky_brezno()        
    # logger.info(f"{zrazky_brezno.__name__}: Celkový čas spracovania: {dt.datetime.now() - start}")
    # print('done')
    
    workflow = [podzemne_vody_sk, prietoky_sk, hladiny_sk, zrazky_sk, teploty]
    for func in workflow:
        log_elapsed_time(func)
    print('done')

if __name__ == "__main__":
    main()  
