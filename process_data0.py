#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases processing
Modul pre spracovanie dat SHMU a databaz
Modul je druhým krokom, načítava hotové databázy, spracúva denné
dáta/sumáre a oddelí Breznianske dáta
Workflow obsahuje cesty pre teploty, zrážky, hladiny, prietoky a podzemné vody
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
from sqlalchemy import create_engine

pd.DataFrame().to_excel(TOPRESDIR + 'vystupxx.xlsx', sheet_name = 'info', index=False)  # vytvorenie prazdneho xlsx suboru
# with pd.ExcelWriter(TOPDIR + 'vystup.xlsx', mode = 'w', engine='openpyxl') as EXCELWRITER:
#     pd.DataFrame.to_excel(EXCELWRITER, sheet_name='info', index=False)

def read_parquets(directory, filter_query=None):
    """
    Read and concatenate parquet files from a directory.

    Parameters
    ----------
    directory : str | pathlib.Path
        Path to the directory containing .parquet files. The function lists files
        matching the pattern '*.parquet' (non-recursively) and sorts them before reading.
    filter_query : str, optional
        A pandas.DataFrame.query()-style boolean expression to apply to each DataFrame
        after reading. If None, no additional query is applied.

    Returns
    -------
    pandas.DataFrame
        A single DataFrame formed by concatenating all read parquet files (in sorted
        order) after excluding rows where the 'Stanica' column equals 'rezno' and
        applying the optional filter_query. The index is reset in the concatenated result.

    Raises
    ------
    ValueError
        If no parquet files are found, concatenation (pd.concat) may raise a ValueError.
    Errors from pandas.read_parquet or from DataFrame.query will propagate to the caller.

    Notes
    -----
    - The function prints the sorted list of parquet files it will read.
    - The 'Stanica' == 'rezno' exclusion is applied to each file before concatenation.
    - The filter_query, if provided, must be a valid expression for pandas.DataFrame.query().
    - Reading/parsing errors and file I/O errors are not caught inside the function.

    Examples
    --------
    >>> df = read_parquets("/path/to/parquets")
    >>> df = read_parquets("/path/to/parquets", "temperature > 0 and region == 'north'")
    """
    def list_parquet_files(directory):
        '''List all parquet files in the given directory'''
        p = Path(directory)
        return sorted([str(f) for f in p.glob('*.parquet')])
    files = list_parquet_files(directory)
    print(files)
    # Read and filter each file, then concatenate
    if filter_query is None:
        dfs = [pd.read_parquet(f) for f in files]  
    else:   
        dfs = [pd.read_parquet(f).query(filter_query) for f in files]   
    
    retval =  pd.concat(dfs, ignore_index=True)
    retval = remove_duplicates(retval)
    return retval

def save_frame(df, dirname, dfname):
    '''ulozi dataframes v adresari dirname vo formate
    dfname.csv, dfname.xlsx, dfname.sqlite3'''
    
    print('-----', dirname + dfname + '.xlsx')
    with pd.ExcelWriter(TOPRESDIR + 'vystupxx.xlsx', mode = 'a', engine='openpyxl', if_sheet_exists='replace') as EXCELWRITER:
        df.to_excel(EXCELWRITER, sheet_name=dfname, index=False)
    
    # ciastkove ulozenie do sqlite3 - nepouzivane
    # conn = sqlite3.connect(dirname + dfname + '.sqlite')
    # df.to_sql(dfname, conn, if_exists='replace', index=False)
    
    # ciastkove ulozenie do postgresql - nepouzivane
    # engine = create_engine(CONNSTR)
    # df.to_sql(dfname, engine, if_exists='replace', index=False)
    
    # ciastkove ulozenie do parquet - nepouzivane
    # df.to_parquet(TOPRESDIR + dfname + '.parquet', engine='auto') 



def teploty(infile=RES_TEPLOTY_SK_DIR, lokalita='Brezno'):
    ''' teploty denné sumáre pre cele SK a lokalita '''
    #read parquet file
    df = read_parquets(infile) # + '.parquet')
    print('starting teploty')
    print(df.info())
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.set_index('Cas_CET').groupby('Stanica').resample('D').Teplota.agg(['min', 'max', 'mean'])
    df_agg.reset_index(inplace=True)
    df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    df_agg.sort_values(by=['Stanica','Cas_CET'], inplace=True)
    
    # filter lokalita
    # surove data pre lokalitu 
    df_lokalita_raw = df[df['Stanica'] == lokalita]
    save_frame(df_lokalita_raw,RES_TEPLOTY_SK_DIR, 'teploty_'+ lokalita.replace(' - ', '_').lower())
    print(RES_TEPLOTY_SK_DIR, f"teploty_{lokalita}", ' Saved')
    
    df_lokalita_agg = df_agg[df_agg['Stanica'] == lokalita]
    save_frame(df_lokalita_agg,RES_TEPLOTY_SK_DIR, 'teploty_denne_'+lokalita.replace(' - ', '_').lower())
    print(RES_TEPLOTY_SK_DIR, f"teploty_denne_{lokalita}", ' Saved')
        
def zrazky_sk(infile=RES_ZRAZKY_SK_DIR, lokalita = 'Brezno'):
    df = read_parquets(infile) # + '.parquet'   )
    # make daily averages
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.groupby(['Stanica', pd.Grouper(key='Cas_CET', freq='D')], observed=True)['Zrážky 1h'].agg(['sum', 'count']).reset_index()
    
    # save whole SK
    save_frame(df_agg,RES_ZRAZKY_SK_DIR, 'zrazky_denne_sk')
       
    # filter lokalita
    # surove data pre lokalitu 
    df_lokalita_raw = df[df['Stanica'] == lokalita]
    df_lokalita_raw = df_lokalita_raw.sort_values(by='Cas_CET')
    save_frame(df_lokalita_raw, RES_ZRAZKY_SK_DIR, 'zrazky_'+ lokalita.replace(' - ', '_').lower())
    print(RES_ZRAZKY_SK_DIR, f"zrazky_{lokalita}", ' Saved')
    
    df_lokalita_agg = df_agg[df_agg['Stanica'] == lokalita]
    # df_lokalita_agg = df_lokalita_agg.sort_values(by='Cas_CET')
    save_frame(df_lokalita_agg,RES_ZRAZKY_SK_DIR, 'zrazky_denne_'+lokalita.replace(' - ', '_').lower())
    print(RES_ZRAZKY_SK_DIR, f"zrazky_denne_{lokalita}", ' Saved')
    
    # save_frame(df, ZRAZKY_SK_DIR, 'zrazky_sk')    
    logger.info(f"ZRAZKY_SK - {len(df)} riadkov")

def hladiny_sk(infile=RES_HLADINY_SK_DIR, lokalita='Brezno', tok='Hron'):   
    df = read_parquets(infile) # + '.parquet'   )
    # df = df.drop_duplicates(keep='first').sort_values(by='Cas_CET')
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.groupby(['Stanica','Tok', pd.Grouper(key='Cas_CET', freq='D')], observed=True)['Vodný stav'].agg(['min', 'max', 'mean']).reset_index()
    # df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    # df_agg.sort_values(by=['Stanica', 'Tok', 'Cas_CET'], inplace=True)  
    
    # save whole SK
    save_frame(df_agg, HLADINY_SK_DIR, 'hladiny_sk')    
    
    # filter lokalita
    # surove data pre lokalitu
    df_lokalita_raw = df[(df['Stanica'] == lokalita) & (df['Tok'] == tok)]
    df_lokalita_raw = df_lokalita_raw.sort_values(by='Cas_CET')
    save_frame(df_lokalita_raw,RES_HLADINY_SK_DIR, 'hladiny_'+ lokalita.replace(' - ', '_').lower())
    print(RES_HLADINY_SK_DIR, f"hladiny_{lokalita}", ' Saved')
    # denné agregované data pre lokalitu
    df_lokalita_agg = df_agg[(df_agg['Stanica'] == lokalita) & (df_agg['Tok'] == tok)]
    df_lokalita_agg = df_lokalita_agg.sort_values(by='level_2')
    save_frame(df_lokalita_agg, RES_HLADINY_SK_DIR, 'hladiny_denne_'+lokalita.replace(' - ', '_').lower())
    print(RES_HLADINY_SK_DIR, f"hladiny denne_{lokalita}", ' Saved')
    # filter by statio n and tok    
    logger.info(f"HLADINY_SK - {len(df)} riadkov")  
    


def prietoky_sk(infile=RES_PRIETOKY_SK_DIR, lokalita='Brezno - Hron'):   
    df = read_parquets(infile) # + '.parquet'   )
    # df = df.drop_duplicates(keep='first').sort_values(by='Cas_CET')
    dfb = df[df['Stanica - tok'] == lokalita]
    dfb.loc[:,['Cas_CET']] = pd.to_datetime(dfb['Cas_CET'].dt.date, errors='coerce')
    dfb = dfb.sort_values(by='Cas_CET')    
    save_frame(dfb, PRIETOKY_SK_DIR, 'prietoky_'+ lokalita.replace(' - ', '_').lower())    
    logger.info(f"PRIETOKY_BREZNO - {len(dfb)} riadkov")
    # save_frame(df, PRIETOKY_SK_DIR, 'prietoky_sk')
    # logger.info(f"PRIETOKY_SK - {len(df)} riadkov")


def podzemne_vody_prm_sk(infile=RES_PODZEMNE_VODY_PRM_SK_DIR):
    df = read_parquets(infile) # + '.parquet')
    # print(df.count())
    # df = df.drop_duplicates(keep='first').sort_values(by='Cas_CET')
    # print(df.count())
    # df = df.drop_duplicates(subset=['Cas_CET', 'Stanica']).sort_values(by='Cas_CET')
    # print(df.count())
    
    save_frame(df, RES_PODZEMNE_VODY_PRM_SK_DIR, 'podzemne_vody_prm_sk')    
    logger.info(f"PODZEMNE_VODY_PRM_SK - {len(df)} riadkov")
    return   

def podzemne_vody_vrt_sk(infile=RES_PODZEMNE_VODY_VRT_SK_DIR):
    df = read_parquets(infile) # + '.parquet')
    # df = df.drop_duplicates(keep='first').sort_values(by='Cas_CET')
    save_frame(df, RES_PODZEMNE_VODY_VRT_SK_DIR, 'podzemne_vody_vrt_sk')    
    logger.info(f"PODZEMNE_VODY_VRT_SK - {len(df)} riadkov")
    return   

def log_elapsed_time(func):
    start = dt.datetime.now()
    func()
    elapsed = dt.datetime.now() - start
    logger.info(f"{func.__name__}: Celkový čas spracovania: {elapsed}")


def main():
    
    workflow = [podzemne_vody_prm_sk, podzemne_vody_vrt_sk, prietoky_sk, hladiny_sk, zrazky_sk, teploty]
    #workflow = [zrazky_sk, teploty]
    workflow = [prietoky_sk]
    for func in workflow:
        log_elapsed_time(func)
    print('done')

if __name__ == "__main__":
    main()  
