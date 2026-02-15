#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases processing
Modul pre spracovanie dat SHMU a databaz
Modul je druhým krokom, načítava hotové databázy, spracúva denné
dáta/sumáre a oddelí Breznianske dáta
Workflow obsahuje cesty pre teploty, zrážky, hladiny, prietoky a podzemné vody
'''

from venv import logger
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

XLSNAME = 'vystup - ' + dt.datetime.now().strftime('%Y%m%d') + '.xlsx'

pd.DataFrame().to_excel(TOPRESDIR + XLSNAME, sheet_name = 'info', index=False)  # vytvorenie prazdneho xlsx suboru
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

    Returns     pandas.DataFrame
    
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
    retval = remove_duplicates(retval, logger=log_inf) # odstrani duplikaty a loguje pocet zaznamov a duplikatov
    return retval

def save_frame(df, dirname, dfname):
    '''ulozi dataframes v adresari dirname vo formate
    dfname.csv, dfname.xlsx, dfname.sqlite3'''
    
    print(f'ukladám {dfname}')
    if len(df) < 1.048e6:
        with pd.ExcelWriter(TOPRESDIR + XLSNAME, mode = 'a', engine='openpyxl', if_sheet_exists='replace') as EXCELWRITER:
            df.to_excel(EXCELWRITER, sheet_name=dfname, index=False)
            log_inf.info(f"{dirname} - {dfname} {len(df)} riadkov uložených do excelu")
    else:
        log_err.error(f'{dfname} obsahuje {len(df)} riadkov, viac ako je povolený limit excelu, nie je uložené')
        
    # ciastkove ulozenie do sqlite3 - nepouzivane
    # conn = sqlite3.connect(dirname + dfname + '.sqlite')
    # df.to_sql(dfname, conn, if_exists='replace', index=False)
    
    # ciastkove ulozenie do postgresql - nepouzivane
    engine = create_engine(CONNSTR)
    df.to_sql('kompl-'+ dfname, engine, if_exists='replace', index=False)
    log_inf.info(f"{dirname} - {dfname} {len(df)} riadkov uložených do postgresu")
    
    # ciastkove ulozenie do parquet - nepouzivane
    # df.to_parquet(TOPRESDIR + dfname + '.parquet', engine='auto') 



def teploty(infile=RES_TEPLOTY_SK_DIR, lokalita=None, **args):
    ''' teploty denné sumáre pre cele SK a lokalita '''
    #read parquet file
    df = read_parquets(infile) # + '.parquet')
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
    log_inf.info(f"TEPLOTY_SK - {lokalita} - {len(df_lokalita_raw)} riadkov")
    df_lokalita_agg = df_agg[df_agg['Stanica'] == lokalita]
    save_frame(df_lokalita_agg,RES_TEPLOTY_SK_DIR, 'teploty_denne_'+lokalita.replace(' - ', '_').lower())
    log_inf.info(f"TEPLOTY_SK_DENNE -{lokalita} - {len(df_lokalita_agg)} riadkov")
    
def zrazky_sk(infile=RES_ZRAZKY_SK_DIR, lokalita = 'Brezno'):
    df = read_parquets(infile) # + '.parquet'   )
    # make daily averages
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.groupby(['Stanica', pd.Grouper(key='Cas_CET', freq='D')], observed=True)['Zrážky 1h'].agg(['sum', 'count']).reset_index()
    
    # save whole SK
    save_frame(df_agg,RES_ZRAZKY_SK_DIR, 'zrazky_denne_sk')
    log_inf.info(f"ZRAZKY_DENNE_SK - {len(df_agg)} riadkov")   
    # filter lokalita
    # surove data pre lokalitu 
    df_lokalita_raw = df[df['Stanica'] == lokalita]
    df_lokalita_raw = df_lokalita_raw.sort_values(by='Cas_CET')
    save_frame(df_lokalita_raw, RES_ZRAZKY_SK_DIR, 'zrazky_'+ lokalita.replace(' - ', '_').lower())
    log_inf.info(f"ZRAZKY_SK - {len(df_lokalita_raw)} riadkov")
    
    df_lokalita_agg = df_agg[df_agg['Stanica'] == lokalita]
    # df_lokalita_agg = df_lokalita_agg.sort_values(by='Cas_CET')
    save_frame(df_lokalita_agg,RES_ZRAZKY_SK_DIR, 'zrazky_denne_'+lokalita.replace(' - ', '_').lower())
    # save_frame(df, ZRAZKY_SK_DIR, 'zrazky_sk')    
    log_inf.info(f"ZRAZKY_SK - {len(df_lokalita_agg)} riadkov")

def hladiny_sk(infile=RES_HLADINY_SK_DIR, lokalita=None, **args):
    ''' hladiny denné sumáre pre cele SK a lokalita - momentálne bez denného resamplovania, len agregácia na denné sumáre, bez denného resamplovania'''   
    df = read_parquets(infile) # + '.parquet'   )
    # df = df.drop_duplicates(keep='first').sort_values(by='Cas_CET')
    # grouped/aggregated by station
    df_agg = df
    df_agg = df_agg.groupby(['Stanica','Tok', pd.Grouper(key='Cas_CET', freq='D')], observed=True)['Vodný stav'].agg(['min', 'max', 'mean']).reset_index()
    # df_agg['Cas_CET'] = df_agg['Cas_CET'].dt.strftime('%Y-%m-%d')
    # df_agg.sort_values(by=['Stanica', 'Tok', 'Cas_CET'], inplace=True)  
    
    # save whole SK
    save_frame(df, HLADINY_SK_DIR, 'hladiny_sk')    
    save_frame(df_agg, HLADINY_SK_DIR, 'hladiny_sk_denne')    
    
    # filter lokalita
    # surove data pre lokalitu
    tok = args.get('Tok', None)
    df_lokalita_raw = df[(df['Stanica'] == lokalita) & (df['Tok'] == tok)]
    df_lokalita_raw = df_lokalita_raw.sort_values(by='Cas_CET')
    save_frame(df_lokalita_raw,RES_HLADINY_SK_DIR, 'hladiny_'+ lokalita.replace(' - ', '_').lower())
    # denné agregované data pre lokalitu
    df_lokalita_agg = df_agg[(df_agg['Stanica'] == lokalita) & (df_agg['Tok'] == tok)]
    df_lokalita_agg = df_lokalita_agg.sort_values(by='level_2')
    save_frame(df_lokalita_agg, RES_HLADINY_SK_DIR, 'hladiny_denne_'+lokalita.replace(' - ', '_').lower())
    log_inf.info(f"HLADINY_SK - {len(df)} riadkov")  
    


def prietoky_sk(infile=RES_PRIETOKY_SK_DIR, lokalita=None, **args):
    '''V tomto stave ukladá surové dáta pre lokalitu, bez agregácie, bez denného resamplovania
    prietoky pre celé SK sa neuložia do excelu, lebo sú príliš veľké'''   
    df = read_parquets(infile) # + '.parquet'   )
    # df = df.drop_duplicates(keep='first').sort_values(by='Cas_CET')
    dfb = df[df['Stanica - tok'] == lokalita]
    dfb.loc[:,['Cas_CET']] = pd.to_datetime(dfb['Cas_CET'].dt.date, errors='coerce')
    dfb = dfb.sort_values(by='Cas_CET')    
    save_frame(df, PRIETOKY_SK_DIR, 'prietoky_sk')
    log_inf.info(f"PRIETOKY_SK - {len(df)} riadkov")
    save_frame(dfb, PRIETOKY_SK_DIR, 'prietoky_'+ lokalita.replace(' - ', '_').lower())    
    log_inf.info(f"PRIETOKY_BREZNO - {len(dfb)} riadkov")
    

def podzemne_vody_prm_sk(infile=RES_PODZEMNE_VODY_PRM_SK_DIR, lokalita=None, **args):
    df = read_parquets(infile) # + '.parquet')
    save_frame(df, RES_PODZEMNE_VODY_PRM_SK_DIR, 'podzemne_vody_prm_sk')    
    log_inf.info(f"PODZEMNE_VODY_PRM_SK - {len(df)} riadkov")
    return   

def podzemne_vody_vrt_sk(infile=RES_PODZEMNE_VODY_VRT_SK_DIR, lokalita=None, **args):
    df = read_parquets(infile) # + '.parquet')
    save_frame(df, RES_PODZEMNE_VODY_VRT_SK_DIR, 'podzemne_vody_vrt_sk')    
    log_inf.info(f"PODZEMNE_VODY_VRT_SK - {len(df)} riadkov")
    return   


log_err = create_logger('err-2')
log_inf = create_logger('inf-2')

def run_func(wkflow):
    for item in wkflow:
            start = dt.datetime.now()
            func = eval(item.pop('func'))
            logger.info(f"{func.__name__}: Začiatok spracovania:{start}") if logger else None
            func(infile=item.pop('infile'), lokalita=item.pop('lokalita'), **item)
            elapsed = dt.datetime.now() - start
            logger.info(f"{func.__name__}: Celkový čas spracovania: {elapsed}") if logger else None




def main():
    print(workflow)
    run_func(workflow)
    print('dokončené')
    exit()

if __name__ == "__main__":
    main()  
