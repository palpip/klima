#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases processing

Modul pre spracovanie dat SHMU a databaz
Created on Mon Aug  5 10:20:00 2024
'''

import pandas as pd
import re
import datetime as dt
import pyarrow as pa
# import openpyxl
# import sqlite3
# import logging

from pathlib import Path
import logging
from sqlalchemy import create_engine
from funcs import *
from config import *

   

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
    with open(file=htmlfile, mode='r', encoding='utf-8') as file:
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
    log_inf.info(f"{dirname} - {dfname} {len(df)} riadkov saved to csv")
    
    df.to_parquet(dirname + dfname + '.parquet', engine='auto')
    log_inf.info(f"{dirname} - {dfname} {len(df)} riadkov saved to parquet")
    
    engine = create_engine(CONNSTR)
    df.to_sql(dfname, engine, if_exists='replace', index=False)
    log_inf.info(f"{dfname} {len(df)} riadkov saved to database")
   
    # df.to_excel(dirname + dfname + '.xlsx')
    # log_inf.info(f"{dirname}{dfname} {len(df)} riadkov saved to excel")

    # conn = sqlite3.connect(dirname + dfname + '.sqlite')
    # df.to_sql(dfname, conn, if_exists='replace', index=False)
    # conn.close()
    # log_inf.info(f"{dirname} - {dfname} {len(df)} riadkov saved to sqlite")
    
    
def to_num(df, cols):
    '''prevedie stlpce cols na numeric'''
    for col in cols:
        # df[col] = pd.to_numeric(df[col], errors='coerce', dtype_backend='numpy_nullable').astype('Float32')
        #memory usage 60347 vs 80255 bytes
        df[col] = pd.to_numeric(df[col], errors='coerce', dtype_backend='pyarrow' ).astype(pd.ArrowDtype(pa.float64())) 
        
    return df

def to_decimal(df, cols):
    '''prevedie stlpce cols na decimal'''
    for col in cols:
        # df[col] = df[col].astype(pd.ArrowDtype(pa.decimal128(21, 2))) # pyarrow 13
        #df[col] = df[col].astype(pd.ArrowDtype(pa.decimal64(9, 2))) # pyarrow 12, OK, ale nejde do SQLite
        df[col] = df[col].astype(pd.ArrowDtype(pa.decimal64(6, 1))) # pyarrow 12, OK, ale nejde do SQLite
    return df

def to_cat(df, cols):
    '''prevedie stlpce cols na decimal'''
    for col in cols:
        df[col] = df[col].astype('category')
    return df

def teploty():
    dates = get_date_interval(TEPLOTY_SK_DIR)
    for date in dates:
        print(f'Spracovávam teploty pre dátum: {date}')
        htmlfiles = list(Path(TEPLOTY_SK_DIR).glob(f'*{date}*.html'))
        df = pd.DataFrame()
        for file_path in htmlfiles:
            if file_path.stat().st_size > 0:
                print(file_path)
                tables = pd.read_html(file_path)
                table = tables[0]
                table.columns = table.columns.droplevel(0)
                table.Teplota = table.Teplota.str.replace(' °C','').apply(pd.to_numeric, errors='coerce').astype(float)
                table['Rýchlosť'] = table['Rýchlosť'].str.replace(' m/s','').apply(pd.to_numeric, errors='coerce').astype(float)
                table.Tlak = table.Tlak.str.replace(' hPa','').apply(pd.to_numeric, errors='coerce').astype(float)
                regex_pattern = r'sia - (.*) - (.*) (LSE|SEČ)'
                [datum,cas] = extract_date_from_html(file_path, regex_pattern)
                table['Cas_CET'] = dt.datetime.strptime(f'{datum} {cas}','%d.%m.%Y %H:%M')
                table['file'] = file_path.name.split('.')[0]
                df = pd.concat([df, table])
                # print(df)
            else:
                log_err.error(f"Súbor {file_path} je prázdny")
        # df.drop_duplicates(inplace=True)
        df =remove_duplicates(df)
        df.sort_values(by=['Cas_CET'], inplace=True)
        save_frame(df, RES_TEPLOTY_SK_DIR, f'teploty_sk_{date}')
        
        brezno = df[df['Stanica'] == 'Brezno']
        save_frame(brezno, RES_TEPLOTY_SK_DIR, f'teploty_brezno_{date}')
        log_inf.info(f"{date}-TEPLOTY_SK - {len(df)} riadkov")
        log_inf.info(f"{date}-TEPLOTY_BREZNO - {len(brezno)} riadkov")
        pack_to_zip(htmlfiles, RES_TEPLOTY_SK_DIR + f'teploty_sk_{date}.zip')
        log_inf.info(f"{date}-teploty_sk - {len(htmlfiles)} suborov zabalených do ZIP")
        if date != dates[-1]:  # neodstranuj subory pre aktualny mesiac
            remove_files_list(htmlfiles)
            log_inf.info(f"{date}-teploty_sk - {len(htmlfiles)} suborov odstránených")
        
def zrazky_brezno():   
    dates = get_date_interval(ZRAZKY_BREZNO_DIR)
    for date in dates:
        print(f'Spracovávam teploty pre dátum: {date}')
        df = pd.DataFrame()
        htmlfiles = list(Path(ZRAZKY_BREZNO_DIR).glob(f'*{date}*.html'))
        for file_path in htmlfiles:
            print(file_path)
            table = extract_tables_from_html(file_path,1)
            table['file'] = file_path.name.split('.')[0]
            df = pd.concat([df, table])
        
        df['Cas_CET'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
        # df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET')
        df = remove_duplicates(df).sort_values(by='Cas_CET')
        save_frame(df, RES_ZRAZKY_BREZNO_DIR, f'zrazky_brezno_{date}')    
        log_inf.info(f"{date}-ZRAZKY_BREZNO - {len(df)} riadkov")
        pack_to_zip(htmlfiles, RES_ZRAZKY_BREZNO_DIR + f'zrazky_brezno_{date}.zip')
        log_inf.info(f"{date}-zrazky_brezno - {len(htmlfiles)} suborov zabalených do ZIP")
        if date != dates[-1]:  # neodstranuj subory pre aktualny mesiac
            remove_files_list(htmlfiles)
            log_inf.info(f"{date}-zrazky_brezno - {len(htmlfiles)} suborov odstránených")
        
    
def zrazky_sk():   
    dates = get_date_interval(ZRAZKY_SK_DIR)
    for date in dates:
        print(f'Spracovávam teploty pre dátum: {date}')
        df = pd.DataFrame()
        htmlfiles = list(Path(ZRAZKY_SK_DIR).glob(f'*{date}*.html'))
        df = pd.DataFrame()
        for file_path in htmlfiles:
            if file_path.stat().st_size > 0:
                print(file_path)
                tables = extract_tables_from_html(file_path,100)
                for table in tables:
                    table['file'] = file_path.name.split('.')[0]
                    df = pd.concat([df, table])
            # print(df.count().max())    
            # if df.count().max() > 50000:
            #     # log_err.warning(f"Súbor {file_path} má viac ako 50 riadkov, skontrolujte správnosť údajov!")
            #     break
            else:
                log_err.error(f"Súbor {file_path} je prázdny")
        df = df[df['Čas merania'] != 'Priemery:']
        df['Cas_CET'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')

        # df = df.drop_duplicates(subset=['Stanica', 'Typ', 'Cas_CET'], keep='first').sort_values(by='Čas merania')
        df = remove_duplicates(df).sort_values(by='Cas_CET')
        
        df = to_cat(df, ['Stanica', 'Typ']) # prevedenie na category - nie je permanentne
        df = to_num(df, ['Zrážky 1h', 'Zrážky 3h', 'Zrážky 6h', 'Zrážky 12h', 'Zrážky 24h']) # prevedenie na float32
        df = df.convert_dtypes(dtype_backend='pyarrow') # prevedenie vsetkych stlpcov na pyarrow dtype
        
        save_frame(df[['Stanica', 'Typ', 'Cas_CET', 'Zrážky 1h', 'Zrážky 24h', 'file']], RES_ZRAZKY_SK_DIR, f'zrazky_sk_{date}')    
        log_inf.info(f"{date}-ZRAZKY_SK - {len(df)} riadkov")
        pack_to_zip(htmlfiles, RES_ZRAZKY_SK_DIR + f'zrazky_sk_{date}.zip')
        log_inf.info(f"{date}-ZRAZKY_SK - {len(htmlfiles)} suborov zabalených do ZIP")
        if date != dates[-1]:  # neodstranuj subory pre aktualny mesiac
            remove_files_list(htmlfiles)
            log_inf.info(f"{date}-ZRAZKY_SK - {len(htmlfiles)} suborov odstránených")
        
def hladiny_sk():
    dates = get_date_interval(HLADINY_SK_DIR)
    for date in dates:
        print(f'Spracovávam teploty pre dátum: {date}')
        df = pd.DataFrame()
        htmlfiles = list(Path(HLADINY_SK_DIR).glob(f'*{date}*.html'))
        for file_path in htmlfiles:
            if file_path.stat().st_size > 0:
                print(file_path)
                tables = extract_tables_from_html(file_path,100)
                for table in tables:
                    table['file'] = file_path.name.split('.')[0]
                    df = pd.concat([df, table])
            else:
                log_err.error(f"Súbor {file_path} je prázdny")
        df['Cas_CET'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
        df = df.rename(columns={'Unnamed: 0': 'Typ'}) # premenovanie stlpca
        # df = df.drop_duplicates(subset=['Stanica', 'Tok', 'Cas_CET'], keep='first').sort_values(by='Cas_CET')
        df = remove_duplicates(df).sort_values(by='Cas_CET')
        df = to_cat(df, ['Stanica', 'Tok', 'Typ']) # prevedenie na category - nie je permanentne
        df = df.convert_dtypes(dtype_backend='pyarrow') # prevedenie vsetkych stlpcov na pyarrow dtype, cisla su v integer
        save_frame(df[['Stanica', 'Tok', 'Cas_CET', 'Vodný stav', 'file']], RES_HLADINY_SK_DIR, f'hladiny_sk_{date}')    
        log_inf.info(f"{date}-HLADINY_SK - {len(df)} riadkov")
        pack_to_zip(htmlfiles, RES_HLADINY_SK_DIR + f'hladiny_sk_{date}.zip')
        log_inf.info(f"{date}-HLADINY_SK - {len(htmlfiles)} suborov zabalených do ZIP")
        if date != dates[-1]:  # neodstranuj subory pre aktualny mesiac
            remove_files_list(htmlfiles)
            log_inf.info(f"{date}-HLADINY_SK - {len(htmlfiles)} suborov odstránených")
        
def podzemne_vody_sk():   
    dates = get_date_interval(PODZEMNE_VODY_SK_DIR)
    for date in dates:
        print(f'Spracovávam teploty pre dátum: {date}')
        htmlfiles = list(Path(PODZEMNE_VODY_SK_DIR).glob(f'*{date}*.html'))
        df_vrt = pd.DataFrame()
        df_prm = pd.DataFrame()
        for file_path in htmlfiles:
            if file_path.stat().st_size > 0:
                print(file_path)
                filename = file_path.name.split('.')[0]
                [tablea,tableb] = extract_tables_from_html(file_path,100) # dve tabulky
                tablea['file'] = filename
                tableb['file'] = filename
                df_vrt = pd.concat([df_vrt, tablea])
                df_prm = pd.concat([df_prm, tableb])
            else:
                log_err.error(f"Súbor {file_path} je prázdny")
        df_vrtcols ={'Číslo stanice' : 'Stanica', 'Názov lokality' : 'Nazov_lok', 'Hĺbka vrtu [m]' : 'Hlbka_vrtu', 'Nadmorská výška terénu [m]' : 'vyska_terenu',
                'Dátum a čas merania' : 'Cas', 'Úroveň podzemnej vody [m n.m.]' : 'uroven_PV'}
        df_prmcols ={'Číslo stanice' : 'Stanica', 'Názov lokality' : 'Nazov_lok', 'Názov prameňa' : 'Nazov_prm', 'Nadmorská výška objektu [m]' : 'vyska_objektu',
                'Dátum a čas merania' : 'Cas', 'Výdatnosť prameňa [l.s-1]' : 'vydatnost'}
        df_vrt = df_vrt.rename(columns=df_vrtcols) # premenovanie stlpca
        df_vrt['Cas_CET'] = pd.to_datetime(df_vrt['Cas'], format='%d.%m.%Y %H:%M')
        # df_vrt = df_vrt.drop_duplicates(subset=['Stanica', 'Nazov_lok', 'Povodie', 'Cas_CET'], keep='first').sort_values(by='Cas_CET')
        df_vrt = remove_duplicates(df_vrt).sort_values(by='Cas_CET')
        df_vrt = to_cat(df_vrt, ['Stanica', 'Povodie', 'Nazov_lok']) # prevedenie na category - nie je permanentne
        df_vrt = to_num(df_vrt, ['vyska_terenu', 'Hlbka_vrtu']) # prevedenie na numeric
        
        df_vrt = df_vrt.convert_dtypes(dtype_backend='pyarrow') # prevedenie vsetkych stlpcov na pyarrow dtype, cisla su v integer
        df_vrt = df_vrt.sort_values(by=['Stanica', 'Nazov_lok', 'Cas_CET'])
        
        df_prm = df_prm.rename(columns=df_prmcols) # premenovanie stlpca
        df_prm['Cas_CET'] = pd.to_datetime(df_prm['Cas'], format='%d.%m.%Y %H:%M')
        # df_prm = df_prm.drop_duplicates(subset=['Stanica', 'Nazov_lok', 'Povodie', 'Cas_CET'], keep='first').sort_values(by='Cas_CET')
        df_prm = remove_duplicates(df_prm).sort_values(by='Cas_CET')    
        df_prm = to_cat(df_prm, ['Stanica', 'Povodie', 'Nazov_lok']) # prevedenie na category - nie je permanentne
        df_prm = to_num(df_prm, ['vyska_objektu', 'vydatnost']) # prevedenie na category - nie je permanentne
        df_prm = df_prm.convert_dtypes(dtype_backend='pyarrow') # prevedenie vsetkych stlpcov na pyarrow dtype, cisla su v integer
        df_prm = df_prm.sort_values(by=['Stanica', 'Nazov_prm', 'Cas_CET'])
        
 
        save_frame(df_vrt, RES_PODZEMNE_VODY_VRT_SK_DIR, f'PV_vrt_sk_{date}')    
        save_frame(df_prm, RES_PODZEMNE_VODY_PRM_SK_DIR, f'PV_prm_sk_{date}')    
        
        log_inf.info(f"{date}-PODZEMNE_VODY_VRT_SK - df_vrt {len(df_vrt)} riadkov")
        log_inf.info(f"{date}-PODZEMNE_VODY_PRM_SK - df_prm {len(df_prm)} riadkov")
        pack_to_zip(htmlfiles, RES_PODZEMNE_VODY_SK_DIR + f'podzemne_vody_sk_{date}.zip')
        log_inf.info(f"{date}-PODZEMNE_VODY_SK - {len(htmlfiles)} suborov zabalených do ZIP")
        if date != dates[-1]:  # neodstranuj subory pre aktualny mesiac
            remove_files_list(htmlfiles)
            log_inf.info(f"{date}-PODZEMNE_VODY_SK - {len(htmlfiles)} suborov odstránených")
    

def prietoky_sk():
    dates = get_date_interval(PRIETOKY_SK_DIR)
    for date in dates:
        print(f'Spracovávam teploty pre dátum: {date}')
        htmlfiles = list(Path(PRIETOKY_SK_DIR).glob(f'*{date}*.html'))
        df = pd.DataFrame()
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
                log_err.error(f"Súbor {file_path} je prázdny")
        #cistenie dat
        df.columns = df.columns.droplevel(1) # odstranenie viacriadkovych hlaviciek 
        df = df.rename(columns={'∆H': 'dH', 'QM,N' : 'QMN'}) # premenovanie stlpca
        df.Z = df.Z.replace('//', 0)    # nahradenie hodnoty  '//' na 0, OVERENE
        # df = df.drop_duplicates(df, keep='first').sort_values(by='Cas_CET') # odstranenie duplicit a zoradenie podla casu
        df = remove_duplicates(df).sort_values(by='Cas_CET')
        df = to_num(df, ['H','L','dH','Q','Tvo','Tvz','Z','QMN', 'PA']) # prevedenie na float32
        df.L = df.L.astype('Int16') # prevedenie na Int16
        df = to_cat(df, ['Stanica - tok','P']) # prevedenie na category - nie je permanentne 
        df = df.convert_dtypes(dtype_backend='pyarrow') # prevedenie vsetkych stlpcov na pyarrow dtype
        # ulozenie spracovanych dat a vymazanie zdrojovych suborov - archivacia. POZOR aktualny mesiac sa nevymazava
        save_frame(df, RES_PRIETOKY_SK_DIR, f'prietoky_sk_{date}')
        log_inf.info(f"{date}-PRIETOKY_SK - {len(df)} riadkov")
        pack_to_zip(htmlfiles, RES_PRIETOKY_SK_DIR + f'prietoky_sk_{date}.zip')
        log_inf.info(f"{date}-PRIETOKY_SK - {len(htmlfiles)} suborov zabalených do ZIP")
        if date != dates[-1]:  # neodstranuj subory pre aktualny mesiac
            remove_files_list(htmlfiles)
            log_inf.info(f"{date}-PRIETOKY_SK - {len(htmlfiles)} suborov odstránených")
        

def remove_files_list(to_delete):
    '''odstrani zoznam suborov v zozname to_delete'''
    import os
    for file_path in to_delete:
        try:
            pass
            # os.remove(file_path)
            # log_inf.info(f'Odstránený súbor: {file_path}')
        except Exception as e:
            log_err.error(f'Chyba pri odstraňovaní súboru {file_path}: {e}')

def get_date_interval(datadir=TEPLOTY_SK_DIR):
    '''zisti interval dat v adresaroch na zaklade nazvov html suborov
    vrati zoznam datumov v poradi   '''
    htmlfiles = list(Path(datadir).glob('*.html'))
    if len(htmlfiles) > 0:
        dates = set()
        for file_path in htmlfiles:
            filename = file_path.name.split('.')[0]
            date_str = filename[-16:-9]  # predpokladame format 'YYYY-MM-DD-HH-MM'
            if date_str == '':
                continue    
            #dates.append(date_str)
            # date_obj = dt.datetime.strptime(date_str, '%Y-%m-%d-%H-%M')
            # dates.append(date_obj)
            dates.add(date_str)
        min_date = min(dates)
        max_date = max(dates)
        log_inf.info(f"Adresár {datadir} obsahuje dáta od {min_date} do {max_date}, počet súborov: {len(htmlfiles)}")
        return sorted(dates)
    else:
        log_err.warning(f"Adresár {datadir} neobsahuje žiadne HTML súbory.")

def pack_to_zip(filelist, zipfilename):
    '''zabali subory filelist do zip suboru zipfilename'''
    import zipfile
    with zipfile.ZipFile(zipfilename, 'w') as zipf:
        for file in filelist:
            zipf.write(file, arcname=Path(file).name)
    log_inf.info(f'Vytvorený ZIP súbor: {zipfilename}')

def log_elapsed_time(func):
    start = dt.datetime.now()
    log_inf.info(f"{func.__name__}: Za4iatok: {start}")
    func()
    elapsed = dt.datetime.now() - start
    log_inf.info(f"{func.__name__}: Celkový čas spracovania: {elapsed}")

log_err = create_logger('err-1')
log_inf = create_logger('inf-1')

def main():
    workflow = [podzemne_vody_sk, prietoky_sk, hladiny_sk, zrazky_brezno, teploty] # zrazky_sk,
    workflow = [prietoky_sk]
    workflow = [zrazky_sk]
    workflow = [zrazky_sk, zrazky_brezno, teploty]   
    workflow = [podzemne_vody_sk, prietoky_sk, hladiny_sk]
    workflow = [teploty]
    workflow = [prietoky_sk]
    workflow = [podzemne_vody_sk,prietoky_sk, hladiny_sk, zrazky_sk, zrazky_brezno, teploty]   
    # workflow = [podzemne_vody_sk]
    # workflow = [zrazky_sk, zrazky_brezno, teploty]
    # workflow = [prietoky_sk]
     
    for func in workflow:
        log_elapsed_time(func)
    print('done')

def main1():
  print ()
if __name__ == "__main__":
    main()  
