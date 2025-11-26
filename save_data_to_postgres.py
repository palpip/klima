#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Module for SHMU data and databases processing
modul nacitava hotove databazy a uklada ich do postgresql
ziadnu inu funkcionalitu nema'''

import pandas as pd
import pyarrow as pa
import datetime as dt
# import re
# import openpyxl
# import sqlite3
# import logging

from pathlib import Path
from sqlalchemy import create_engine
from config import *
from funcs import *


def save_frame_postgres(df, dfname):
    '''ulozi dataframes do postgresql databazy'''
    engine = create_engine('postgresql://pp:ppp@192.168.1.88:5432/shmu')
    df.to_sql(dfname, engine, if_exists='replace', index=False)

def main():
    

    workflow = [RES_TEPLOTY_SK_DIR, RES_ZRAZKY_BREZNO_DIR, RES_ZRAZKY_SK_DIR, 
                RES_HLADINY_SK_DIR, RES_PRIETOKY_SK_DIR, RES_PODZEMNE_VODY_SK_DIR]
    for dir in workflow:
        df = get_data_from_parquets(dir)
        logger_inf.info(f'Načítané dáta z {dir}: {df.shape[0]} riadkov, {df.shape[1]} stĺpcov')
        save_frame_postgres(df, Path(dir).name)
        logger_inf.info(f'Uložené dáta do PostgreSQL tabuľky: {Path(dir).name}')
        print(f'Uložené dáta do PostgreSQL tabuľky: {Path(dir).name}')
    
    df0 = get_data_from_parquets(RES_HLADINY_SK_DIR)
    df1 = get_data_from_parquets(RES_HLADINY_SK_DIR, filter_query="Stanica == 'Brezno' & Tok == 'Hron'")
    print (
        df0.head(5),
        df0.count(),   
        df1.head(5),
        df1.count())

    print('done')

if __name__ == "__main__":
    main()  
