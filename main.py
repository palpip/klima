import pandas as pd
import re
import openpyxl
import datetime
import sqlite3
import pyarrow
from pathlib import Path

TOPDIR = r'/home/pp/program/jupyter/Brezno - Klima/'
TEMPDIR= TOPDIR + r'temp/'
UHRNDIR = TOPDIR + r'uhrn/'
UHRNYDIR = TOPDIR + r'zrazkycelk/'
VODOMERDIR = TOPDIR + r'VodomerneStanice/'
HYDROMETERDIR = TOPDIR + r'HydrologickeSpravodajstvo/'
TEMPTESTFILE = TEMPDIR + '2025-07-30-15-00.html'
UHRNTESTFILE = UHRNDIR + '2025-07-30-23-45.html'


# Read all tables from an HTML file or string
# tables = pd.read_tblhtml('your_file.html')  # or pd.read_html(html_string)

# Access tables as DataFrames
def extract_tables_from_html(html_content, tableno = 0):
    """Extract all tables from an HTML content string.
    :param html_content: str, HTML content containing tables
    :return: list of DataFrames
    """
    tables = pd.read_html(html_content)    
    # for i, table in enumerate(tables):
    #     print(f"Table {i}:\n", table)
    
    
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

# Example usage:
# for file_path in find_files_with_extension(TOPDIR, '.html'):
#     print(file_path)

# extract_tables_from_html(TEMPTESTFILE)  
# extract_tables_from_html(UHRNTESTFILE)

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
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def teploty():
    df = pd.DataFrame()
    htmlfiles = Path(TEMPDIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = pd.read_html(file_path)
            table = tables[0]
            table.columns = table.columns.droplevel(0)
            table.Teplota = table.Teplota.str.replace(' °C','')
            table['Rýchlosť'] = table['Rýchlosť'].str.replace(' m/s','')
            table.Tlak = table.Tlak.str.replace(' hPa','')
            regex_pattern = r'sia - (.*) - (.*) LSE'
            [datum,cas] = extract_date_from_html(file_path, regex_pattern)
            table['datetime'] = datetime.datetime.strptime(f'{datum} {cas}','%d.%m.%Y %H:%M')
            df = pd.concat([df, table])
            # print(df)
        else:
            print(f"!!!Empty file!!!: {file_path}")
    df.drop_duplicates(inplace=True)
    df.sort_values(by=['datetime'], inplace=True)
    
    brezno = df[df['Stanica'] == 'Brezno']
    save_frame(df, TEMPDIR, 'teplotyx')
    save_frame(brezno, TEMPDIR, 'teploty_breznox')
    print('done')   
 
def uhrny():   
    df = pd.DataFrame()
    htmlfiles = Path(UHRNDIR).glob('*.html')
    for file_path in htmlfiles:
        print(file_path)
        table = extract_tables_from_html(file_path,1)
        df = pd.concat([df, table])
    
    df['datetime'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
    df = df.drop_duplicates(df, keep='first').sort_values(by='datetime')
    save_frame(df, UHRNDIR, 'uhrny')    
    
def uhrnycelk():   
    df = pd.DataFrame()
    htmlfiles = Path(UHRNYDIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = extract_tables_from_html(file_path,100)
            for tbl in tables:
                df = pd.concat([df, tbl])
        else:
            print(f"!!!Empty file!!!: {file_path}")
    df = df[df['Čas merania'] != 'Priemery:']
    df['datetime'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
    df = df.drop_duplicates(df, keep='first').sort_values(by='Čas merania')
    save_frame(df, UHRNYDIR, 'uhrnycelk')    
    
def vodomerne_stanice():   
    df = pd.DataFrame()
    htmlfiles = Path(VODOMERDIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = extract_tables_from_html(file_path,100)
            for tbl in tables:
                df = pd.concat([df, tbl])
        else:
            print(f"!!!Empty file!!!: {file_path}")
    df['datetime'] = pd.to_datetime(df['Čas merania'], format='%d.%m.%Y %H:%M')
    df = df.drop_duplicates(df, keep='first').sort_values(by='datetime')
    save_frame(df, VODOMERDIR, 'vodomerne_stanice')    
    

def hydrometricke_stanice():   
    df = pd.DataFrame()
    htmlfiles = Path(HYDROMETERDIR).glob('*.html')
    for file_path in htmlfiles:
        if file_path.stat().st_size > 0:
            print(file_path)
            tables = extract_tables_from_html(file_path,100)
            regex_pattern = r'(?:<.*?>)?\s?(\d{1,2}\.\d{1,2}\.\d{4}) o (\d{1,2}:\d\d)'
            [datum,cas] = extract_date_from_html(file_path, regex_pattern)
            table=tables[0]
            table['datetime'] = datetime.datetime.strptime(f'{datum} {cas}','%d.%m.%Y %H:%M')
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
    save_frame(df, HYDROMETERDIR, 'hydrometricke_stanice')

hydrometricke_stanice()
vodomerne_stanice()
uhrnycelk()    
uhrny()        
teploty()
print('done')