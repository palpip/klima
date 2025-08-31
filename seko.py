import pandas as pd
import re
from pathlib import Path

TOPDIR = f'/home/pp/program/jupyter/Brezno - Klima/'
TEMPDIR= f'/home/pp/program/jupyter/Brezno - Klima/temp/'
UHRNDIR = f'/home/pp/program/jupyter/Brezno - Klima/uhrn/'
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
    
    tbl = tables[tableno]
    if tableno == 0:
        tbl.columns = tbl.columns.droplevel(0)
    return tbl


def extract_date_from_html(htmlfile):
    """Extract date from HTML content.
    :param html_content: str, HTML content containing date information
    :return: str, extracted date
    """
    with open(htmlfile, 'r') as file:
        retval = re.findall('sia - (.*) LSE', file.read())[0].split(' - ')
        print(retval)
    return retval 
    
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

def main():
    retval = pd.DataFrame()
    for file_path in find_files_with_extension(TEMPDIR, '.html'):
        print(file_path)
        table = extract_tables_from_html(file_path,0)
        [datum,cas] = extract_date_from_html(file_path)
        table['Datum'] = datum
        table['Čas'] = cas        
        retval = pd.concat([retval, table])
        print(retval)
    retval.drop_duplicates(inplace=True)
    retval.sort_values(by=['Datum', 'Čas'], inplace=True)
    brezno = retval[retval['Stanica'] == 'Brezno']
    retval.to_csv (TEMPDIR + 'teploty.csv', index=False)
    brezno.to_csv (TEMPDIR + 'teploty_brezno.csv', index=False)    
    for i, row in retval.iterrows():
            print(f"Row {i}: {row.to_dict()}")
    
    retval = pd.DataFrame()
    for file_path in find_files_with_extension(UHRNDIR, '.html'):
        print(file_path)
        table = extract_tables_from_html(file_path,1)
        retval = pd.concat([retval, table])
    print(retval)
    retval = retval.drop_duplicates(retval, keep='first').sort_values(by='Čas merania') 
    retval.to_csv(UHRNDIR + 'uhrn.csv', index=False)    
    for i, row in retval.iterrows():
            print(f"Row {i}: {row.to_dict()}")
    
    
        
main()