import pandas as pd
import pandas as pd
import re
import openpyxl
import datetime
import sqlite3
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
TOPDIR = r'/home/pp/program/jupyter/Brezno - Klima/'
TEMPDIR= TOPDIR + r'temp/'

# Replace 'your_file.parquet' with the path to your Parquet file
df = pd.read_parquet(TEMPDIR + 'teplotyx.parquet'   )

print(df.head())