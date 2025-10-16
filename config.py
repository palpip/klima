'''Configuration file for SHMU data processing'''

SHMUDIR =f'/home/pp/program/jupyter/SHMU/' 
TOPDIR = SHMUDIR + r'zber/'
TEPLOTY_SK_DIR = TOPDIR + r'teploty_sk/'
ZRAZKY_BREZNO_DIR = TOPDIR + r'zrazky_brezno/'
ZRAZKY_SK_DIR = TOPDIR + r'zrazky_sk/'
HLADINY_SK_DIR = TOPDIR + r'hladiny_sk/'
PRIETOKY_SK_DIR = TOPDIR + r'prietoky_sk/'
PODZEMNE_VODY_SK_DIR = TOPDIR + r'podzemne_vody_sk/'

TEMPTESTFILE = TEPLOTY_SK_DIR + '2025-07-30-15-00.html'
UHRNTESTFILE = ZRAZKY_BREZNO_DIR + '2025-07-30-23-45.html'

TOPDATADIR = SHMUDIR + r'data/'
DATADIRS = ['teploty_sk/', 'zrazky_brezno/', 'zrazky_sk/', 'hladiny_sk/','prietoky_sk/']
DATABASES = [
    TEPLOTY_SK_DIR+'teploty_sk',
    ZRAZKY_BREZNO_DIR + 'zrazky_brezno',
    ZRAZKY_SK_DIR + 'zrazky_sk',
    HLADINY_SK_DIR + 'hladiny_sk',
    PRIETOKY_SK_DIR +'prietoky_sk',
    PODZEMNE_VODY_SK_DIR + 'podzemne_vody_sk']
# DBFILES = [db+'.parquet' for db in DATABASES]
        

