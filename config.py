'''Configuration file for SHMU data processing'''

SHMUDIR =f'/home/pp/program/jupyter/SHMU/' 
TOPDIR = SHMUDIR + r'zber/'
TEPLOTY_SK_DIR = TOPDIR + r'teploty_sk/'
ZRAZKY_BREZNO_DIR = TOPDIR + r'zrazky_brezno/'
ZRAZKY_SK_DIR = TOPDIR + r'zrazky_sk/'
HLADINY_SK_DIR = TOPDIR + r'hladiny_sk/'
PRIETOKY_SK_DIR = TOPDIR + r'prietoky_sk/'
PODZEMNE_VODY_SK_DIR = TOPDIR + r'podzemne_vody_sk/'

TOPRESDIR = SHMUDIR + r'data/'
RES_TEPLOTY_SK_DIR = TOPRESDIR + r'teploty_sk/'
RES_ZRAZKY_BREZNO_DIR = TOPRESDIR + r'zrazky_brezno/'
RES_ZRAZKY_SK_DIR = TOPRESDIR + r'zrazky_sk/'
RES_HLADINY_SK_DIR = TOPRESDIR + r'hladiny_sk/'
RES_PRIETOKY_SK_DIR = TOPRESDIR + r'prietoky_sk/'
RES_PODZEMNE_VODY_SK_DIR = TOPRESDIR + r'podzemne_vody_sk/'



TEMPTESTFILE = TEPLOTY_SK_DIR + '2025-07-30-15-00.html'
UHRNTESTFILE = ZRAZKY_BREZNO_DIR + '2025-07-30-23-45.html'

DATADIRS = ['teploty_sk/', 'zrazky_brezno/', 'zrazky_sk/', 'hladiny_sk/','prietoky_sk/','podzemne_vody_sk/']
DATABASES = [
    TEPLOTY_SK_DIR+'teploty_sk',
    ZRAZKY_BREZNO_DIR + 'zrazky_brezno',
    ZRAZKY_SK_DIR + 'zrazky_sk',
    HLADINY_SK_DIR + 'hladiny_sk',
    PRIETOKY_SK_DIR +'prietoky_sk',
    PODZEMNE_VODY_SK_DIR + 'podzemne_vody_sk']
# DBFILES = [db+'.parquet' for db in DATABASES]
        

