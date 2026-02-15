'''Configuration file for SHMU data processing'''

CONNSTRS = ['postgresql://pp:ppp@192.168.1.105:5432/shmu',  # database connection string doma
            'postgresql://pp:ppp@172.16.0.119:5432/shmu']   # database connection string ENVIGEO - postgresql server

SHMUDIRS =[f'/home/pp/program/jupyter/SHMUSSD/',f'f:/AAA/DATA/SHMU/', f'C:/Users/envigeo/program/python/klima/SHMU/']  # possible SHMU data directories
SHMUDIR = SHMUDIRS[2]  # select the appropriate SHMU data directory
CONNSTR = CONNSTRS[1]  # select the appropriate database connection string

TOPDIR = SHMUDIR + r'zber/'
TEPLOTY_SK_DIR = TOPDIR + r'teploty_sk/'
ZRAZKY_BREZNO_DIR = TOPDIR + r'zrazky_brezno/'
ZRAZKY_SK_DIR = TOPDIR + r'zrazky_sk/'
HLADINY_SK_DIR = TOPDIR + r'hladiny_sk/'
PRIETOKY_SK_DIR = TOPDIR + r'prietoky_sk/'
PODZEMNE_VODY_SK_DIR = TOPDIR + r'podzemne_vody_sk/'
# odlisne adresare pre podzemne vody, lebo v jednom html subore su dve tabulky jednadata pre vrty a v druhom pre pramene
# PODZEMNE_VODY_PRM_SK_DIR = TOPDIR + r'podzemne_vody_prm_sk/'
# PODZEMNE_VODY_VRT_SK_DIR = TOPDIR + r'podzemne_vody_vrt_sk/'

TOPRESDIR = SHMUDIR + r'data/'
RES_TEPLOTY_SK_DIR = TOPRESDIR + r'teploty_sk/'
RES_ZRAZKY_BREZNO_DIR = TOPRESDIR + r'zrazky_brezno/'
RES_ZRAZKY_SK_DIR = TOPRESDIR + r'zrazky_sk/'
RES_HLADINY_SK_DIR = TOPRESDIR + r'hladiny_sk/'
RES_PRIETOKY_SK_DIR = TOPRESDIR + r'prietoky_sk/'
RES_PODZEMNE_VODY_SK_DIR = TOPRESDIR + r'podzemne_vody_sk/'
RES_PODZEMNE_VODY_PRM_SK_DIR = TOPRESDIR + r'podzemne_vody_prm_sk/'
RES_PODZEMNE_VODY_VRT_SK_DIR = TOPRESDIR + r'podzemne_vody_vrt_sk/'



TEMPTESTFILE = TEPLOTY_SK_DIR + '2025-07-30-15-00.html'
UHRNTESTFILE = ZRAZKY_BREZNO_DIR + '2025-07-30-23-45.html'


workflow = [
    {'func': 'hladiny_sk', 'infile': RES_HLADINY_SK_DIR, 'lokalita': 'Brezno', 'Tok':'Hron'},
    {'func': 'prietoky_sk', 'infile': RES_PRIETOKY_SK_DIR, 'lokalita': 'Brezno - Hron'},
    {'func': 'podzemne_vody_prm_sk', 'infile': RES_PODZEMNE_VODY_PRM_SK_DIR, 'lokalita': None, 'povodie': 'Hron'},
    {'func': 'podzemne_vody_vrt_sk', 'infile': RES_PODZEMNE_VODY_VRT_SK_DIR, 'lokalita': None},
    {'func': 'zrazky_sk', 'infile': RES_ZRAZKY_SK_DIR, 'lokalita': 'Brezno'},
    {'func': 'teploty', 'infile': RES_TEPLOTY_SK_DIR, 'lokalita': 'Brezno'}
    ]


# DATADIRS = ['teploty_sk/', 'zrazky_brezno/', 'zrazky_sk/', 'hladiny_sk/','prietoky_sk/','podzemne_vody_sk/']
# DATABASES = [
#     TEPLOTY_SK_DIR+'teploty_sk',
#     ZRAZKY_BREZNO_DIR + 'zrazky_brezno',
#     ZRAZKY_SK_DIR + 'zrazky_sk',
#     HLADINY_SK_DIR + 'hladiny_sk',
#     PRIETOKY_SK_DIR +'prietoky_sk',
#     PODZEMNE_VODY_SK_DIR + 'podzemne_vody_sk']
# DBFILES = [db+'.parquet' for db in DATABASES]
        
# not used
# MEASUREMENTS = {
#     'teploty': 'Teplota vzduchu (°C)',
#     'zrazky_brezno': 'Úhrn zrážok (mm)',
#     'zrazky_sk': 'Úhrn zrážok (mm)',
#     'hladiny_sk': 'Vodný stav (cm)',
#     'prietoky_sk': 'Prietok (m3/s)',
#     'podzemne_vody_sk': 'Hladina podzemnej vody (m'}

