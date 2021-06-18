import os
import sqlite3
import time

import numpy as np
import pandas as pd

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def prepare_conn(dbpath, indexpath, corepath, servicepath):
    global STIME
    global DEBUG
    sqlite3.register_adapter(np.int32, int)
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
    cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
    cur.execute(f'''ATTACH DATABASE '{servicepath}' AS DBSERVICE;''')
    conn.commit()

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Prepared database connector and cursor')
    
    return conn, cur


def initialize_database(conn, cur):
    global STIME
    global DEBUG
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initializing cache database')
    cur.execute('''PRAGMA journal_mode = NORMAL;''')
    cur.execute('''PRAGMA synchronous = WAL;''')
    conn.commit()
    cur.execute('''CREATE TABLE IF NOT EXISTS AddrFeature (
                     addr INTEGER PRIMARY KEY,
                     update INTEGER NOT NULL,
                     tx INTEGER NOT NULL,
                     txin INTEGER NOT NULL,
                     txout INTEGER NOT NULL,
                     btc REAL NOT NULL,
                     btcinmin REAL NOT NULL,
                     btcinmax REAL NOT NULL,
                     btcinsum REAL NOT NULL,
                     btcinmedian REAL NOT NULL,
                     btcinstdev REAL NOT NULL,
                     btcinm1 REAL NOT NULL,
                     btcinm2 REAL NOT NULL,
                     btcinm3 REAL NOT NULL,
                     btcinm4 REAL NOT NULL,
                     btcoutmin REAL NOT NULL,
                     btcoutmax REAL NOT NULL,
                     btcoutsum REAL NOT NULL,
                     btcoutmedian REAL NOT NULL,
                     btcoutstdev REAL NOT NULL,
                     btcoutm1 REAL NOT NULL,
                     btcoutm2 REAL NOT NULL,
                     btcoutm3 REAL NOT NULL,
                     btcoutm4 REAL NOT NULL,
                     use INTEGER NOT NULL,
                     usein INTEGER NOT NULL,
                     useout INTEGER NOT NULL,
                     age INTEGER NOT NULL,
                     agemin INTEGER NOT NULL,
                     agemax INTEGER NOT NULL,
                     agestdev REAL NOT NULL,
                     agem1 REAL NOT NULL,
                     agem2 REAL NOT NULL,
                     agem3 REAL NOT NULL,
                     agem4 REAL NOT NULL,
                     agein INTEGER NOT NULL,
                     ageinmin INTEGER NOT NULL,
                     ageinmax INTEGER NOT NULL,
                     ageinstdev REAL NOT NULL,
                     ageinm1 REAL NOT NULL,
                     ageinm2 REAL NOT NULL,
                     ageinm3 REAL NOT NULL,
                     ageinm4 REAL NOT NULL,
                     ageout INTEGER NOT NULL,
                     ageoutmin INTEGER NOT NULL,
                     ageoutmax INTEGER NOT NULL,
                     ageoutstdev REAL NOT NULL,
                     ageoutm1 REAL NOT NULL,
                     ageoutm2 REAL NOT NULL,
                     ageoutm3 REAL NOT NULL,
                     ageoutm4 REAL NOT NULL,
                     isbech32 INTEGER NOT NULL,
                     isp2pkh INTEGER NOT NULL,
                     isp2sh INTEGER NOT NULL);''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_AddrFeature_2 ON AddrFeature(update);''')
    conn.commit()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialized cache database')


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = prepare_conn(FLAGS.cache, FLAGS.index, 
                             FLAGS.core, FLAGS.service)
    initialize_database(conn, cur)

    # Multiprocessing

    conn.close()


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--index', type=str, required=True,
                        help='The path for index database')
    parser.add_argument('--core', type=str, required=True,
                        help='The path for core database')
    parser.add_argument('--service', type=str, required=True,
                        help='The path for util database')
    parser.add_argument('--cache', type=str, default='./dbv3-cache.db',
                        help='The path for cache database')
    parser.add_argument('--process', type=int,
                        default=min(multiprocessing.cpu_count()//2, 16),
                        help='The number of multiprocess')
    parser.add_argument('--pagesize', type=int, default=1024*4,
                        help='The page size of database (Max: 1024*64)')
    parser.add_argument('--cachesize', type=int, default=-1*2000,
                        help='The cache size of page (GBx1024×1024×1024÷(64×1024))')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
    FLAGS.cache = os.path.abspath(os.path.expanduser(FLAGS.cache))

    DEBUG = FLAGS.debug

    main()

