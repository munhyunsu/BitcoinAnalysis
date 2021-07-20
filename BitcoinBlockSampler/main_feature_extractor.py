import os
import sqlite3
import time

import numpy as np
import pandas as pd

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def prepare_conn(indexpath, corepath, utilpath, servicepath):
    global STIME
    global DEBUG
    sqlite3.register_adapter(np.int32, int)
    conn = sqlite3.connect(servicepath)
    cur = conn.cursor()
    cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
    cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
    cur.execute(f'''ATTACH DATABASE '{utilpath}' AS DBUTIL;''')
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
                   );''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_AddrFeature_2 ON AddrFeature(update);''')
    conn.commit()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialized cache database')


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = prepare_conn(FLAGS.index, FLAGS.core, 
                             FLAGS.util, FLAGS.service)
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
    parser.add_argument('--util', type=str, required=True,
                        help='The path for util database')
    parser.add_argument('--service', type=str, required=True,
                        help='The path for service database')
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
    FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))

    DEBUG = FLAGS.debug

    main()

