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
        print((f'[{int(time.time()-STIME)}] '
               f'Prepared database connector and cursor'))
    
    return conn, cur


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = prepare_conn(FLAGS.cache, FLAGS.index, 
                             FLAGS.core, FLAGS.service)

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

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
    FLAGS.cache = os.path.abspath(os.path.expanduser(FLAGS.cache))

    DEBUG = FLAGS.debug

    main()

