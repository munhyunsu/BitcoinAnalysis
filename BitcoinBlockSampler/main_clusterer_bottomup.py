import os
import sqlite3
import time

import numpy as np
import pandas as pd

import db_manager
import data_structure

FLAGS = _ = None
DEBUG = False
STIME = None


def int32_to_int(value : np.int32):
    return int(value)


def prepare_conn(dbpath, indexpath, corepath):
    global DEBUG
    sqlite3.register_adapter(np.int32, int)
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
    cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
    conn.commit()
    
    return conn, cur


def initialize_cluster(conn, cur):
    global DEBUG
    global STIME
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialize Cluster Database')
    cur.execute('''PRAGMA journal_mode = NORMAL''')
    cur.execute('''PRAGMA synchronous = WAL''')
    conn.commit()
    cur.execute('''CREATE TABLE IF NOT EXISTS Cluster (
                     addr INTEGER PRIMARY KEY,
                     cluster NOT NULL);''')
    cur.execute('''CREATE TABLE IF NOT EXISTS TagID (
                     id INTEGER PRIMARY KEY,
                     tag TEXT UNIQUE);''')
    cur.execute('''CREATE TABLE IF NOT EXISTS Tag (
                     addr INTEGER NOT NULL,
                     tag INTEGER NOT NULL,
                     UNIQUE (addr, tag));''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_Cluster_2 ON Cluster(cluster);''')
    conn.commit()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialize Cluster Database Complete')


def get_index_status(conn, cur):
    global DEBUG
    cur.execute('''SELECT COUNT(BlkID.id) AS BlockHeight FROM DBINDEX.BlkID;''')
    block_height = int(cur.fetchone()[0])
    cur.execute('''SELECT COUNT(TxID.id) AS TransactionCounts FROM DBINDEX.TxID;''')
    tx_cnt = int(cur.fetchone()[0])
    cur.execute('''SELECT COUNT(AddrID.id) AS AddressCounts FROM DBINDEX.AddrID;''')
    addr_cnt = int(cur.fetchone()[0])
    cur.execute('''SELECT DATETIME(MAX(BlkTime.unixtime), 'unixepoch') AS Datetime 
                   FROM DBCORE.BlkTime;''')
    date = cur.fetchone()[0]
    
    return block_height, tx_cnt, addr_cnt, date


def do_clustering(conn, cur, tx_cnt, addr_cnt):
    global DEBUG
    global STIME
    cluster = data_structure.UnionFind(addr_cnt)
    addrs = list()
    for txid in range(1, tx_cnt+1):
        t1 = time.time()
        addrs.clear()
        for result in cur.execute(db_manager.QUERY['SELECT_TXIN_ADDRS'], (txid, )):
            addrs.append(result[0])
        t2 = time.time()
        if len(addrs) > 1:
            x = min(addrs)
            for y in addrs:
                cluster.union(x, y)
        t3 = time.time()
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Processed {txid}', end='\r')
    if DEBUG:
        print()
    return cluster


def write_db(conn, cur, cluster, addr_cnt):
    global DEBUG
    global STIME
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Write db start')
    cur.execute('''PRAGMA journal_mode = OFF''')
    cur.execute('''PRAGMA synchronous = OFF''')
    conn.commit()
    cur.execute('BEGIN TRANSACTION')
    for i in range(1, addr_cnt+1):
        c = cluster.find(i)
        cur.execute('''INSERT OR REPLACE INTO Cluster (addr, cluster)
                       VALUES (?, ?);''', (i, c))
    cur.execute('COMMIT TRANSACTION')
    cur.execute('''PRAGMA journal_mode = NORMAL''')
    cur.execute('''PRAGMA synchronous = WAL''')
    conn.commit()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Write db done')


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    print(f'[{int(time.time()-STIME)}] Start clusterer')

    conn, cur = prepare_conn(FLAGS.service, FLAGS.index, FLAGS.core)
    initialize_cluster(conn, cur)

    block_height, tx_cnt, addr_cnt, date = get_index_status(conn, cur)
    if DEBUG:
        print('비트코인 데이터베이스 불러오기 완료')
        print((f'블록 높이: {block_height} ({date} UTC 마이닝)\n'
               f'트랜잭션 개수: {tx_cnt}\n'
               f'주소 개수: {addr_cnt}'))

    cluster = do_clustering(conn, cur, tx_cnt, addr_cnt)
    
    write_db(conn, cur, cluster, addr_cnt)
    
    print(f'[{int(time.time()-STIME)}] Terminate clusterer')


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
    parser.add_argument('--service', type=str, default='./service.db',
                        help='The path for util database')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))

    DEBUG = FLAGS.debug
    
    STIME = time.time()
    main()
