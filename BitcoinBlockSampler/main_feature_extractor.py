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
    cur.execute('''CREATE TABLE IF NOT EXISTS Feature (
                 addr INTEGER PRIMARY KEY,
                 updatetime INTEGER NOT NULL,
                 cnttx INTEGER NOT NULL,
                 cnttxin INTEGER NOT NULL,
                 cnttxout INTEGER NOT NULL,
                 btc REAL NOT NULL,
                 btcin REAL NOT NULL,
                 btcout REAL NOT NULL,
                 dollar REAL NOT NULL,
                 dollarin REAL NOT NULL,
                 dollarout REAL NOT NULL,
                 cntuse INTEGER NOT NULL,
                 cntusein INTEGER NOT NULL,
                 cntuseout INTEGER NOT NULL,
                 age INTEGER NOT NULL,
                 agein INTEGER NOT NULL,
                 ageout INTEGER NOT NULL,
                 addrtypep2pkh INTEGER NOT NULL,
                 addrtypep2sh INTEGER NOT NULL,
                 addrtypebech32 INTEGER NOT NULL,
                 addrtypeother INTEGER NOT NULL
               );''')
    conn.commit()
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_AddrFeature_2 
                   ON Feature(updatetime);''')
    conn.commit()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialized cache database')


def get_feature(conn, cur, addr):
    result = dict()
    result['addr'] = addr
    result['updatetime'] = int(datetime.datetime.now().timestamp())
    # tx
    cur.execute('''SELECT COUNT(tx)
                   FROM (
                     SELECT DBCORE.TxIn.tx AS tx
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.addr = ?
                     UNION
                     SELECT DBCORE.TxOut.tx AS tx
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.addr = ?);''', (addr, addr))
    result['cnttx'] = cur.fetchone()[0] # Always return
    cur.execute('''SELECT COUNT(DISTINCT DBCORE.TxIn.tx)
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                          AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.addr = ?;''', (addr, ))
    result['cnttxin'] = cur.fetchone()[0] # Always return
    cur.execute('''SELECT COUNT(DISTINCT DBCORE.TxOut.tx)
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.addr = ?;''', (addr, ))
    result['cnttxout'] = cur.fetchone()[0] # Always return
    # btc
    cur.execute('''SELECT A.btc + B.btc
                   FROM (
                     SELECT SUM(DBCORE.TxOut.btc) AS btc
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.Addr = ?) AS A
                     , (
                     SELECT SUM(DBCORE.TxOut.btc) AS btc
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.Addr = ?) AS B;''', (addr, addr))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['btc'] = res
    cur.execute('''SELECT SUM(DBCORE.TxOut.btc) AS btc
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                          AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.Addr = ?''', (addr,))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['btcin'] = res
    cur.execute('''SELECT SUM(DBCORE.TxOut.btc) AS btc
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.Addr = ?''', (addr,))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['btcout'] = res
    # use
    cur.execute('''SELECT COUNT(tx)
                   FROM (
                     SELECT DBCORE.TxIn.tx AS tx
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.addr = ?
                     UNION ALL
                     SELECT DBCORE.TxOut.tx AS tx
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.addr = ?);''', (addr, addr))
    result['cntuse'] = cur.fetchone()[0] # Always return
    cur.execute('''SELECT COUNT(DBCORE.TxIn.tx)
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                          AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.addr = ?;''', (addr, ))
    result['cntusein'] = cur.fetchone()[0] # Always return
    cur.execute('''SELECT COUNT(DBCORE.TxOut.tx)
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.addr = ?;''', (addr, ))
    result['cntuseout'] = cur.fetchone()[0] # Always return
    # age
    cur.execute('''SELECT MAX(DBCORE.BlkTime.unixtime) - MIN(DBCORE.BlkTime.unixtime)
                   FROM (
                     SELECT DBCORE.TxIn.tx AS tx
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.addr = ?
                     UNION
                     SELECT DBCORE.TxOut.tx AS tx
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.addr = ?) AS T
                   INNER JOIN DBCORE.BlkTx ON T.tx = DBCORE.BlkTx.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk''', (addr, addr))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['age'] = res
    cur.execute('''SELECT MAX(DBCORE.BlkTime.unixtime) - MIN(DBCORE.BlkTime.unixtime)
                   FROM (
                     SELECT DBCORE.TxIn.tx AS tx
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.addr = ?) AS T
                   INNER JOIN DBCORE.BlkTx ON T.tx = DBCORE.BlkTx.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk''', (addr,))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['agein'] = res
    cur.execute('''SELECT MAX(DBCORE.BlkTime.unixtime) - MIN(DBCORE.BlkTime.unixtime)
                   FROM (
                     SELECT DBCORE.TxOut.tx AS tx
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.addr = ?) AS T
                   INNER JOIN DBCORE.BlkTx ON T.tx = DBCORE.BlkTx.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk''', (addr,))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['ageout'] = res
    # addrtype
    result['addrtypep2pkh'] = 0 # 1
    result['addrtypep2sh'] = 0 # 3
    result['addrtypebech32'] = 0 #bc1
    result['addrtypeother'] = 0
    cur.execute('''SELECT DBINDEX.AddrID.addr
                   FROM DBINDEX.AddrID
                   WHERE DBINDEX.AddrID.id = ?''', (addr,))
    res = cur.fetchone()
    if res is None:
        pass
    elif res[0].startswith('1'):
        result['addrtypep2pkh'] = 1
    elif res[0].startswith('3'):
        result['addrtypep2sh'] = 1
    elif res[0].startswith('bc1'):
        result['addrtypebech32'] = 1
    else:
        result['addrtypeother'] = 1

    return result


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

