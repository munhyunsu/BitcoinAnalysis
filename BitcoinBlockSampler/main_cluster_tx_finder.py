import os
import csv
import sqlite3
import pickle
import time
import statistics

import numpy as np
np.warnings.filterwarnings('ignore')
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

FLAGS = _ = None
DEBUG = False
STIME = None


def prepare_conn(dbpath, indexpath, corepath):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
    cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
    conn.commit()
    
    return conn, cur


def insert_db(conn, cur, data):
    global DEBUG
    global STIME
    
    # Get AddrID
    cur.execute('''SELECT id FROM DBINDEX.AddrID
                   WHERE DBINDEX.AddrID.addr = ?;''', (data['address'],))
    addrid = cur.fetchone()[0]
    
    # Get TagID
    cur.execute('''INSERT OR IGNORE INTO TagID (tag)
                   VALUES (?);''', (data['tag'],))
    conn.commit()
    cur.execute('''SELECT id FROM TagID
                   WHERE TagID.tag = ?;''', (data['tag'],))
    tagid = cur.fetchone()[0]
    
    # Insert Addr-Tag
    cur.execute('''INSERT OR IGNORE INTO Tag (addr, tag)
                   VALUES (?, ?);''', (addrid, tagid))
    conn.commit()

def get_entity(conn, cur, addr):
    global DEBUG
    global STIME
    
#     # Get AddrID
#     cur.execute('''SELECT id FROM DBINDEX.AddrID
#                    WHERE DBINDEX.AddrID.addr = ?;''', (addr,))
#     addrid = cur.fetchone()[0]
    addrid = addr
    
    # Get ClusterID
    cur.execute('''SELECT cluster FROM Cluster
                   WHERE addr = ?;''', (addrid,))
    clusterid = cur.fetchone()[0]
    
    # Get Tags
    cur.execute('''SELECT TagID.tag
                   FROM Tag
                   INNER JOIN TagID ON TagID.id = Tag.tag
                   WHERE Tag.addr IN (SELECT Cluster.addr
                                      FROM Cluster
                                      WHERE Cluster.cluster = ?);''', (clusterid,))
    tags = set()
    for row in cur.fetchall():
        tags.add(row[0])
        
    return tags
    
def prepare_clf(model_path):
    clf = None
    with open(model_path, 'rb') as f:
        clf = pickle.load(f)
    return clf

def get_category(conn, cur, clf, addrid):
    # Get ClusterID
    cur.execute('''SELECT cluster FROM Cluster
                   WHERE addr = ?;''', (addrid,))
    clusterid = cur.fetchone()[0]
    
    # Get Addrs
    addresses = set()
    cur.execute('''SELECT addr FROM Cluster
                   WHERE cluster = ?;''', (clusterid,))
    for row in cur.fetchall():
        addresses.add(row[0])

    inds = list()
    inbs = list()
    outds = list()
    outbs = list()
    for address in addresses:
        for ind, inb in cur.execute('''SELECT COUNT(*) AS Outdegree, SUM(btc) AS Outcome
                                       FROM TxIn
                                       INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
                                       WHERE TxOut.addr = ?;''', (address, )):
            if ind is None:
                ind = 0
            if inb is None:
                inb = 0
            inds.append(ind)
            inbs.append(inb)
        for outd, outb in cur.execute('''SELECT COUNT(*) AS Indegree, SUM(btc) AS Income
                                         FROM TxOut
                                         WHERE TxOut.addr = ?;''', (address, )):
            if outd is None:
                outd = 0
            if outb is None:
                outb = 0
            outds.append(outd)
            outbs.append(outb)
    feature = [
        min(inds),
        statistics.mean(inds),
        statistics.median(inds),
        max(inds),
        min(inbs),
        statistics.mean(inbs),
        statistics.median(inbs),
        max(inbs),
        min(outds),
        statistics.mean(outds),
        statistics.median(outds),
        max(outds),
        min(outbs),
        statistics.mean(outbs),
        statistics.median(outbs),
        max(outbs)
    ]
    
    return clf.predict([feature])


def get_tx(conn, cur, tx):
    global DEBUG
    global STIME
    
    # Get TxID
    cur.execute('''SELECT id FROM DBINDEX.TxID
                   WHERE DBINDEX.TxID.txid = ?;''', (tx,))
    txid = cur.fetchone()[0]
    
    # In_addr
    iaddr = set()
    cur.execute('''SELECT DBCORE.TxOut.addr
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND
                                              DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxIn.tx = ?;''', (txid,))
    for row in cur.fetchall():
        iaddr.add(row[0])
    
    # Out_addr
    oaddr = set()
    cur.execute('''SELECT DBCORE.TxOut.addr
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.tx = ?;''', (txid,))
    for row in cur.fetchall():
        oaddr.add(row[0])
    
    # In_tag
    itag = set()
    for iad in iaddr:
        itag.update(get_entity(conn, cur, iad))
    
    # Out_tag
    otag = set()
    for oad in oaddr:
        otag.update(get_entity(conn, cur, oad))
    
    return itag, otag


def get_tx_cat(conn, cur, clf, tx):
    global DEBUG
    global STIME
    
    # Get TxID
    cur.execute('''SELECT id FROM DBINDEX.TxID
                   WHERE DBINDEX.TxID.txid = ?;''', (tx,))
    txid = cur.fetchone()[0]
    
    # In_addr
    iaddr = set()
    cur.execute('''SELECT DBCORE.TxOut.addr
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND
                                              DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxIn.tx = ?;''', (txid,))
    for row in cur.fetchall():
        iaddr.add(row[0])
    
    # Out_addr
    oaddr = set()
    cur.execute('''SELECT DBCORE.TxOut.addr
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.tx = ?;''', (txid,))
    for row in cur.fetchall():
        oaddr.add(row[0])
    
    # In_tag
    icat = set()
    for iad in iaddr:
        icat.add(get_category(conn, cur, clf, iad)[0])
    
    # Out_tag
    ocat = set()
    for oad in oaddr:
        ocat.add(get_category(conn, cur, clf, oad)[0])
    
    return icat, ocat


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    print(f'[{int(time.time()-STIME)}] Start tag search')

    conn, cur = prepare_conn(FLAGS.service, FLAGS.index, FLAGS.core)
    clf = prepare_clf(FLAGS.model)

    in_tags, out_tags = get_tx(conn, cur, FLAGS.target)
    print(f'입/출력 엔티티 (태그)')
    print(f'{in_tags}')
    print(f' ==> ')
    print(f'{out_tags}')
    print(f'')
    print(f'')
    
    in_cats, out_cats = get_tx_cat(conn, cur, clf, FLAGS.target)
    print(f'입/출력 카테고리 예측')
    print(f'{in_cats}')
    print(f' ==> ')
    print(f'{out_cats}')
    print(f'')
    
    print(f'[{int(time.time()-STIME)}] Terminate tag search')


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
    parser.add_argument('--model', type=str, required=True,
                        help='The path for category model')
    parser.add_argument('--target', type=str, required=True,
                        help='Target address')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
    FLAGS.model = os.path.abspath(os.path.expanduser(FLAGS.model))

    DEBUG = FLAGS.debug

    STIME = time.time()
    main()
