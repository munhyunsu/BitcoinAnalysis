import os
import sqlite3
import time
import datetime
import multiprocessing

import numpy as np
import pandas as pd

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def prepare_conn(indexpath, corepath, utilpath, servicepath):
    global STIME
    global DEBUG
    sqlite3.register_adapter(np.int32, int)
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
    cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
    cur.execute(f'''ATTACH DATABASE '{utilpath}' AS DBUTIL;''')
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
    cur.execute('''CREATE TABLE IF NOT EXISTS Feature (
                 addr INTEGER PRIMARY KEY,
                 updatetime INTEGER NOT NULL,
                 cnttx INTEGER NOT NULL,
                 cnttxin INTEGER NOT NULL,
                 cnttxout INTEGER NOT NULL,
                 btc REAL NOT NULL,
                 btcin REAL NOT NULL,
                 btcout REAL NOT NULL,
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
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_Feature_2 
                   ON Feature(updatetime);''')
    conn.commit()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialized cache database')


def get_associate_addr(conn, cur, addr):
    # target
    result = {addr}
    a = set()
    b = set()
    c = set()
    # multiinput
    for row in cur.execute('''SELECT DBSERVICE.Cluster.addr
                              FROM DBSERVICE.Cluster
                              WHERE DBSERVICE.Cluster.cluster IN (
                                SELECT DBSERVICE.Cluster.cluster
                                FROM DBSERVICE.Cluster
                                WHERE DBSERVICE.Cluster.addr = ?);''', (addr,)):
        result.add(row[0])
        a.add(row[0])
    for row in cur.execute('''SELECT DBUTIL.Edge.src
                              FROM DBUTIL.Edge
                              WHERE DBUTIL.Edge.dst = ?;''', (addr,)):
        result.add(row[0])
        b.add(row[0])
    for row in cur.execute('''SELECT DBUTIL.Edge.dst
                              FROM DBUTIL.Edge
                              WHERE DBUTIL.Edge.src = ?;''', (addr,)):
        result.add(row[0])
        c.add(row[0])
    return (result, a, b, c)


def get_feature_vector(conn, cur, addrid):
    vector = []
    a, m, i, o = get_associate_addr(conn, cur, addrid)
    # target
    cur.execute('''SELECT cnttx, cnttxin, cnttxout,
                          btc, btcin, btcout,
                          cntuse, cntusein, cntuseout,
                          age, agein, ageout,
                          addrtypep2pkh, addrtypep2sh,
                         addrtypebech32, addrtypeother
                   FROM Feature
                   WHERE Feature.addr = ?;''', (addrid,))
    res = cur.fetchone()
    vector.append(res[0])
    vector.append(res[1])
    vector.append(res[2])
    vector.append(res[3])
    vector.append(res[4])
    vector.append(res[5])
    vector.append(res[6])
    vector.append(res[7])
    vector.append(res[8])
    vector.append(res[9])
    vector.append(res[10])
    vector.append(res[11])
    vector.append(res[12])
    vector.append(res[13])
    vector.append(res[14])
    vector.append(res[15])
    # mi
    cur.execute('''DROP TABLE IF EXISTS AddrList;''')
    cur.execute('''CREATE TABLE IF NOT EXISTS AddrList (
                     addr INTEGER PRIMARY KEY);''')
    conn.commit()
    cur.execute('BEGIN TRANSACTION')
    for addr in m:
        cur.execute('''INSERT OR IGNORE INTO AddrList (
                         addr) VALUES (
                         ?);''', (addr,))
    cur.execute('COMMIT TRANSACTION')
    conn.commit()
    cur.execute('''SELECT DBSERVICE.Feature.cnttx, DBSERVICE.Feature.cnttxin, DBSERVICE.Feature.cnttxout,
                          DBSERVICE.Feature.btc, DBSERVICE.Feature.btcin, DBSERVICE.Feature.btcout,
                          DBSERVICE.Feature.cntuse, DBSERVICE.Feature.cntusein, DBSERVICE.Feature.cntuseout,
                          DBSERVICE.Feature.age, DBSERVICE.Feature.agein, DBSERVICE.Feature.ageout,
                          DBSERVICE.Feature.addrtypep2pkh, DBSERVICE.Feature.addrtypep2sh,
                          DBSERVICE.Feature.addrtypebech32, DBSERVICE.Feature.addrtypeother
                   FROM AddrList
                   INNER JOIN DBSERVICE.Feature ON DBSERVICE.Feature.addr = AddrList.addr;''')
    res = pd.DataFrame(cur.fetchall())
    if len(res) > 0:
        vector.append(min(res[0]))
        vector.append(max(res[0]))
        vector.append(sum(res[0]))
        vector.append(statistics.median(res[0]))
        vector.append(statistics.mean(res[0]))
        vector.append(moment(res[0], moment=2))
        vector.append(moment(res[0], moment=3))
        vector.append(moment(res[0], moment=4))
        vector.append(min(res[1]))
        vector.append(max(res[1]))
        vector.append(sum(res[1]))
        vector.append(statistics.median(res[1]))
        vector.append(statistics.mean(res[1]))
        vector.append(moment(res[1], moment=2))
        vector.append(moment(res[1], moment=3))
        vector.append(moment(res[1], moment=4))
        vector.append(min(res[2]))
        vector.append(max(res[2]))
        vector.append(sum(res[2]))
        vector.append(statistics.median(res[2]))
        vector.append(statistics.mean(res[2]))
        vector.append(moment(res[2], moment=2))
        vector.append(moment(res[2], moment=3))
        vector.append(moment(res[2], moment=4))
        vector.append(min(res[3]))
        vector.append(max(res[3]))
        vector.append(sum(res[3]))
        vector.append(statistics.median(res[3]))
        vector.append(statistics.mean(res[3]))
        vector.append(moment(res[3], moment=2))
        vector.append(moment(res[3], moment=3))
        vector.append(moment(res[3], moment=4))
        vector.append(min(res[4]))
        vector.append(max(res[4]))
        vector.append(sum(res[4]))
        vector.append(statistics.median(res[4]))
        vector.append(statistics.mean(res[4]))
        vector.append(moment(res[4], moment=2))
        vector.append(moment(res[4], moment=3))
        vector.append(moment(res[4], moment=4))
        vector.append(min(res[5]))
        vector.append(max(res[5]))
        vector.append(sum(res[5]))
        vector.append(statistics.median(res[5]))
        vector.append(statistics.mean(res[5]))
        vector.append(moment(res[5], moment=2))
        vector.append(moment(res[5], moment=3))
        vector.append(moment(res[5], moment=4))
        vector.append(min(res[6]))
        vector.append(max(res[6]))
        vector.append(sum(res[6]))
        vector.append(statistics.median(res[6]))
        vector.append(statistics.mean(res[6]))
        vector.append(moment(res[6], moment=2))
        vector.append(moment(res[6], moment=3))
        vector.append(moment(res[6], moment=4))
        vector.append(min(res[7]))
        vector.append(max(res[7]))
        vector.append(sum(res[7]))
        vector.append(statistics.median(res[7]))
        vector.append(statistics.mean(res[7]))
        vector.append(moment(res[7], moment=2))
        vector.append(moment(res[7], moment=3))
        vector.append(moment(res[7], moment=4))
        vector.append(min(res[8]))
        vector.append(max(res[8]))
        vector.append(sum(res[8]))
        vector.append(statistics.median(res[8]))
        vector.append(statistics.mean(res[8]))
        vector.append(moment(res[8], moment=2))
        vector.append(moment(res[8], moment=3))
        vector.append(moment(res[8], moment=4))
        vector.append(min(res[9]))
        vector.append(max(res[9]))
        vector.append(sum(res[9]))
        vector.append(statistics.median(res[9]))
        vector.append(statistics.mean(res[9]))
        vector.append(moment(res[9], moment=2))
        vector.append(moment(res[9], moment=3))
        vector.append(moment(res[9], moment=4))
        vector.append(min(res[10]))
        vector.append(max(res[10]))
        vector.append(sum(res[10]))
        vector.append(statistics.median(res[10]))
        vector.append(statistics.mean(res[10]))
        vector.append(moment(res[10], moment=2))
        vector.append(moment(res[10], moment=3))
        vector.append(moment(res[10], moment=4))
        vector.append(min(res[11]))
        vector.append(max(res[11]))
        vector.append(sum(res[11]))
        vector.append(statistics.median(res[11]))
        vector.append(statistics.mean(res[11]))
        vector.append(moment(res[11], moment=2))
        vector.append(moment(res[11], moment=3))
        vector.append(moment(res[11], moment=4))
        vector.append(min(res[12]))
        vector.append(max(res[12]))
        vector.append(sum(res[12]))
        vector.append(statistics.median(res[12]))
        vector.append(statistics.mean(res[12]))
        vector.append(moment(res[12], moment=2))
        vector.append(moment(res[12], moment=3))
        vector.append(moment(res[12], moment=4))
        vector.append(min(res[13]))
        vector.append(max(res[13]))
        vector.append(sum(res[13]))
        vector.append(statistics.median(res[13]))
        vector.append(statistics.mean(res[13]))
        vector.append(moment(res[13], moment=2))
        vector.append(moment(res[13], moment=3))
        vector.append(moment(res[13], moment=4))
        vector.append(min(res[14]))
        vector.append(max(res[14]))
        vector.append(sum(res[14]))
        vector.append(statistics.median(res[14]))
        vector.append(statistics.mean(res[14]))
        vector.append(moment(res[14], moment=2))
        vector.append(moment(res[14], moment=3))
        vector.append(moment(res[14], moment=4))
        vector.append(min(res[15]))
        vector.append(max(res[15]))
        vector.append(sum(res[15]))
        vector.append(statistics.median(res[15]))
        vector.append(statistics.mean(res[15]))
        vector.append(moment(res[15], moment=2))
        vector.append(moment(res[15], moment=3))
        vector.append(moment(res[15], moment=4))
    else:
        vector.extend([0]*128)
    # in
    cur.execute('''DROP TABLE IF EXISTS AddrList;''')
    cur.execute('''CREATE TABLE IF NOT EXISTS AddrList (
                     addr INTEGER PRIMARY KEY);''')
    conn.commit()
    cur.execute('BEGIN TRANSACTION')
    for addr in i:
        cur.execute('''INSERT OR IGNORE INTO AddrList (
                         addr) VALUES (
                         ?);''', (addr,))
    cur.execute('COMMIT TRANSACTION')
    conn.commit()
    cur.execute('''SELECT DBSERVICE.Feature.cnttx, DBSERVICE.Feature.cnttxin, DBSERVICE.Feature.cnttxout,
                          DBSERVICE.Feature.btc, DBSERVICE.Feature.btcin, DBSERVICE.Feature.btcout,
                          DBSERVICE.Feature.cntuse, DBSERVICE.Feature.cntusein, DBSERVICE.Feature.cntuseout,
                          DBSERVICE.Feature.age, DBSERVICE.Feature.agein, DBSERVICE.Feature.ageout,
                          DBSERVICE.Feature.addrtypep2pkh, DBSERVICE.Feature.addrtypep2sh,
                          DBSERVICE.Feature.addrtypebech32, DBSERVICE.Feature.addrtypeother
                   FROM AddrList
                   INNER JOIN DBSERVICE.Feature ON DBSERVICE.Feature.addr = AddrList.addr;''')
    res = pd.DataFrame(cur.fetchall())
    if len(res) > 0:
        vector.append(min(res[0]))
        vector.append(max(res[0]))
        vector.append(sum(res[0]))
        vector.append(statistics.median(res[0]))
        vector.append(statistics.mean(res[0]))
        vector.append(moment(res[0], moment=2))
        vector.append(moment(res[0], moment=3))
        vector.append(moment(res[0], moment=4))
        vector.append(min(res[1]))
        vector.append(max(res[1]))
        vector.append(sum(res[1]))
        vector.append(statistics.median(res[1]))
        vector.append(statistics.mean(res[1]))
        vector.append(moment(res[1], moment=2))
        vector.append(moment(res[1], moment=3))
        vector.append(moment(res[1], moment=4))
        vector.append(min(res[2]))
        vector.append(max(res[2]))
        vector.append(sum(res[2]))
        vector.append(statistics.median(res[2]))
        vector.append(statistics.mean(res[2]))
        vector.append(moment(res[2], moment=2))
        vector.append(moment(res[2], moment=3))
        vector.append(moment(res[2], moment=4))
        vector.append(min(res[3]))
        vector.append(max(res[3]))
        vector.append(sum(res[3]))
        vector.append(statistics.median(res[3]))
        vector.append(statistics.mean(res[3]))
        vector.append(moment(res[3], moment=2))
        vector.append(moment(res[3], moment=3))
        vector.append(moment(res[3], moment=4))
        vector.append(min(res[4]))
        vector.append(max(res[4]))
        vector.append(sum(res[4]))
        vector.append(statistics.median(res[4]))
        vector.append(statistics.mean(res[4]))
        vector.append(moment(res[4], moment=2))
        vector.append(moment(res[4], moment=3))
        vector.append(moment(res[4], moment=4))
        vector.append(min(res[5]))
        vector.append(max(res[5]))
        vector.append(sum(res[5]))
        vector.append(statistics.median(res[5]))
        vector.append(statistics.mean(res[5]))
        vector.append(moment(res[5], moment=2))
        vector.append(moment(res[5], moment=3))
        vector.append(moment(res[5], moment=4))
        vector.append(min(res[6]))
        vector.append(max(res[6]))
        vector.append(sum(res[6]))
        vector.append(statistics.median(res[6]))
        vector.append(statistics.mean(res[6]))
        vector.append(moment(res[6], moment=2))
        vector.append(moment(res[6], moment=3))
        vector.append(moment(res[6], moment=4))
        vector.append(min(res[7]))
        vector.append(max(res[7]))
        vector.append(sum(res[7]))
        vector.append(statistics.median(res[7]))
        vector.append(statistics.mean(res[7]))
        vector.append(moment(res[7], moment=2))
        vector.append(moment(res[7], moment=3))
        vector.append(moment(res[7], moment=4))
        vector.append(min(res[8]))
        vector.append(max(res[8]))
        vector.append(sum(res[8]))
        vector.append(statistics.median(res[8]))
        vector.append(statistics.mean(res[8]))
        vector.append(moment(res[8], moment=2))
        vector.append(moment(res[8], moment=3))
        vector.append(moment(res[8], moment=4))
        vector.append(min(res[9]))
        vector.append(max(res[9]))
        vector.append(sum(res[9]))
        vector.append(statistics.median(res[9]))
        vector.append(statistics.mean(res[9]))
        vector.append(moment(res[9], moment=2))
        vector.append(moment(res[9], moment=3))
        vector.append(moment(res[9], moment=4))
        vector.append(min(res[10]))
        vector.append(max(res[10]))
        vector.append(sum(res[10]))
        vector.append(statistics.median(res[10]))
        vector.append(statistics.mean(res[10]))
        vector.append(moment(res[10], moment=2))
        vector.append(moment(res[10], moment=3))
        vector.append(moment(res[10], moment=4))
        vector.append(min(res[11]))
        vector.append(max(res[11]))
        vector.append(sum(res[11]))
        vector.append(statistics.median(res[11]))
        vector.append(statistics.mean(res[11]))
        vector.append(moment(res[11], moment=2))
        vector.append(moment(res[11], moment=3))
        vector.append(moment(res[11], moment=4))
        vector.append(min(res[12]))
        vector.append(max(res[12]))
        vector.append(sum(res[12]))
        vector.append(statistics.median(res[12]))
        vector.append(statistics.mean(res[12]))
        vector.append(moment(res[12], moment=2))
        vector.append(moment(res[12], moment=3))
        vector.append(moment(res[12], moment=4))
        vector.append(min(res[13]))
        vector.append(max(res[13]))
        vector.append(sum(res[13]))
        vector.append(statistics.median(res[13]))
        vector.append(statistics.mean(res[13]))
        vector.append(moment(res[13], moment=2))
        vector.append(moment(res[13], moment=3))
        vector.append(moment(res[13], moment=4))
        vector.append(min(res[14]))
        vector.append(max(res[14]))
        vector.append(sum(res[14]))
        vector.append(statistics.median(res[14]))
        vector.append(statistics.mean(res[14]))
        vector.append(moment(res[14], moment=2))
        vector.append(moment(res[14], moment=3))
        vector.append(moment(res[14], moment=4))
        vector.append(min(res[15]))
        vector.append(max(res[15]))
        vector.append(sum(res[15]))
        vector.append(statistics.median(res[15]))
        vector.append(statistics.mean(res[15]))
        vector.append(moment(res[15], moment=2))
        vector.append(moment(res[15], moment=3))
        vector.append(moment(res[15], moment=4))
    else:
        vector.extend([0]*128)
    # out
    cur.execute('''DROP TABLE IF EXISTS AddrList;''')
    cur.execute('''CREATE TABLE IF NOT EXISTS AddrList (
                     addr INTEGER PRIMARY KEY);''')
    conn.commit()
    cur.execute('BEGIN TRANSACTION')
    for addr in o:
        cur.execute('''INSERT OR IGNORE INTO AddrList (
                         addr) VALUES (
                         ?);''', (addr,))
    cur.execute('COMMIT TRANSACTION')
    conn.commit()
    cur.execute('''SELECT DBSERVICE.Feature.cnttx, DBSERVICE.Feature.cnttxin, DBSERVICE.Feature.cnttxout,
                          DBSERVICE.Feature.btc, DBSERVICE.Feature.btcin, DBSERVICE.Feature.btcout,
                          DBSERVICE.Feature.cntuse, DBSERVICE.Feature.cntusein, DBSERVICE.Feature.cntuseout,
                          DBSERVICE.Feature.age, DBSERVICE.Feature.agein, DBSERVICE.Feature.ageout,
                          DBSERVICE.Feature.addrtypep2pkh, DBSERVICE.Feature.addrtypep2sh,
                          DBSERVICE.Feature.addrtypebech32, DBSERVICE.Feature.addrtypeother
                   FROM AddrList
                   INNER JOIN DBSERVICE.Feature ON DBSERVICE.Feature.addr = AddrList.addr;''')
    res = pd.DataFrame(cur.fetchall())
    if len(res) > 0:
        vector.append(min(res[0]))
        vector.append(max(res[0]))
        vector.append(sum(res[0]))
        vector.append(statistics.median(res[0]))
        vector.append(statistics.mean(res[0]))
        vector.append(moment(res[0], moment=2))
        vector.append(moment(res[0], moment=3))
        vector.append(moment(res[0], moment=4))
        vector.append(min(res[1]))
        vector.append(max(res[1]))
        vector.append(sum(res[1]))
        vector.append(statistics.median(res[1]))
        vector.append(statistics.mean(res[1]))
        vector.append(moment(res[1], moment=2))
        vector.append(moment(res[1], moment=3))
        vector.append(moment(res[1], moment=4))
        vector.append(min(res[2]))
        vector.append(max(res[2]))
        vector.append(sum(res[2]))
        vector.append(statistics.median(res[2]))
        vector.append(statistics.mean(res[2]))
        vector.append(moment(res[2], moment=2))
        vector.append(moment(res[2], moment=3))
        vector.append(moment(res[2], moment=4))
        vector.append(min(res[3]))
        vector.append(max(res[3]))
        vector.append(sum(res[3]))
        vector.append(statistics.median(res[3]))
        vector.append(statistics.mean(res[3]))
        vector.append(moment(res[3], moment=2))
        vector.append(moment(res[3], moment=3))
        vector.append(moment(res[3], moment=4))
        vector.append(min(res[4]))
        vector.append(max(res[4]))
        vector.append(sum(res[4]))
        vector.append(statistics.median(res[4]))
        vector.append(statistics.mean(res[4]))
        vector.append(moment(res[4], moment=2))
        vector.append(moment(res[4], moment=3))
        vector.append(moment(res[4], moment=4))
        vector.append(min(res[5]))
        vector.append(max(res[5]))
        vector.append(sum(res[5]))
        vector.append(statistics.median(res[5]))
        vector.append(statistics.mean(res[5]))
        vector.append(moment(res[5], moment=2))
        vector.append(moment(res[5], moment=3))
        vector.append(moment(res[5], moment=4))
        vector.append(min(res[6]))
        vector.append(max(res[6]))
        vector.append(sum(res[6]))
        vector.append(statistics.median(res[6]))
        vector.append(statistics.mean(res[6]))
        vector.append(moment(res[6], moment=2))
        vector.append(moment(res[6], moment=3))
        vector.append(moment(res[6], moment=4))
        vector.append(min(res[7]))
        vector.append(max(res[7]))
        vector.append(sum(res[7]))
        vector.append(statistics.median(res[7]))
        vector.append(statistics.mean(res[7]))
        vector.append(moment(res[7], moment=2))
        vector.append(moment(res[7], moment=3))
        vector.append(moment(res[7], moment=4))
        vector.append(min(res[8]))
        vector.append(max(res[8]))
        vector.append(sum(res[8]))
        vector.append(statistics.median(res[8]))
        vector.append(statistics.mean(res[8]))
        vector.append(moment(res[8], moment=2))
        vector.append(moment(res[8], moment=3))
        vector.append(moment(res[8], moment=4))
        vector.append(min(res[9]))
        vector.append(max(res[9]))
        vector.append(sum(res[9]))
        vector.append(statistics.median(res[9]))
        vector.append(statistics.mean(res[9]))
        vector.append(moment(res[9], moment=2))
        vector.append(moment(res[9], moment=3))
        vector.append(moment(res[9], moment=4))
        vector.append(min(res[10]))
        vector.append(max(res[10]))
        vector.append(sum(res[10]))
        vector.append(statistics.median(res[10]))
        vector.append(statistics.mean(res[10]))
        vector.append(moment(res[10], moment=2))
        vector.append(moment(res[10], moment=3))
        vector.append(moment(res[10], moment=4))
        vector.append(min(res[11]))
        vector.append(max(res[11]))
        vector.append(sum(res[11]))
        vector.append(statistics.median(res[11]))
        vector.append(statistics.mean(res[11]))
        vector.append(moment(res[11], moment=2))
        vector.append(moment(res[11], moment=3))
        vector.append(moment(res[11], moment=4))
        vector.append(min(res[12]))
        vector.append(max(res[12]))
        vector.append(sum(res[12]))
        vector.append(statistics.median(res[12]))
        vector.append(statistics.mean(res[12]))
        vector.append(moment(res[12], moment=2))
        vector.append(moment(res[12], moment=3))
        vector.append(moment(res[12], moment=4))
        vector.append(min(res[13]))
        vector.append(max(res[13]))
        vector.append(sum(res[13]))
        vector.append(statistics.median(res[13]))
        vector.append(statistics.mean(res[13]))
        vector.append(moment(res[13], moment=2))
        vector.append(moment(res[13], moment=3))
        vector.append(moment(res[13], moment=4))
        vector.append(min(res[14]))
        vector.append(max(res[14]))
        vector.append(sum(res[14]))
        vector.append(statistics.median(res[14]))
        vector.append(statistics.mean(res[14]))
        vector.append(moment(res[14], moment=2))
        vector.append(moment(res[14], moment=3))
        vector.append(moment(res[14], moment=4))
        vector.append(min(res[15]))
        vector.append(max(res[15]))
        vector.append(sum(res[15]))
        vector.append(statistics.median(res[15]))
        vector.append(statistics.mean(res[15]))
        vector.append(moment(res[15], moment=2))
        vector.append(moment(res[15], moment=3))
        vector.append(moment(res[15], moment=4))
    else:
        vector.extend([0]*128)

    return vector


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = prepare_conn(FLAGS.index, FLAGS.core, 
                             FLAGS.util, FLAGS.service)
    initialize_database(conn, cur)

    # Multiprocessing
    cur.execute('''SELECT MAX(Feature.addr)
                   FROM Feature;''')
    try:
        start_addrid = cur.fetchone()[0] + 1
    except sqlite3.Error:
        start_addid = 1
    cur.execute('''SELECT MAX(DBINDEX.AddrID.id)
                   FROM DBINDEX.AddrID;''')
    end_addrid = cur.fetchone()[0] + 1
    cur.execute('''BEGIN TRANSACTION;''')
    if DEBUG:
        print(f'From {start_addrid} To {end_addrid}')
    for addr in range(start_addrid, end_addrid):
        result = get_feature(conn, cur, addr)
        cur.execute('''INSERT OR IGNORE INTO Feature (
                       addr, updatetime,
                       cnttx, cnttxin, cnttxout,
                       btc, btcin, btcout,
                       cntuse, cntusein, cntuseout,
                       age, agein, ageout,
                       addrtypep2pkh, addrtypep2sh, addrtypebech32, addrtypeother) VALUES (
                       ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?, ?);''', (result['addr'], result['updatetime'],
                                         result['cnttx'], result['cnttxin'], result['cnttxout'],
                                         result['btc'], result['btcin'], result['btcout'],
                                         result['cntuse'], result['cntusein'], result['cntuseout'],
                                         result['age'], result['agein'], result['ageout'],
                                         result['addrtypep2pkh'], result['addrtypep2sh'], result['addrtypebech32'], result['addrtypeother']))
        if addr % 100000 == 0:
            cur.execute('''COMMIT TRANSACTION;''')
            cur.execute('''BEGIN TRANSACTION;''')
            conn.commit()
            if DEBUG:
                print(f'[{int(time.time()-STIME)}] {addr} / {end_addrid} ({addr/end_addrid:.2f}) Done')
    cur.execute('''COMMIT TRANSACTION;''')
    conn.commit()

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
    parser.add_argument('--target', type=str, required=True,
                        help='The target csv file include "Address" field')
    parser.add_argument('--output', type=str,
                        help='The feature dataframe output')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))

    DEBUG = FLAGS.debug

    main()

