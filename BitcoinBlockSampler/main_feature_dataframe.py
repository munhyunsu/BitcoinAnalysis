import os
import sqlite3
import time
import datetime
import statistics

import numpy as np
import pandas as pd
from scipy.stats import moment

FLAGS = _ = None
DEBUG = False
STIME = time.time()
FEATURES = ['AddressID',
            'CntTx', 'CntTxIn', 'CntTxOut', 'BTC', 'BTCIn', 'BTCOut',
            'CntUse', 'CntUseIn', 'CntUseOut', 'Age', 'AgeIn', 'AgeOut',
            'AddrTypeP2PKH', 'AddrTypeP2SH', 'AddrTypeBech32', 'AddrTypeOther',
            'MI_CntTx_MIN', 'MI_CntTx_MAX', 'MI_CntTx_SUM', 'MI_CntTx_MEDIAN',
            'MI_CntTx_M1', 'MI_CntTx_M2', 'MI_CntTx_M3', 'MI_CntTx_M4',
            'MI_CntTxIn_MIN', 'MI_CntTxIn_MAX', 'MI_CntTxIn_SUM', 'MI_CntTxIn_MEDIAN',
            'MI_CntTxIn_M1', 'MI_CntTxIn_M2', 'MI_CntTxIn_M3', 'MI_CntTxIn_M4',
            'MI_CntTxOut_MIN', 'MI_CntTxOut_MAX', 'MI_CntTxOut_SUM', 'MI_CntTxOut_MEDIAN',
            'MI_CntTxOut_M1', 'MI_CntTxOut_M2', 'MI_CntTxOut_M3', 'MI_CntTxOut_M4',
            'MI_BTC_MIN', 'MI_BTC_MAX', 'MI_BTC_SUM', 'MI_BTC_MEDIAN',
            'MI_BTC_M1', 'MI_BTC_M2', 'MI_BTC_M3', 'MI_BTC_M4',
            'MI_BTCIn_MIN', 'MI_BTCIn_MAX', 'MI_BTCIn_SUM', 'MI_BTCIn_MEDIAN',
            'MI_BTCIn_M1', 'MI_BTCIn_M2', 'MI_BTCIn_M3', 'MI_BTCIn_M4',
            'MI_BTCOut_MIN', 'MI_BTCOut_MAX', 'MI_BTCOut_SUM', 'MI_BTCOut_MEDIAN',
            'MI_BTCOut_M1', 'MI_BTCOut_M2', 'MI_BTCOut_M3', 'MI_BTCOut_M4',
            'MI_CntUse_MIN', 'MI_CntUse_MAX', 'MI_CntUse_SUM', 'MI_CntUse_MEDIAN',
            'MI_CntUse_M1', 'MI_CntUse_M2', 'MI_CntUse_M3', 'MI_CntUse_M4',
            'MI_CntUseIn_MIN', 'MI_CntUseIn_MAX', 'MI_CntUseIn_SUM', 'MI_CntUseIn_MEDIAN',
            'MI_CntUseIn_M1', 'MI_CntUseIn_M2', 'MI_CntUseIn_M3', 'MI_CntUseIn_M4',
            'MI_CntUseOut_MIN', 'MI_CntUseOut_MAX', 'MI_CntUseOut_SUM', 'MI_CntUseOut_MEDIAN',
            'MI_CntUseOut_M1', 'MI_CntUseOut_M2', 'MI_CntUseOut_M3', 'MI_CntUseOut_M4',
            'MI_Age_MIN', 'MI_Age_MAX', 'MI_Age_SUM', 'MI_Age_MEDIAN',
            'MI_Age_M1', 'MI_Age_M2', 'MI_Age_M3', 'MI_Age_M4',
            'MI_AgeIn_MIN', 'MI_AgeIn_MAX', 'MI_AgeIn_SUM', 'MI_AgeIn_MEDIAN',
            'MI_AgeIn_M1', 'MI_AgeIn_M2', 'MI_AgeIn_M3', 'MI_AgeIn_M4',
            'MI_AgeOut_MIN', 'MI_AgeOut_MAX', 'MI_AgeOut_SUM', 'MI_AgeOut_MEDIAN',
            'MI_AgeOut_M1', 'MI_AgeOut_M2', 'MI_AgeOut_M3', 'MI_AgeOut_M4',
            'MI_AddrTypeP2PKH_MIN', 'MI_AddrTypeP2PKH_MAX', 'MI_AddrTypeP2PKH_SUM', 'MI_AddrTypeP2PKH_MEDIAN',
            'MI_AddrTypeP2PKH_M1', 'MI_AddrTypeP2PKH_M2', 'MI_AddrTypeP2PKH_M3', 'MI_AddrTypeP2PKH_M4',
            'MI_AddrTypeP2SH_MIN', 'MI_AddrTypeP2SH_MAX', 'MI_AddrTypeP2SH_SUM', 'MI_AddrTypeP2SH_MEDIAN',
            'MI_AddrTypeP2SH_M1', 'MI_AddrTypeP2SH_M2', 'MI_AddrTypeP2SH_M3', 'MI_AddrTypeP2SH_M4',
            'MI_AddrTypeBech32_MIN', 'MI_AddrTypeBech32_MAX', 'MI_AddrTypeBech32_SUM', 'MI_AddrTypeBech32_MEDIAN',
            'MI_AddrTypeBech32_M1', 'MI_AddrTypeBech32_M2', 'MI_AddrTypeBech32_M3', 'MI_AddrTypeBech32_M4',
            'MI_AddrTypeOther_MIN', 'MI_AddrTypeOther_MAX', 'MI_AddrTypeOther_SUM', 'MI_AddrTypeOther_MEDIAN',
            'MI_AddrTypeOther_M1', 'MI_AddrTypeOther_M2', 'MI_AddrTypeOther_M3', 'MI_AddrTypeOther_M4',
            'IN1_CntTx_MIN', 'IN1_CntTx_MAX', 'IN1_CntTx_SUM', 'IN1_CntTx_MEDIAN',
            'IN1_CntTx_M1', 'IN1_CntTx_M2', 'IN1_CntTx_M3', 'IN1_CntTx_M4',
            'IN1_CntTxIn_MIN', 'IN1_CntTxIn_MAX', 'IN1_CntTxIn_SUM', 'IN1_CntTxIn_MEDIAN',
            'IN1_CntTxIn_M1', 'IN1_CntTxIn_M2', 'IN1_CntTxIn_M3', 'IN1_CntTxIn_M4',
            'IN1_CntTxOut_MIN', 'IN1_CntTxOut_MAX', 'IN1_CntTxOut_SUM', 'IN1_CntTxOut_MEDIAN',
            'IN1_CntTxOut_M1', 'IN1_CntTxOut_M2', 'IN1_CntTxOut_M3', 'IN1_CntTxOut_M4',
            'IN1_BTC_MIN', 'IN1_BTC_MAX', 'IN1_BTC_SUM', 'IN1_BTC_MEDIAN',
            'IN1_BTC_M1', 'IN1_BTC_M2', 'IN1_BTC_M3', 'IN1_BTC_M4',
            'IN1_BTCIn_MIN', 'IN1_BTCIn_MAX', 'IN1_BTCIn_SUM', 'IN1_BTCIn_MEDIAN',
            'IN1_BTCIn_M1', 'IN1_BTCIn_M2', 'IN1_BTCIn_M3', 'IN1_BTCIn_M4',
            'IN1_BTCOut_MIN', 'IN1_BTCOut_MAX', 'IN1_BTCOut_SUM', 'IN1_BTCOut_MEDIAN',
            'IN1_BTCOut_M1', 'IN1_BTCOut_M2', 'IN1_BTCOut_M3', 'IN1_BTCOut_M4',
            'IN1_CntUse_MIN', 'IN1_CntUse_MAX', 'IN1_CntUse_SUM', 'IN1_CntUse_MEDIAN',
            'IN1_CntUse_M1', 'IN1_CntUse_M2', 'IN1_CntUse_M3', 'IN1_CntUse_M4',
            'IN1_CntUseIn_MIN', 'IN1_CntUseIn_MAX', 'IN1_CntUseIn_SUM', 'IN1_CntUseIn_MEDIAN',
            'IN1_CntUseIn_M1', 'IN1_CntUseIn_M2', 'IN1_CntUseIn_M3', 'IN1_CntUseIn_M4',
            'IN1_CntUseOut_MIN', 'IN1_CntUseOut_MAX', 'IN1_CntUseOut_SUM', 'IN1_CntUseOut_MEDIAN',
            'IN1_CntUseOut_M1', 'IN1_CntUseOut_M2', 'IN1_CntUseOut_M3', 'IN1_CntUseOut_M4',
            'IN1_Age_MIN', 'IN1_Age_MAX', 'IN1_Age_SUM', 'IN1_Age_MEDIAN',
            'IN1_Age_M1', 'IN1_Age_M2', 'IN1_Age_M3', 'IN1_Age_M4',
            'IN1_AgeIn_MIN', 'IN1_AgeIn_MAX', 'IN1_AgeIn_SUM', 'IN1_AgeIn_MEDIAN',
            'IN1_AgeIn_M1', 'IN1_AgeIn_M2', 'IN1_AgeIn_M3', 'IN1_AgeIn_M4',
            'IN1_AgeOut_MIN', 'IN1_AgeOut_MAX', 'IN1_AgeOut_SUM', 'IN1_AgeOut_MEDIAN',
            'IN1_AgeOut_M1', 'IN1_AgeOut_M2', 'IN1_AgeOut_M3', 'IN1_AgeOut_M4',
            'IN1_AddrTypeP2PKH_MIN', 'IN1_AddrTypeP2PKH_MAX', 'IN1_AddrTypeP2PKH_SUM', 'IN1_AddrTypeP2PKH_MEDIAN',
            'IN1_AddrTypeP2PKH_M1', 'IN1_AddrTypeP2PKH_M2', 'IN1_AddrTypeP2PKH_M3', 'IN1_AddrTypeP2PKH_M4',
            'IN1_AddrTypeP2SH_MIN', 'IN1_AddrTypeP2SH_MAX', 'IN1_AddrTypeP2SH_SUM', 'IN1_AddrTypeP2SH_MEDIAN',
            'IN1_AddrTypeP2SH_M1', 'IN1_AddrTypeP2SH_M2', 'IN1_AddrTypeP2SH_M3', 'IN1_AddrTypeP2SH_M4',
            'IN1_AddrTypeBech32_MIN', 'IN1_AddrTypeBech32_MAX', 'IN1_AddrTypeBech32_SUM', 'IN1_AddrTypeBech32_MEDIAN',
            'IN1_AddrTypeBech32_M1', 'IN1_AddrTypeBech32_M2', 'IN1_AddrTypeBech32_M3', 'IN1_AddrTypeBech32_M4',
            'IN1_AddrTypeOther_MIN', 'IN1_AddrTypeOther_MAX', 'IN1_AddrTypeOther_SUM', 'IN1_AddrTypeOther_MEDIAN',
            'IN1_AddrTypeOther_M1', 'IN1_AddrTypeOther_M2', 'IN1_AddrTypeOther_M3', 'IN1_AddrTypeOther_M4',
            'OUT1_CntTx_MIN', 'OUT1_CntTx_MAX', 'OUT1_CntTx_SUM', 'OUT1_CntTx_MEDIAN',
            'OUT1_CntTx_M1', 'OUT1_CntTx_M2', 'OUT1_CntTx_M3', 'OUT1_CntTx_M4',
            'OUT1_CntTxIn_MIN', 'OUT1_CntTxIn_MAX', 'OUT1_CntTxIn_SUM', 'OUT1_CntTxIn_MEDIAN',
            'OUT1_CntTxIn_M1', 'OUT1_CntTxIn_M2', 'OUT1_CntTxIn_M3', 'OUT1_CntTxIn_M4',
            'OUT1_CntTxOut_MIN', 'OUT1_CntTxOut_MAX', 'OUT1_CntTxOut_SUM', 'OUT1_CntTxOut_MEDIAN',
            'OUT1_CntTxOut_M1', 'OUT1_CntTxOut_M2', 'OUT1_CntTxOut_M3', 'OUT1_CntTxOut_M4',
            'OUT1_BTC_MIN', 'OUT1_BTC_MAX', 'OUT1_BTC_SUM', 'OUT1_BTC_MEDIAN',
            'OUT1_BTC_M1', 'OUT1_BTC_M2', 'OUT1_BTC_M3', 'OUT1_BTC_M4',
            'OUT1_BTCIn_MIN', 'OUT1_BTCIn_MAX', 'OUT1_BTCIn_SUM', 'OUT1_BTCIn_MEDIAN',
            'OUT1_BTCIn_M1', 'OUT1_BTCIn_M2', 'OUT1_BTCIn_M3', 'OUT1_BTCIn_M4',
            'OUT1_BTCOut_MIN', 'OUT1_BTCOut_MAX', 'OUT1_BTCOut_SUM', 'OUT1_BTCOut_MEDIAN',
            'OUT1_BTCOut_M1', 'OUT1_BTCOut_M2', 'OUT1_BTCOut_M3', 'OUT1_BTCOut_M4',
            'OUT1_CntUse_MIN', 'OUT1_CntUse_MAX', 'OUT1_CntUse_SUM', 'OUT1_CntUse_MEDIAN',
            'OUT1_CntUse_M1', 'OUT1_CntUse_M2', 'OUT1_CntUse_M3', 'OUT1_CntUse_M4',
            'OUT1_CntUseIn_MIN', 'OUT1_CntUseIn_MAX', 'OUT1_CntUseIn_SUM', 'OUT1_CntUseIn_MEDIAN',
            'OUT1_CntUseIn_M1', 'OUT1_CntUseIn_M2', 'OUT1_CntUseIn_M3', 'OUT1_CntUseIn_M4',
            'OUT1_CntUseOut_MIN', 'OUT1_CntUseOut_MAX', 'OUT1_CntUseOut_SUM', 'OUT1_CntUseOut_MEDIAN',
            'OUT1_CntUseOut_M1', 'OUT1_CntUseOut_M2', 'OUT1_CntUseOut_M3', 'OUT1_CntUseOut_M4',
            'OUT1_Age_MIN', 'OUT1_Age_MAX', 'OUT1_Age_SUM', 'OUT1_Age_MEDIAN',
            'OUT1_Age_M1', 'OUT1_Age_M2', 'OUT1_Age_M3', 'OUT1_Age_M4',
            'OUT1_AgeIn_MIN', 'OUT1_AgeIn_MAX', 'OUT1_AgeIn_SUM', 'OUT1_AgeIn_MEDIAN',
            'OUT1_AgeIn_M1', 'OUT1_AgeIn_M2', 'OUT1_AgeIn_M3', 'OUT1_AgeIn_M4',
            'OUT1_AgeOut_MIN', 'OUT1_AgeOut_MAX', 'OUT1_AgeOut_SUM', 'OUT1_AgeOut_MEDIAN',
            'OUT1_AgeOut_M1', 'OUT1_AgeOut_M2', 'OUT1_AgeOut_M3', 'OUT1_AgeOut_M4',
            'OUT1_AddrTypeP2PKH_MIN', 'OUT1_AddrTypeP2PKH_MAX', 'OUT1_AddrTypeP2PKH_SUM', 'OUT1_AddrTypeP2PKH_MEDIAN',
            'OUT1_AddrTypeP2PKH_M1', 'OUT1_AddrTypeP2PKH_M2', 'OUT1_AddrTypeP2PKH_M3', 'OUT1_AddrTypeP2PKH_M4',
            'OUT1_AddrTypeP2SH_MIN', 'OUT1_AddrTypeP2SH_MAX', 'OUT1_AddrTypeP2SH_SUM', 'OUT1_AddrTypeP2SH_MEDIAN',
            'OUT1_AddrTypeP2SH_M1', 'OUT1_AddrTypeP2SH_M2', 'OUT1_AddrTypeP2SH_M3', 'OUT1_AddrTypeP2SH_M4',
            'OUT1_AddrTypeBech32_MIN', 'OUT1_AddrTypeBech32_MAX', 'OUT1_AddrTypeBech32_SUM', 'OUT1_AddrTypeBech32_MEDIAN',
            'OUT1_AddrTypeBech32_M1', 'OUT1_AddrTypeBech32_M2', 'OUT1_AddrTypeBech32_M3', 'OUT1_AddrTypeBech32_M4',
            'OUT1_AddrTypeOther_MIN', 'OUT1_AddrTypeOther_MAX', 'OUT1_AddrTypeOther_SUM', 'OUT1_AddrTypeOther_MEDIAN',
            'OUT1_AddrTypeOther_M1', 'OUT1_AddrTypeOther_M2', 'OUT1_AddrTypeOther_M3', 'OUT1_AddrTypeOther_M4']


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
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialized cache database')


def get_associate_addr(conn, cur, addr):
    # target
    result = {addr}
    mi = set()
    in1 = set()
    out1 = set()
    # multiinput
    for row in cur.execute('''SELECT DBSERVICE.Cluster.addr
                              FROM DBSERVICE.Cluster
                              WHERE DBSERVICE.Cluster.cluster IN (
                                SELECT DBSERVICE.Cluster.cluster
                                FROM DBSERVICE.Cluster
                                WHERE DBSERVICE.Cluster.addr = ?);''', (addr,)):
        result.add(row[0])
        mi.add(row[0])
    for row in cur.execute('''SELECT DBUTIL.Edge.src
                              FROM DBUTIL.Edge
                              WHERE DBUTIL.Edge.dst = ?;''', (addr,)):
        result.add(row[0])
        in1.add(row[0])
    for row in cur.execute('''SELECT DBUTIL.Edge.dst
                              FROM DBUTIL.Edge
                              WHERE DBUTIL.Edge.src = ?;''', (addr,)):
        result.add(row[0])
        out1.add(row[0])
    return (result, mi, in1, out1)


def get_feature_vector(conn, cur, addrid):
    vector = [addrid]
    total, mi, in1, out1 = get_associate_addr(conn, cur, addrid)
    # target
    cur.execute('''SELECT DBSERVICE.Feature.cnttx, DBSERVICE.Feature.cnttxin, DBSERVICE.Feature.cnttxout,
                          DBSERVICE.Feature.btc, DBSERVICE.Feature.btcin, DBSERVICE.Feature.btcout,
                          DBSERVICE.Feature.cntuse, DBSERVICE.Feature.cntusein, DBSERVICE.Feature.cntuseout,
                          DBSERVICE.Feature.age, DBSERVICE.Feature.agein, DBSERVICE.Feature.ageout,
                          DBSERVICE.Feature.addrtypep2pkh, DBSERVICE.Feature.addrtypep2sh,
                          DBSERVICE.Feature.addrtypebech32, DBSERVICE.Feature.addrtypeother
                   FROM DBSERVICE.Feature
                   WHERE DBSERVICE.Feature.addr = ?;''', (addrid,))
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
    for addr in mi:
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
    for addr in in1:
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
    for addr in out1:
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

    df = pd.read_csv(FLAGS.input)
    df_len = len(df)
    columns = FEATURES

    # TODO(LuHa): More efficient method needed
    #             Manage multiinput addresses for improve
    ## Make list of targe address, address_id, and etc.
    targets = set()
    addrid_list = [] # for merge df
    for index, row in df.iterrows():
        addr = row['Address']
        cur.execute('''SELECT DBINDEX.AddrID.id FROM DBINDEX.AddrID
                       WHERE DBINDEX.AddrID.addr = ?;''', (addr,))
        addrid = cur.fetchone()[0]
        addrid_list.append(addrid)
        targets.add(addrid)
    df['AddressID'] = addrid_list
    data = []
    # Change for to while loop
    # memory efficiency calculation needed
    while len(targets) > 0:
        addrid = targets.pop() # Set has add, pop method
        vector = get_feature_vector(conn, cur, addrid)
        data.append(vector)
        if DEBUG and index%100 == 0:
            print(f'[{int(time.time()-STIME)}] {index} / {df_len} ({index/df_len:.2f}) Done')
    fdf = pd.DataFrame(data, columns=FEATURES)
    #df_output.to_pickle(FLAGS.output)

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
    parser.add_argument('--input', type=str, required=True,
                        help='The target csv file include "Address" field')
    parser.add_argument('--output', type=str,
                        help='The feature dataframe output')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
    FLAGS.input = os.path.abspath(os.path.expanduser(FLAGS.input))
    if FLAGS.output is None:
        now = datetime.datetime.now().strftime('%Y-%m-%d')
        FLAGS.output = f'{os.path.splitext(FLAGS.input)[0]}_{now}.pkl'
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))

    DEBUG = FLAGS.debug

    main()

