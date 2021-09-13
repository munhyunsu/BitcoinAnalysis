import os
import time
import sqlite3
import statistics

import numpy as np
import pandas as pd
from scipy.stats import moment

FLAGS = _ = None
DEBUG = False
STIME = time.time()
FEATURES = ['Tx',
            'TxIn', 'TxInP2PKH', 'TxInP2SH', 'TxInBech32',
            'TxInm1', 'TxInm2', 'TxInm3', 'TxInm4',
            'TxInBTC', 'TxInBTCm1', 'TxInBTCm2', 'TxInBTCm3', 'TxInBTCm4',
            'TxOut', 'TxOutP2PKH', 'TxOutP2SH', 'TxOutBech32',
            'TxOutm1', 'TxOutm2', 'TxOutm3', 'TxOutm4',
            'TxOutBTC', 'TxOutBTCm1', 'TxOutBTCm2', 'TxOutBTCm3', 'TxOutBTCm4',
           ]


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


def get_feature(conn, cur, tx):
    vector = [tx]
    adict = {'P2PKH': 0, 'P2SH': 0, 'Bech32': 0}
    abtc = []
    for res in cur.execute('''SELECT DBINDEX.AddrID.addr, DBCORE.TxOut.btc
                              FROM DBCORE.TxIn
                              INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx
                                                     AND DBCORE.TxOut.n = DBCORE.TxIn.pn
                              INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
                              WHERE DBCORE.TxIn.tx = ?;''', (tx,)):
        if result[0].startswith('1'):
            adict['P2PKH'] += 1
        elif result[0].startswith('3'):
            adict['P2SH'] += 1
        elif result[0].startswith('bc1'):
            adict['Bech32'] += 1
        abtc.append(res[1])
    vector.append(sum(adict.values()))
    vector.append(adict['P2PKH'])
    vector.append(adict['P2SH'])
    vector.append(adict['Bech32'])
    vector.append(statistics.mean(adict.values()))
    vector.append(moment(list(adict.values()), moment=2))
    vector.append(moment(list(adict.values()), moment=3))
    vector.append(moment(list(adict.values()), moment=4))
    vector.append(sum(abtc))
    vector.append(statistics.mean(abtc))
    vector.append(moment(abtc, moment=2))
    vector.append(moment(abtc, moment=3))
    vector.append(moment(abtc, moment=4))

    adict = {'P2PKH': 0, 'P2SH': 0, 'Bech32': 0}
    abtc = []
    for res in cur.execute('''SELECT DBINDEX.AddrID.addr, DBCORE.TxOut.btc
                              FROM DBCORE.TxOut
                              INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
                              WHERE DBCORE.TxOut.tx = ?;''', (tx,)):
        if result[0].startswith('1'):
            adict['P2PKH'] += 1
        elif result[0].startswith('3'):
            adict['P2SH'] += 1
        elif result[0].startswith('bc1'):
            adict['Bech32'] += 1
        abtc.append(res[1])
    vector.append(sum(adict.values()))
    vector.append(adict['P2PKH'])
    vector.append(adict['P2SH'])
    vector.append(adict['Bech32'])
    vector.append(statistics.mean(adict.values()))
    vector.append(moment(list(adict.values()), moment=2))
    vector.append(moment(list(adict.values()), moment=3))
    vector.append(moment(list(adict.values()), moment=4))
    vector.append(sum(abtc))
    vector.append(statistics.mean(abtc))
    vector.append(moment(abtc, moment=2))
    vector.append(moment(abtc, moment=3))
    vector.append(moment(abtc, moment=4))

    return vector


def main():
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Parsed arguments {FLAGS}')
        print(f'[{int(time.time()-STIME)}] Unparsed arguments {_}')

    conn, cur = prepare_conn(FLAGS.index, FLAGS.core, 
                             FLAGS.util, FLAGS.service)

    # Tx
    df = pd.read_csv(FLAGS.input)
    df_len = len(df)
    columns = list(df.columns) + ['Tx'] + FEATURES

    txs = []
    for index, row in df.iterrows():
        txid = row['TxID']
        cur.execute('''SELECT DBINDEX.TxID.id
                       FROM DBINDEX.TxID
                       WHERE DBINDEX.TxID.txid = ?;''', (txid,))
        tx = cur.fetchone()[0]
        txs.append(tx)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] TxID to Tx Done!')
    df['Tx'] = txs

    data = []
    for index, row in df.iterrows():
        tx = row['Tx']
        vector = [tx] + get_feature(conn, cur, tx)
        data.append(vector)
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] {index} / {df_len} ({index/df_len:.2f}) Done!')
    fdf = pd.DataFrame(data, columns=FEATURES)

    new_df = df.merge(fdf, on='Tx')
    new_df.to_pickle(FLAGS.output)

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
                        help='The target csv file with "TxID", "Class" field')
    parser.add_argument('--output', type=str,
                        help='The feature dataframe output')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
    FLAGS.input = os.path.abspath(os.path.expanduser(FLAGS.input))
    if FLAGS.output is None:
        FLAGS.output = f'f2-dataset.pkl'
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))

    DEBUG = FLAGS.debug

    main()
