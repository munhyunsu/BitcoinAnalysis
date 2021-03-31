import copy
import os
import sqlite3
import datetime
import time

TZ_UTC = datetime.timezone(datetime.timedelta())
FLAGS = _ = None
DEBUG = False
STIME = None


def get_time(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=TZ_UTC)


def prepare_conn(indexpath, corepath, utilpath, servicepath):
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
    cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
    cur.execute(f'''ATTACH DATABASE '{utilpath}' AS DBUTIL;''')
    cur.execute(f'''ATTACH DATABASE '{servicepath}' AS DBSERVICE;''')
    conn.commit()
    
    return conn, cur


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = prepare_conn(FLAGS.index, FLAGS.core, FLAGS.util, FLAGS.service)

    cur.execute('''SELECT DBINDEX.AddrID.id
                   FROM DBINDEX.AddrID
                   WHERE DBINDEX.AddrID.addr = ?;''', (FLAGS.target,))
    try:
        address_id = cur.fetchone()[0]
    except:
        print(f'Can not find address in our database')
        return

    result = {address_id}
    next_target = copy.deepcopy(result)
    while True:
        temp_result = set()
        for addr in next_target:
            for row in cur.execute('''SELECT DBCORE.TxOut.addr AS addr
                                      FROM DBCORE.TxIn
                                      INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                                      WHERE DBCORE.txIn.tx IN (
                                        SELECT DBCORE.TxIn.tx
                                        FROM DBCORE.TxIn
                                        INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                                        WHERE DBCORE.TxOut.addr = ?)
                                      GROUP BY DBCORE.TxOut.addr;''', (addr,)):
                if row[0] in result and row[0] not in temp_result:
                    continue
                temp_result.add(row[0])
        next_target = temp_result
        result = result | next_target # Union
        if len(next_target) == 0:
            break

    next_target = copy.deepcopy(result)
    temp_result = set()
    for addr in next_target:
        for row in cur.execute('''SELECT DBCORE.TxOut.addr
                                  INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND DBCORE.TxOut.n = DBCORE.TxIn.pn
                                  WHERE DBCORE.TxIn.tx IN (
                                    SELECT DBCORE.TxIn.tx
                                    FROM DBCORE.TxIn
                                    INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.tx
                                    WHERE DBCORE.TxIn.tx IN (
                                      SELECT DBCORE.TxOut.tx
                                      FROM DBCORE.TxOut
                                      WHERE DBCORE.TxOut.addr = ?)
                                    GROUP BY DBCORE.TxIn.tx
                                    HAVING COUNT(DISTINCT DBCORE.TxIn.n) > 1 AND COUNT(DISTINCT DBCORE.TxOut.n) = 1)
                                  GROUP BY DBCORE.TxOut.addr;''', (addr,)):
            if row[0] in result and row[0] not in temp_result:
                continue
            temp_result.add(row[0])
    next_target = temp_result
    result = result | next_target # Union

    print('address, txcount, btcinsum, btcoutsum, balance, firstdatetime, lastdatetime')
    for addr in result:
        cur.execute('''SELECT DBINDEX.AddrID.addr
                       FROM DBINDEX.AddrID
                       WHERE DBINDEX.AddrID.id = ?;''', (addr,))
        address = cur.fetchone()[0]
        cur.execute('''SELECT COUNT(DISTINCT DBCORE.TxIn.tx)
                       FROM (
                         SELECT DBCORE.TxIn.tx
                         FROM DBCORE.TxIn
                         INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx
                                                AND DBCORE.TxOut.n = DBCORE.TxIn.pn
                         WHERE DBCORE.TxOut.addr = ?
                         UNION
                         SELECT DBCORE.TxOut.tx
                         FROM DBCORE.TxOut
                         WHERE DBCORE.TxOut.addr = ?);''', (addr, addr))
        txcount = cur.fetchone()[0]
        cur.execute('''SELECT SUM(DBCORE.TxOut.btc)
                       FROM DBCORE.TxIn 
                       INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx
                                              AND DBCORE.TxOut.n = DBCORE.TxIn.pn
                       WHERE DBCORE.TxOut.addr = ?;''', (addr,))
        btcinsum = cur.fetchone()[0]
        cur.execute('''SELECT SUM(DBCORE.TxOut.btc)
                       FROM DBCORE.TxOut
                       WHERE DBCORE.TxOut.addr = ?;''', (addr,))
        btcoutsum = cur.fetchone()[0]
        cur.execute('''SELECT Income.value-Outcome.value AS Balance
                       FROM (
                         SELECT SUM(DBCORE.TxOut.btc) AS value
                         FROM DBCORE.TxOut
                         WHERE DBCORE.TxOut.addr = ?) AS Income, (
                         SELECT SUM(DBCORE.TxOut.btc) AS value
                         FROM DBCORE.TxIn
                         INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                                AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                         WHERE DBCORE.TxOut.addr = ?) AS Outcome;''', (addr, addr))
        balance = cur.fetchone()[0]
        cur.execute('''SELECT MIN(DBCORE.BlkTime.unixtime)
                       FROM DBCORE.BlkTime
                       INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
                       WHERE DBCORE.BlkTx.tx IN (
                         SELECT tx
                         FROM (
                           SELECT DISTINCT DBCORE.TxIn.tx
                           FROM DBCORE.TxIn
                           INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx
                                                  AND DBCORE.TxOut.n = DBCORE.TxIn.pn
                           WHERE DBCORE.TxOut.addr = ?
                           UNION 
                           SELECT DISTINCT DBCORE.TxOut.tx
                           FROM DBCORE.TxOut
                           WHERE DBCORE.TxOut.addr = ?));''', (addr, addr))
        firstdatetime = get_time(cur.fetchone()[0])
        cur.execute('''SELECT MAX(DBCORE.BlkTime.unixtime)
                       FROM DBCORE.BlkTime
                       INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
                       WHERE DBCORE.BlkTx.tx IN (
                         SELECT tx
                         FROM (
                           SELECT DISTINCT DBCORE.TxIn.tx
                           FROM DBCORE.TxIn
                           INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx
                                                  AND DBCORE.TxOut.n = DBCORE.TxIn.pn
                           WHERE DBCORE.TxOut.addr = ?
                           UNION 
                           SELECT DISTINCT DBCORE.TxOut.tx
                           FROM DBCORE.TxOut
                           WHERE DBCORE.TxOut.addr = ?));''', (addr, addr))
        lastdatetime = get_time(cur.fetchone()[0])
        print(f'{address}, {txcount}, {btcinsum}, {btcoutsum}, {balance}, {firstdatetime}, {lastdatetime}')


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
                        help='The path for util database')
    parser.add_argument('--target', type=str, required=True,
                        help='Target address')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))

    DEBUG = FLAGS.debug

    STIME = time.time()
    main()
