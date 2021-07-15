import csv
import sqlite3
import time

STIME = time.time()

conn = sqlite3.connect('./dbv3-service.db')
cur = conn.cursor()
cur.execute('''ATTACH DATABASE './dbv3-index.db' AS DBINDEX;''')
cur.execute('''ATTACH DATABASE './dbv3-core.db' AS DBCORE;''')
cur.execute('''ATTACH DATABASE './dbv3-util.db' AS DBUTIL;''')
conn.commit()

cur.execute('''PRAGMA journal_mode = NORMAL''')
cur.execute('''PRAGMA synchronous = WAL''')
cur.execute('''DROP TABLE IF EXISTS AddrList;''')
cur.execute('''CREATE TABLE IF NOT EXISTS AddrList (
                 addr INTEGER PRIMARY KEY
               );''')
conn.commit()

def get_associate_addr(conn, cur, addr):
    # target
    result = {addr}
    # multiinput
    for row in cur.execute('''SELECT Cluster.addr
                              FROM Cluster
                              WHERE Cluster.cluster IN (
                                SELECT Cluster.cluster
                                FROM Cluster
                                WHERE Cluster.addr = ?);''', (addr,)):
        result.add(row[0])
    for row in cur.execute('''SELECT DBUTIL.Edge.src
                              FROM DBUTIL.Edge
                              WHERE DBUTIL.Edge.dst = ?;''', (addr,)):
        result.add(row[0])
    for row in cur.execute('''SELECT DBUTIL.Edge.dst
                              FROM DBUTIL.Edge
                              WHERE DBUTIL.Edge.src = ?;''', (addr,)):
        result.add(row[0])
    return result

with open('data/named_added.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        count = 0
        cur.execute('BEGIN TRANSACTION')
        cur.execute('''SELECT DBINDEX.AddrID.id
                       FROM DBINDEX.AddrID
                       WHERE DBINDEX.AddrID.addr = ?''', (row['RootAddress'],))
        addrid = cur.fetchone()[0]
        for addr in get_associate_addr(conn, cur, addrid):
            count = count + 1
            cur.execute('''INSERT OR IGNORE INTO AddrList (
                             addr) VALUES (
                             ?);''', (addr,))
        cur.execute('COMMIT TRANSACTION')
        conn.commit()
        print(f'[{int(time.time()-STIME)}] {count} added from {row["RootAddress"]}({addrid})')

