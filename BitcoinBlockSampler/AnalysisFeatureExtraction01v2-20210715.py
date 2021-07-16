import csv
import sqlite3
import time

STIME = time.time()

conn = sqlite3.connect(':memory:')
cur = conn.cursor()
cur.execute('''ATTACH DATABASE './dbv3-index.db' AS DBINDEX;''')
cur.execute('''ATTACH DATABASE './dbv3-core.db' AS DBCORE;''')
cur.execute('''ATTACH DATABASE './dbv3-util.db' AS DBUTIL;''')
cur.execute('''ATTACH DATABASE './dbv3-service.db' AS DBSERVICE;''')
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
    return result, a, b, c

print('ClusterName, Category, RootAddress, Associate, Multiinput, In, Out')
with open('data/named_added.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        count = 0
        cur.execute('''SELECT DBINDEX.AddrID.id
                       FROM DBINDEX.AddrID
                       WHERE DBINDEX.AddrID.addr = ?''', (row['RootAddress'],))
        addrid = cur.fetchone()[0]
        r, a, b, c = get_associate_addr(conn, cur, addrid)
        print(f'{row["ClusterName"]}, {row["Category"]}, {row["RootAddress"]}, {len(r)}, {len(a)}, {len(b)}, {len(c)}')
