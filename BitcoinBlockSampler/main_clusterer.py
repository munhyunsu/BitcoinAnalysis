import os
import time
import sqlite3
import collections

from db_manager import QUERY, DBReader

FLAGS = None
_ = None
DEBUG = False
INDEX = None
CORE = None
CLUSTER = None
CONN = None
CUR = None


def initialize_cluster():
    global FLAGS
    global DEBUG
    global INDEX
    global CORE
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialize Cluster Database')
    conn = sqlite3.connect(FLAGS.cluster)
    cur = conn.cursor()
    cur.execute('''PRAGMA journal_mode = OFF''')
    cur.execute('''PRAGMA synchronous = OFF''')
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
    INDEX.cur.execute('''SELECT MAX(id) FROM AddrID;''')
    cur.execute('BEGIN TRANSACTION')
    for i in range(1, INDEX.cur.fetchone()[0]+1):
        cur.execute('''INSERT INTO Cluster (addr, cluster)
                           VALUES (?, ?);''', (i, -1))
    cur.execute('COMMIT TRANSACTION')
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Bulk Insert Into Cluster Database Complete')
    cur.execute('''CREATE INDEX idx_Cluster_2 ON Cluster(cluster);''')
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] CREATE Index Complete')
    cur.execute('''PRAGMA journal_mode = NORMAL''')
    cur.execute('''PRAGMA synchronous = WAL''')
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Initialize Cluster Database Complete')
    conn.commit()


def get_addrid_multiinput(addrid, clustered, queue):
    global CORE
    for new_addrid in CORE.selectcur('SELECT_MULTIINPUT', (addrid,)):
        new_addrid = new_addrid[0]
        if new_addrid not in clustered:
            clustered.add(new_addrid)
            queue.append(new_addrid)


def get_addrid_singleoutput(addrid, clustered, queue):
    global CORE
    for new_addrid in CORE.selectcur('SELECT_SINGLEOUTPUT', (addrid,)):
        new_addrid = new_addrid[0]
        if new_addrid not in clustered:
            clustered.add(new_addrid)
            queue.append(new_addrid)


def get_next_clusterid(addrid):
    global CONN
    global CUR
    CUR.execute('''SELECT (CASE WHEN cluster != -1
                                   THEN cluster
                                   ELSE (SELECT MAX(cluster)+1 FROM Cluster)
                                   END) as cluster_num
                           FROM Cluster 
                           WHERE addr = ?;''', (addrid,))
    return CUR.fetchone()[0]

def get_samecluster_addrs(addrid):
    global CONN
    global CUR

    CUR.execute('''SELECT cluster
                   FROM Cluster
                   WHERE addr = ?;''', (addrid,))
    ccid = CUR.fetchone()[0]
    if ccid == -1:
        return list(addrid)

    result = list()
    CUR.execute('''SELECT DISTINCT addr
                   FROM Cluster
                   WHERE cluster = ?;''', (ccid,))
    for new_addr in CUR:
        result.append(new_addr[0])
    return result
        

def insert_tag(addrid):
    global CONN
    global CUR
    CUR.execute('''INSERT OR IGNORE INTO TagID (tag) 
                     VALUES (?);''', (FLAGS.tag,))
    CONN.commit()
    CUR.execute('''INSERT OR IGNORE INTO Tag (addr, tag) 
                     VALUES (?, (SELECT id 
                                 FROM TagID 
                                 WHERE tag = ?));''', (addrid, FLAGS.tag))
    CONN.commit()


def main():
    global FLAGS
    global STIME
    global INDEX
    global CORE
    global CONN
    global CUR
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    STIME = time.time()
    INDEX = db_index = DBReader(FLAGS.index)
    CORE = db_core = DBReader(FLAGS.core)

    if not os.path.exists(FLAGS.cluster):
        initialize_cluster()
    CONN = conn = sqlite3.connect(FLAGS.cluster)
    CUR = cur = conn.cursor()
    CUR.execute(f'ATTACH DATABASE "{os.path.basename(FLAGS.index)}" AS DBINDEX;')
    CUR.execute(f'ATTACH DATABASE "{os.path.basename(FLAGS.core)}" AS DBCORE;')
    
    addr_id = INDEX.select('SELECT_ADDRID', (FLAGS.seed,))
    cluster_id = get_next_clusterid(addr_id)
    clustered = set()
    if FLAGS.resume:
        queue = collections.deque(get_samecluster_addrs(addr_id))
    else:
        queue = collections.deque([addr_id])
    
    if FLAGS.tag is not None:
        insert_tag(addr_id)
    
    cnt = 0
    while (FLAGS.loop != 0 and FLAGS.loop > cnt) or (FLAGS.loop == 0 and len(queue) > 0):
        new_queue = collections.deque()
        while queue:
            target = queue.popleft()
            if FLAGS.heuristic == 'multiinput':
                get_addrid_multiinput(target, clustered, new_queue)
            elif FLAGS.heuristic == 'singleoutput':
                get_addrid_singleoutput(target, clustered, new_queue)
        CUR.execute('BEGIN TRANSACTION')
        CUR.executemany('''UPDATE Cluster SET Cluster = ? WHERE addr = ?''',
                           [(cluster_id, new_addr) for new_addr in new_queue])
        CUR.execute('COMMIT TRANSACTION')
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Finded {len(new_queue)}')
        queue.extend(new_queue)
        cnt = cnt + 1
    CUR.execute('''SELECT COUNT(*)
                   FROM Cluster
                   WHERE cluster = ?''', (cluster_id,))
    print(f'[{int(time.time()-STIME)}] Clustered {CUR.fetchone()[0]} Addresses which tagged {FLAGS.tag}')

    INDEX.close()
    CORE.close()
    CONN.close()


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
    parser.add_argument('--cluster', type=str, default='./cluster.db',
                        help='The path for cluster database')
    parser.add_argument('--seed', type=str, required=True,
                        help='The seed bitcoin address')
    parser.add_argument('--heuristic', type=str, required=True,
                        choices=('multiinput', 'singleoutput'),
                        help='Bitcoin address heuristic')
    parser.add_argument('--loop', type=int, default=0,
                        help='The number of lopp')
    parser.add_argument('--tag', type=str,
                        help='The bitcoin address tag')
    parser.add_argument('--resume', action='store_true',
                        help='Resume or not')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.cluster = os.path.abspath(os.path.expanduser(FLAGS.cluster))

    DEBUG = FLAGS.debug

    main()
