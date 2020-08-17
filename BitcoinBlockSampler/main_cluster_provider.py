import os
import sqlite3
import time
import http.server
import socketserver
import collections

from db_manager import QUERY, DBReader

FLAGS = None
_ = None
DEBUG = False
INDEX = None
CORE = None
CONN = None
CUR = None

def get_clusterid(addr):
    global FLAGS
    global DEBUG
    global INDEX
    global CORE
    global CONN
    global CUR
    global STIME

    addrid = INDEX.select('SELECT_ADDRID', (addr,))
    CUR.execute('''SELECT cluster FROM Cluster WHERE addr = ?''', (addrid,))
    clusterid = CUR.fetchone()[0]
    if clusterid == -1:
        CUR.execute('''SELECT MAX(cluster)+1 FROM Cluster;''')
        nextclusterid = CUR.fetchone()[0]
        queue = collections.deque([addrid])
        visited = set([addrid])
        while queue:
            caid = queue.popleft()
            for c in CORE.selectcur('SELECT_MULTIINPUT', (caid,)):
                c = c[0]
                if c not in visited:
                    visited.add(c)
                    queue.append(c)
            if FLAGS.version == 2:
                for c in CORE.selectcur('SELECT_ONEOUTPUT', (caid,)):
                    c = c[0]
                    if c not in visited:
                        visited.add(c)
                        queue.append(c)
                len3 = len(queue)
            if DEBUG:
                print(f'Queue remain: {len(queue)}', end='\r')
        t1 = time.time()
        CUR.execute('BEGIN TRANSACTION')
        CUR.executemany('''UPDATE Cluster SET Cluster = ? WHERE addr = ?''', [(nextclusterid, c) for c in visited])    
        CUR.execute('COMMIT TRANSACTION')
        if DEBUG:
            print(f'COMMIT during {time.time()-t1}')
        clusterid = nextclusterid
    return clusterid


def get_addrids(clusterid):
    global INDEX
    global CORE
    global CONN
    global CUR
    global STIME
    
    for aid in CUR.execute('''SELECT addr FROM Cluster WHERE cluster = ?''', (clusterid,)):
        yield aid[0]


class HTTPDaemon(socketserver.TCPServer):
    allow_reuse_address = True


class RESTRequestHandler(http.server.BaseHTTPRequestHandler):
    global INDEX
    global CORE
    global CONN
    global CUR
    global STIME

    def do_GET(self):
        if DEBUG:
            print((f'[{int(time.time()-STIME)}] {self.address_string()} -- '
                   f'[{self.date_time_string()}] {self.requestline}'))
        endpoint = (self.requestline.split(' ')[1]).split('/')[1]
        if endpoint != 'addr':
            self.send_response(404)
            return
        addr = (self.requestline.split(' ')[1]).split('/')[-1]
        clusterid = get_clusterid(addr)
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        addrs = 0
        for aid in get_addrids(clusterid):
            a = INDEX.select('SELECT_ADDR', (aid,))
            self.wfile.write(f'''{a}\n'''.encode('utf-8'))
            addrs = addrs + 1
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] {addr} find {addrs}')


def main():
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
        print(f'[{int(time.time()-STIME)}] Initialize Cluster Database')
        conn = sqlite3.connect(FLAGS.cluster)
        cur = conn.cursor()
        cur.execute('''PRAGMA journal_mode = OFF''')
        cur.execute('''PRAGMA synchronous = OFF''')
        cur.execute('''CREATE TABLE IF NOT EXISTS Cluster (
                         addr INTEGER PRIMARY KEY,
                         cluster NOT NULL);''')
        INDEX.cur.execute('''SELECT MAX(id) FROM AddrID;''')
        conn.commit()
        cur.execute('BEGIN TRANSACTION')
        for i in range(1, INDEX.cur.fetchone()[0]+1):
            cur.execute('''INSERT INTO Cluster (addr, cluster)
                           VALUES (?, ?);''', (i, -1))
        cur.execute('COMMIT TRANSACTION')
        print(f'[{int(time.time()-STIME)}] Bulk Insert Into Cluster Database Complete')
        cur.execute('''CREATE INDEX idx_Cluster_2 ON Cluster(cluster);''')
        conn.close()
        print(f'[{int(time.time()-STIME)}] Initialize Cluster Database Complete')
    CONN = conn = sqlite3.connect(FLAGS.cluster)
    CUR = cur = conn.cursor()
    CUR.execute('''PRAGMA journal_mode = NORMAL''')
    CUR.execute('''PRAGMA synchronous = WAL''')
    CONN.commit()
    with HTTPDaemon((FLAGS.ip, FLAGS.port), 
                    RESTRequestHandler) as httpd:
        try:
            print(f'[{int(time.time()-STIME)}] Serving {httpd.server_address}')
            httpd.serve_forever()
        except KeyboardInterrupt:
            print(f'[{int(time.time()-STIME)}] Terminate {httpd.server_address}')
            httpd.shutdown()

    CONN.close()
    db_index.close()
    db_core.close()
        

if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--index', type=str, required=True,
                        help='The path for index database')
    parser.add_argument('--core', type=str, required=True,
                        help='The path for core database')
    parser.add_argument('--cluster', type=str, default='./cluster.db',
                        help='The path for cluster database')
    parser.add_argument('--version', type=int, default=1,
                        choices=(1, 2),
                        help='The version of clustering methods')
    parser.add_argument('--ip', type=str, default='',
                        help='Serving ip address')
    parser.add_argument('--port', type=int, default=8989,
                        help='Serving port')
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.cluster = os.path.abspath(os.path.expanduser(FLAGS.cluster))

    DEBUG = FLAGS.debug

    main()
