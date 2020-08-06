import os
import sqlite3
import time
import http.server
import socketserver

from db_manager import QUERY, DBReader

FLAGS = None
_ = None
DEBUG = False
INDEX = None
CORE = None
CONN = None
CUR = None


class HTTPDaemon(socketserver.TCPServer):
    allow_reuse_address = True


class RESTRequestHandler(http.server.BaseHTTPRequestHandler):
    global INDEX
    global CORE
    global CONN
    global CUR
    global STIME

    def do_POST(self):
        pass

    def do_GET(self):
        if DEBUG:
            print((f'[{int(STIME-time.time())}] {self.address_string()} -- '
                   f'[{self.date_time_string()}] {self.requestline}'))
                  
        addr = (self.requestline.split(' ')[1]).split('/')[-1]
        addrid = INDEX.select('SELECT_ADDRID', (addr,))
        CUR.execute('''SELECT cluster FROM Cluster WHERE addr = ?''', (addrid,))
        clusterid = CUR.fetchone()[0]
        if clusterid == -1:
            CUR.execute('''SELECT MAX(cluster)+1 FROM Cluster;''')
            nextclusterid = CUR.fetchone()[0]
            CUR.execute('BEGIN TRANSACTION')
            for c in CORE.selectcur('SELECT_MULTIINPUT', (addrid,)):
                CUR.execute('''UPDATE Cluster SET Cluster = ? WHERE addr = ?''', (nextclusterid, c))
            CUR.execute('COMMIT TRANSACTION')
            clusterid = nextclusterid
        CUR.execute('''SELECT addr FROM Cluster WHERE cluster = ?''', (clusterid,))
        addrs = list()
        for a in CUR:
            aid = INDEX.select('SELECT_ADDRID', (a,))
            addrs.append(aid)

            
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
        print(f'[{int(STIME-time.time())}] Initialize Cluster Database')
        conn = sqlite3.connect(FLAGS.cluster)
        cur = conn.cursor()
        cur.execute('''PRAGMA journal_mode = NORMAL''')
        cur.execute('''PRAGMA synchronous = WAL''')
        cur.execute('''CREATE TABLE IF NOT EXISTS Cluster (
                         addr INTEGER PRIMARY KEY,
                         cluster NOT NULL);''')
        INDEX.cur.execute('''SELECT MAX(id) FROM AddrID;''')
        cur.execute('BEGIN TRANSACTION')
        for i in range(1, INDEX.cur.fetchone()[0]+1):
            cur.execute('''INSERT INTO Cluster (addr, cluster)
                           VALUES (?, ?);''', (i, -1))
        cur.execute('COMMIT TRANSACTION')
        print(f'[{int(STIME-time.time())}] Bulk Insert Into Cluster Database Complete')
        cur.execute('''CREATE INDEX idx_Cluster_2 ON Cluster(cluster);''')
        conn.close()
        print(f'[{int(STIME-time.time())}] Initialize Cluster Database Complete')
    CONN = conn = sqlite3.connect(FLAGS.cluster)
    CUR = cur = conn.cursor()

    with HTTPDaemon((FLAGS.ip, FLAGS.port), 
                    RESTRequestHandler) as httpd:
        try:
            print(f'[{STIME-int(time.time())}] Serving {httpd.server_address}')
            httpd.serve_forever()
        except KeyboardInterrupt:
            print(f'[{STIME-int(time.time())}] Terminate {httpd.server_address}')
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
