import sqlite3

# !!SQL Query Field!!
## CAUTION! If you have modified this query, please review all source code.
QUERY = dict()
QUERY['CREATE_META_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS Meta (
      key TEXT PRIMARY KEY,
      value TEXT);'''
QUERY['CREATE_BLKID_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS BlkID (
      id INTEGER PRIMARY KEY,
      blkhash TEXT NOT NULL UNIQUE);'''
QUERY['CREATE_TXID_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS TxID (
      id INTEGER PRIMARY KEY,
      txid TEXT NOT NULL UNIQUE);'''
QUERY['CREATE_ADDRID_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS AddrID (
      id INTEGER PRIMARY KEY,
      addr TEXT NOT NULL UNIQUE);'''
QUERY['CREATE_TXHEIGHT_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS TxHeight (
      tx INTEGER PRIMARY KEY,
      height INTEGER NOT NULL);'''
QUERY['CREATE_TXIN_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS TxIn (
      tx INTEGER,
      n INTEGER,
      addr INTEGER,
      UNIQUE (tx, n, addr));'''
QUERY['CREATE_TXOUT_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS TxOut (
      tx INTEGER,
      n INTEGER,
      addr INTEGER,
      UNIQUE (tx, n, addr));'''

QUERY['SELECT_MAX_BLKID'] = '''
    SELECT MAX(id) FROM BlkID;'''

QUERY['INSERT_META'] = '''
    INSERT OR IGNORE INTO Meta (
      key, value) VALUES (
      ?, ?);'''
QUERY['INSERT_BLKID'] = '''
    INSERT OR IGNORE INTO BlkID (
      id, blkhash) VALUES (
      ?, ?);'''
QUERY['INSERT_TXID'] = '''
    INSERT OR IGNORE INTO TxID (
      txid) VALUES (
      ?);'''
QUERY['INSERT_ADDRID'] = '''
    INSERT OR IGNORE INTO AddrID (
      addr) VALUES (
      ?);'''

QUERY['UPDATE_META'] = '''
    UPDATE Meta SET value=? 
    WHERE key=?;'''


class DBBuilder(object):
    def __init__(self, dbtype: str, dbpath: str):
        self.dbpath = dbpath
        self.conn = sqlite3.connect(self.dbpath)
        self.cur = self.conn.cursor()
        self.cur.execute('PRAGMA synchronous = NORMAL;')
        self.cur.execute('PRAGMA journal_mode = WAL;')
        self.conn.commit()
        dbtype = dbtype.lower()
        if dbtype == 'index':
            self._create_table_index()
        elif dbtype == 'core':
            self._create_table_core()
        elif dbtype == 'util':
            self._create_table_util()
        else:
            assert Exception('[Error] dbtype is not one of [index, core, util]')

    def _create_table_index(self):
        self.begin()
        for q in ['CREATE_META_TABLE',
                  'CREATE_BLKID_TABLE',
                  'CREATE_TXID_TABLE',
                  'CREATE_ADDRID_TABLE']:
            self.cur.execute(QUERY[q])
        self.commit()
    
    def select(self, query, *args):
        self.cur.execute(QUERY[query], *args)
        value = self.cur.fetchone()
        if value is not None:
            value = value[0]
        return value

    def selectall(self, query, *args):
        self.cur.execute(QUERY[query], *args)
        return self.cur.fetchall()

    def insert(self, query, *args):
        self.cur.execute(QUERY[query], *args)
        self.commit()

    def insertmany(self, query, *args):
        self.cur.executemany(QUERY[query], *args)

    def begin(self):
        self.cur.execute('BEGIN TRANSACTION;')

    def commit(self):
        self.cur.execute('COMMIT TRANSACTION;')

    def close(self):
        self.conn.close()
