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

QUERY['CREATE_BLKTIME_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS BlkTime (
      blk INTEGER PRIMARY KEY,
      unixtime INTEGER NOT NULL);'''
QUERY['CREATE_BLKTX_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS BlkTx (
      blk INTEGER NOT NULL,
      tx INTEGER NOT NULL,
      UNIQUE (blk, tx));'''
QUERY['CREATE_TXIN_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS TxIn (
      tx INTEGER NOT NULL,
      n INTEGER NOT NULL,
      ptx INTEGER NOT NULL,
      pn INTEGER NOT NULL,
      UNIQUE (tx, n));'''
QUERY['CREATE_TXOUT_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS TxOut (
      tx INTEGER NOT NULL,
      n INTEGER NOT NULL,
      addr INTEGER NOT NULL,
      btc REAL NOT NULL,
      UNIQUE (tx, n, addr));'''

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

QUERY['INSERT_BLKTIME'] = '''
    INSERT OR IGNORE INTO BlkTime (
      blk, unixtime) VALUES (
      ?, ?);'''
QUERY['INSERT_BLKTX'] = '''
    INSERT OR IGNORE INTO BlkTx (
      blk, tx) VALUES (
      ?, ?);'''
QUERY['INSERT_TXIN'] = '''
    INSERT OR IGNORE INTO TxIn (
      tx, n, ptx, pn) VALUES (
      ?, ?, ?, ?);'''
QUERY['INSERT_TXOUT'] = '''
    INSERT OR IGNORE INTO TxOut (
      tx, n, addr, btc) VALUES (
      ?, ?, ?, ?);'''

QUERY['UPDATE_META'] = '''
    UPDATE Meta SET value = ? 
      WHERE key = ?;'''

QUERY['SELECT_BLKID'] = '''
    SELECT id FROM BlkID WHERE blkhash = ?;'''
QUERY['SELECT_TXID'] = '''
    SELECT id FROM TxID WHERE txid = ?;'''
QUERY['SELECT_ADDRID'] = '''
    SELECT id FROM AddrID WHERE addr = ?;'''
QUERY['SELECT_ADDR'] = '''
    SELECT addr FROM AddrID WHERE id = ?;'''
QUERY['SELECT_MAX_BLKID'] = '''
    SELECT MAX(id) FROM BlkID;'''
QUERY['SELECT_META'] = '''
    SELECT value FROM Meta
      WHERE key = ?'''

QUERY['SELECT_MULTIINPUT'] = '''
    SELECT TxOut.addr AS addr
    FROM TxIn
    INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
    WHERE txIn.tx IN (SELECT TxIn.tx
                      FROM TxIn
                      INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
                      WHERE addr = ?)
    GROUP BY addr;'''
QUERY['SELECT_ONEOUTPUT'] = '''
    SELECT TxOut.addr AS addr
    FROM TxIn
    INNER JOIN TxOut ON TxOut.tx = TxIn.ptx AND TxOut.n = TxIn.pn
    WHERE TxIn.tx IN (SELECT TxIn.tx
                      FROM TxIn
                      INNER JOIN TxOut ON TxOut.tx = TxIn.tx
                      WHERE TxIn.tx IN (SELECT TxOut.tx
                                        FROM TxOut
                                        WHERE TxOut.addr = ?)
                      GROUP BY TxIn.tx
                      HAVING COUNT(DISTINCT TxIn.n) > 1 AND COUNT(DISTINCT TxOut.n) = 1)
    GROUP BY TxOut.addr;'''


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
        
    def _create_table_core(self):
        self.begin()
        for q in ['CREATE_META_TABLE',
                  'CREATE_BLKTIME_TABLE',
                  'CREATE_BLKTX_TABLE',
                  'CREATE_TXIN_TABLE',
                  'CREATE_TXOUT_TABLE']:
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
        
    def putmeta(self, key, value):
        self.cur.execute(QUERY['INSERT_META'], (key, value))
        self.cur.execute(QUERY['UPDATE_META'], (value, key))
    
    def getmeta(self, key):
        self.cur.execute(QUERY['SELECT_META'], (key,))
        value = self.cur.fetchone()
        if value is not None:
            value = value[0]
        return value
        
    def begin(self):
        self.cur.execute('BEGIN TRANSACTION;')

    def commit(self):
        self.cur.execute('COMMIT TRANSACTION;')

    def close(self):
        self.conn.close()


class DBReader(object):
    def __init__(self, dbpath: str):
        self.dbpath = dbpath
        self.conn = sqlite3.connect(f'file:{self.dbpath}?mode=ro', uri=True)
        self.cur = self.conn.cursor()
    
    def select(self, query, *args):
        self.cur.execute(QUERY[query], *args)
        value = self.cur.fetchone()
        if value is not None:
            value = value[0]
        return value

    def selectall(self, query, *args):
        self.cur.execute(QUERY[query], *args)
        return self.cur.fetchall()
    
    def selectcur(self, query, *args):
        self.cur.execute(QUERY[query], *args)
        return self.cur

    def close(self):
        self.conn.close()
