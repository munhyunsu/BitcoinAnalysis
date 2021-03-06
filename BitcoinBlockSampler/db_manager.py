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
QUERY['CREATE_ADDRTYPEID_TABLE'] = '''
    CREATE TABLE IF NOT EXISTS AddrTypeID (
      id INTEGER PRIMARY KEY,
      addrtype TEXT NOT NULL UNIQUE);'''

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
QUERY['INSERT_ADDRTYPEID'] = '''
    INSERT OR IGNORE INTO AddrTypeID (
      addrtype) VALUES (
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

QUERY['SELECT_TXIN_ADDRS'] = '''
    SELECT DBCORE.TxOut.addr
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND
                               DBCORE.TxOut.n = DBCORE.TxIn.pn
    WHERE DBCORE.TxIn.tx = ?;'''
QUERY['SELECT_TXOUT_ADDRS'] = '''
    SELECT DBCORE.TxOut.addr
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.tx = ?;'''
QUERY['SELECT_TXTIME'] = '''
    SELECT DBCORE.BlkTime.unixtime
    FROM DBCORE.BlkTx
    INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
    WHERE DBCORE.BlkTx.tx = ?;'''
QUERY['SELECT_FIRSTTIME'] = '''
    SELECT MIN(DBCORE.BlkTime.unixtime)
    FROM DBCORE.TxOut
    INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBCORE.TxOut.tx
    INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
    WHERE DBCORE.TxOut.addr = ?;'''
QUERY['SELECT_LASTTIME'] = '''
    SELECT MAX(DBCORE.BlkTime.unixtime)
    FROM DBCORE.TxOut
    INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBCORE.TxOut.tx
    INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
    WHERE DBCORE.TxOut.addr = ?;'''

QUERY['SELECT_SAMECLUSTER'] = '''
    SELECT addr
      FROM Cluster
      WHERE cluster = (SELECT cluster
                         FROM Cluster
                         WHERE addr = ?);'''
QUERY['SELECT_MULTIINPUT'] = '''
    SELECT DBCORE.TxOut.addr AS addr
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.txIn.tx IN (SELECT DBCORE.TxIn.tx
                      FROM DBCORE.TxIn
                      INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                      WHERE DBCORE.TxOut.addr = ?)
    GROUP BY DBCORE.TxOut.addr;'''
QUERY['SELECT_SINGLEOUTPUT'] = '''
    SELECT DBCORE.TxOut.addr
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND DBCORE.TxOut.n = DBCORE.TxIn.pn
    WHERE DBCORE.TxIn.tx IN (
        SELECT DBCORE.TxIn.tx
        FROM DBCORE.TxIn
        INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.tx
        WHERE DBCORE.TxIn.tx IN (
            SELECT DBCORE.TxOut.tx
            FROM DBCORE.TxOut
            WHERE DBCORE.TxOut.addr = ?
        )
        GROUP BY DBCORE.TxIn.tx
        HAVING COUNT(DISTINCT DBCORE.TxIn.n) > 1 AND COUNT(DISTINCT DBCORE.TxOut.n) = 1
    )
    GROUP BY DBCORE.TxOut.addr;'''

QUERY['ADDRESS_TO_ID'] = '''
    SELECT DBINDEX.AddrID.id
    FROM DBINDEX.AddrID
    WHERE DBINDEX.AddrID.addr = ?;'''
QUERY['ID_TO_ADDRESS'] = '''
    SELECT DBINDEX.AddrID.addr
    FROM DBINDEX.AddrID
    WHERE DBINDEX.AddrID.id = ?;'''
QUERY['TX_TO_ID'] = '''
    SELECT DBINDEX.TxID.id
    FROM DBINDEX.TxID
    WHERE DBINDEX.TxID.txid = ?;'''
QUERY['ID_TO_TX'] = '''
    SELECT DBINDEX.TxID.txid
    FROM DBINDEX.TxID
    WHERE DBINDEX.TxID.id = ?;'''

QUERY['COUNT_TX'] = '''
    SELECT COUNT(DISTINCT tx)
    FROM (
    SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND
                               DBCORE.TxOut.n = DBCORE.TxIn.pn
    WHERE DBCORE.TxOut.addr = ?
    UNION 
    SELECT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr = ?);'''

QUERY['INCOME'] = '''
    SELECT SUM(DBCORE.TxOut.btc)
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr = ?;'''
QUERY['OUTCOME'] = '''
    SELECT SUM(DBCORE.TxOut.btc)
    FROM DBCORE.TxIn 
    INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND
                               DBCORE.TxOut.n = DBCORE.TxIn.pn
    WHERE DBCORE.TxOut.addr = ?;'''
QUERY['BALANCE'] = '''
    SELECT Income.value-Outcome.value AS Balance
    FROM
    (SELECT SUM(DBCORE.TxOut.btc) AS value
     FROM DBCORE.TxOut
     WHERE DBCORE.TxOut.addr = ?) AS Income,
    (SELECT SUM(DBCORE.TxOut.btc) AS value
     FROM DBCORE.TxIn
     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND 
                                DBCORE.TxIn.pn = DBCORE.TxOut.n
     WHERE DBCORE.TxOut.addr = ?) AS Outcome;'''
QUERY['DATETIME_RANGE'] = '''
    SELECT MIN(DBCORE.BlkTime.unixtime), MAX(DBCORE.BlkTime.unixtime)
    FROM DBCORE.BlkTime
    INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
    WHERE DBCORE.BlkTx.tx IN
    (SELECT DBCORE.TxIn.tx
     FROM (
     SELECT DISTINCT DBCORE.TxIn.tx
     FROM DBCORE.TxIn
     INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND
                                DBCORE.TxOut.n = DBCORE.TxIn.pn
     WHERE DBCORE.TxOut.addr = ?
     UNION 
     SELECT DISTINCT DBCORE.TxOut.tx
     FROM DBCORE.TxOut
     WHERE DBCORE.TxOut.addr = ?));'''


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
                  'CREATE_ADDRID_TABLE',
                  'CREATE_ADDRTYPEID_TABLE']:
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
