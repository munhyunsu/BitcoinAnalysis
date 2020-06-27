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
      id INTEGER PRIMARY KEY);'''
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
QUERY['INSERT_META'] = '''
    INSERT OR IGNORE INTO Meta (
      key, value) VALUES (
      ?, ?);'''
QUERY['UPDATE_META'] = '''
    UPDATE Meta SET value=? 
    WHERE key=?;'''

class DBManager(object):
    def __init__(self, dbpath):
        self.dbpath = os.path.abspath(os.path.expanduser(dbpath))
        self.conn = sqlite3.connect(self.dbpath)
        self.cur = self.conn.cursor()    
        self._create_table_all()

    def _create_table_all():
        self.begin()
        self.cur
        for k in QUERY:
            if k.startswith('CREATE'):
                self.cur.execute(QUERY[k])
        self.commit()

    def begin():
        self.cur.execute('BEGIN TRANSACTION;')

    def commit():
        self.cur.execute('COMMIT TRANSACTION;')
