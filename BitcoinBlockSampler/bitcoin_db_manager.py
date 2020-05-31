import sqlite3


class BitcoinDBManager(object):
    def __init__(self, dbpath):
        self.dbpath = os.path.abspath(os.path.expanduser(dbpath))
        self.conn = sqlite3.connect(self.dbpath)
        self.cur = self.conn.cursor()