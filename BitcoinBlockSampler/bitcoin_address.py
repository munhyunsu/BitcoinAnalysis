import time_manager
from db_manager import QUERY


class Address(object):
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.info = dict()
    
    def _get_info():
        pass
