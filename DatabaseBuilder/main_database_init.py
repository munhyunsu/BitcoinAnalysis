import argparse
import os
import time
import multiprocessing

import mariadb

import secret
import utils

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = utils.connectdb(user=secret.dbuser,
                                password=secret.dbpassword,
                                host=secret.dbhost,
                                port=secret.dbport,
                                database=secret.dbdatabase)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Connect to database')

    if FLAGS.reset:
        cur.execute('''DROP TABLE IF EXISTS txin;''')
        cur.execute('''DROP TABLE IF EXISTS txout;''')
        cur.execute('''DROP TABLE IF EXISTS blktx;''')
        cur.execute('''DROP TABLE IF EXISTS blktime;''')
        cur.execute('''DROP TABLE IF EXISTS addrid;''')
        cur.execute('''DROP TABLE IF EXISTS txid;''')
        cur.execute('''DROP TABLE IF EXISTS blkid;''')
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] DROP all tables of {secret.dbdatabase}')
    
    cur.execute('''CREATE TABLE blkid (
                     id INT NOT NULL,
                     blkhash CHAR(64) NOT NULL,
                     PRIMARY KEY (id),
                     UNIQUE (blkhash)
                   );''')
    cur.execute('''CREATE TABLE txid (
                     id INT NOT NULL,
                     txid CHAR(64) NOT NULL,
                     PRIMARY KEY (id),
                     UNIQUE (txid)
                   );''')
    cur.execute('''CREATE TABLE addrid (
                     id INT NOT NULL,
                     addr CHAR(64) NOT NULL,
                     PRIMARY KEY (id),
                     UNIQUE (addr)
                   );''')
    cur.execute('''CREATE TABLE blktime (
                     blk INT NOT NULL,
                     unixtime TIMESTAMP NOT NULL,
                     PRIMARY KEY (blk),
                     FOREIGN KEY (blk) REFERENCES blkid (id)
                   );''')
    cur.execute('''CREATE INDEX idx_unixtime ON blktime (unixtime);''')
    cur.execute('''CREATE TABLE blktx (
                     blk INT NOT NULL,
                     tx INT NOT NULL,
                     UNIQUE (blk, tx),
                     FOREIGN KEY (blk) REFERENCES blkid (id),
                     FOREIGN KEY (tx) REFERENCES txid (id)
                   );''')
    cur.execute('''CREATE TABLE txout (
                     tx INT NOT NULL,
                     n INT NOT NULL,
                     addr INT NOT NULL,
                     btc DOUBLE NOT NULL,
                     UNIQUE (tx, n),
                     FOREIGN KEY (tx) REFERENCES txid (id),
                     FOREIGN KEY (addr) REFERENCES addrid (id)
                   );''')
    cur.execute('''CREATE TABLE txin (
                     tx INT NOT NULL,
                     n INT NOT NULL,
                     ptx INT NOT NULL,
                     pn INT NOT NULL,
                     UNIQUE (tx, n),
                     FOREIGN KEY (tx) REFERENCES txid (id),
                     FOREIGN KEY (ptx, pn) REFERENCES txout (tx, n)
                   );''')
    conn.commit()

    cur.execute('''SHOW TABLES;''')
    res = cur.fetchall()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] CREATE TABLES: {res}')

    conn.commit()
    conn.close()


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true',
                         help='The present debug message')
    parser.add_argument('--reset', action='store_true',
                        help='Clear exists tables before creation')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    main()