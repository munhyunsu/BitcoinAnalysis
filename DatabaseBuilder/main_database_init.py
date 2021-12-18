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
        cur.execute('''DROP TABLE IF EXISTS tag;''')
        cur.execute('''DROP TABLE IF EXISTS tagid;''')
        cur.execute('''DROP TABLE IF EXISTS txin;''')
        cur.execute('''DROP TABLE IF EXISTS txout;''')
        cur.execute('''DROP TABLE IF EXISTS blktx;''')
        cur.execute('''DROP TABLE IF EXISTS addrid;''')
        cur.execute('''DROP TABLE IF EXISTS txid;''')
        cur.execute('''DROP TABLE IF EXISTS blkid;''')
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] DROP all tables of {secret.dbdatabase}')
    
    cur.execute('''CREATE TABLE blkid (
                     id INT NOT NULL,
                     blkhash CHAR(64) NOT NULL,
                     miningtime TIMESTAMP NOT NULL,
                     PRIMARY KEY (id),
                     UNIQUE (blkhash)
                   );''')
    cur.execute('''CREATE TABLE txid (
                     id INT NOT NULL,
                     tx CHAR(64) NOT NULL,
                     blk INT NOT NULL,
                     PRIMARY KEY (id),
                     UNIQUE (tx),
                     FOREIGN KEY (blk) REFERENCES blkid (id)
                   );''')
    cur.execute('''CREATE TABLE addrid (
                     id INT NOT NULL,
                     addr VARCHAR(128) NOT NULL,
                     tx INT NOT NULL,
                     PRIMARY KEY (id),
                     UNIQUE (addr),
                     FOREIGN KEY (tx) REFERENCES txid (id)
                   );''')
    cur.execute('''CREATE INDEX idx_miningtime ON blkid (miningtime);''')
    cur.execute('''CREATE TABLE txout (
                     tx INT NOT NULL,
                     n INT NOT NULL,
                     addr INT NOT NULL,
                     btc DOUBLE NOT NULL,
                     FOREIGN KEY (tx) REFERENCES txid (id),
                     FOREIGN KEY (addr) REFERENCES addrid (id)
                   );''')
    cur.execute('''CREATE TABLE txin (
                     tx INT NOT NULL,
                     n INT NOT NULL,
                     ptx INT NOT NULL,
                     pn INT NOT NULL,
                     FOREIGN KEY (tx) REFERENCES txid (id)
                   );''')

    cur.execute('''CREATE TABLE tagid (
                     id INT NOT NULL,
                     tag CHAR(64) NOT NULL,
                     PRIMARY KEY (id),
                     UNIQUE (tag)
                   );''')
    cur.execute('''CREATE TABLE tag (
                     addr INT NOT NULL,
                     tag INT NOT NULL,
                     UNIQUE (addr, tag),
                     FOREIGN KEY (addr) REFERENCES addrid (id),
                     FOREIGN KEY (tag) REFERENCES tagid (id)
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
