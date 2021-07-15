import os
import time
import sqlite3
import csv

FLAGS = _ = None
DEBUG = False
STIME = None


def prepare_conn(indexpath, corepath, utilpath, servicepath):
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    if indexpath is not None:
        cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
    if corepath is not None:
        cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
    if utilpath is not None:
        cur.execute(f'''ATTACH DATABASE '{utilpath}' AS DBUTIL;''')
    if servicepath is not None:
        cur.execute(f'''ATTACH DATABASE '{servicepath}' AS DBSERVICE;''')
    conn.commit()
    
    return conn, cur


def create_currency_db(conn, cur):
    cur.execute('''PRAGMA journal_mode = NORMAL''')
    cur.execute('''PRAGMA synchronous = WAL''')
    cur.execute('''CREATE TABLE IF NOT EXISTS BTC2Dollar (
                     unixtime INTEGER PRIMARY KEY,
                     dollar REAL NOT NULL
                   );''')
    conn.commit()


def insert_currency_data(conn, cur, csvpath):
    cur.execute('BEGIN TRANSACTION')
    with open(csvpath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            k = datetime.datetime.fromisoformat(f'{row["Timestamp"]}+00:00').timestamp()
            v = float(row['market-price'])
            cur.execute('''INSERT OR IGNORE INTO BTC2Dollar (
                             unixtime, dollar) VALUES (
                             ?, ?);''', (k, v))
    cur.execute('COMMIT TRANSACTION')
    conn.commit()


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = prepare_conn(FLAGS.index, FLAGS.core,
                             FLAGS.util, FLAGS.service)
    create_currency_db(conn, cur)
    insert_currency_data(conn, cur, FLAGS.data)

    for row in cur.execute('''SELECT * FROM BTC2Dollar LIMIT 100;'''):
        print(row)


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--index', type=str,
                        help='The path for index database')
    parser.add_argument('--core', type=str,
                        help='The path for core database')
    parser.add_argument('--util', type=str,
                        help='The path for util database')
    parser.add_argument('--service', type=str, 
    #required=True,
                        help='The path for service database')
    parser.add_argument('--data', type=str,
                        help='The path for BTC to Dollar ratio csv')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug
    STIME = time.time()

    if FLAGS.index is not None:
        FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    if FLAGS.core is not None:
        FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    if FLAGS.util is not None:
        FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    if FLAGS.service is not None:
        FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
    if FLAGS.data is not None:
        FLAGS.data = os.path.abspath(os.path.expanduser(FLAGS.data))

    main()

