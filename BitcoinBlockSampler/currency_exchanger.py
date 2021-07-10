import os
import time

FLAGS = _ = None
DEBUG = False
STIME = None


def prepare_conn(indexpath, corepath, utilpath, servicepath):
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
    cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
    cur.execute(f'''ATTACH DATABASE '{utilpath}' AS DBUTIL;''')
    cur.execute(f'''ATTACH DATABASE '{servicepath}' AS DBSERVICE;''')
    conn.commit()
    
    return conn, cur


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = prepare_conn(FLAGS.index, FLAGS.core,
                             FLAGS.util, FLAGS.service)

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
    parser.add_argument('--service', type=str, required=True,
                        help='The path for service database')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug
    STIME = time.time()

    main()

