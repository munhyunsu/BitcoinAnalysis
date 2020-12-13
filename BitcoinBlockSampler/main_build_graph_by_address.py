import os
import time
import sqlite3

import numpy as np
import pandas as pd
import igraph

FLAGS = _ = None
DEBUG = None
STIME = time.time()
CONN = CUR = None


def main():
    # Opening
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    CONN = sqlite3.connect(':memory:')

    CUR = conn.cursor()
    CUR.execute(f'''ATTACH DATABASE '{FLAGS.index}' AS DBINDEX;''')
    CUR.execute(f'''ATTACH DATABASE '{FLAGS.core}' AS DBCORE;''')
    CUR.execute(f'''ATTACH DATABASE '{FLAGS.util}' AS DBUTIL;''')
    CONN.commit()
    
    # Closing
    CONN.close()


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--index', type=str, required=True,
                        help='The path for index database')
    parser.add_argument('--core', type=str, required=True,
                        help='The path for core database')
    parser.add_argument('--util', type=str, required=True,
                        help='The path for util database')
    parser.add_argument('--output', type=str, required=True,
                        help='The path for igraph file')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))
    DEBUG = FLAGS.debug

    main()

