import argparse
import os
import time

import mariadb

import secret

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true',
                         help='The present debug message')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    main()

