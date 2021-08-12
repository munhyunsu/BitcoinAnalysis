import os
import time
import multiprocessing

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
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_arguments('--debug', action='store_true',
                         help='The present debug message')
    parser.add_arguments('--process', type=int,
                         default=min(multiprocessing.cpu_count()//2, 16),
                         help='The number of processes')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    main()

