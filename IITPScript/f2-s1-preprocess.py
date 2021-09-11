import os
import time

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def main():
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Parsed arguments {FLAGS}')
        print(f'[{int(time.time()-STIME)}] Unparsed arguments {_}')


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
    parser.add_argument('--service', type=str, required=True,
                        help='The path for service database')
    parser.add_argument('--input', type=str, required=True,
                        help='The target csv file with "tx", "class" field')
    parser.add_argument('--output', type=str,
                        help='The feature dataframe output')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
    FLAGS.input = os.path.abspath(os.path.expanduser(FLAGS.input))
    if FLAGS.output is None:
        FLAGS.output = f'f2-dataset.pkl'
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))

    DEBUG = FLAGS.debug

    main()
