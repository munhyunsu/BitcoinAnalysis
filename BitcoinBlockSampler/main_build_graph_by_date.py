import os

FLAGS = None
_ = None
DEBUG = None
STIME = None
CORE = None


def main():
    # Opening
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    STIME = time.time()
    CORE = db_core = DBReader(FLAGS.core)

    
    
    
    
    
    
    
    
    # Closing
    db_core.close()








if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--from', type=str, required=True,
                        help='The from datetime for ISO 8601')
    parser.add_argument('--to', type=str, required=True,
                        help='The to datetime for ISO 8601')
    parser.add_argument('--core', type=str, required=True,
                        help='The path for core database')
    parser.add_argument('--output', type=str, required=True,
                        help='The path for igraph file')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))
    DEBUG = FLAGS.debug

    main()

