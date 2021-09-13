import time
import pickle

import pandas as pd
from sklearn.ensemble import RandomForestClassifier

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def main():
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Parsed arguments {FLAGS}')
        print(f'[{int(time.time()-STIME)}] Unparsed arguments {_}')

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Reading preprocessed pickle')
    df = pd.read_pickle(FLAGS.input)

    X = df[df.columns[3:]]
    Y = df[df.columns[1]]

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Fitting model')

    clf = RandomForestClassifier()
    clf.fit(X, Y)

    with open(FLAGS.output, 'wb') as f:
        pickle.dump(clf, f)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Exported model to {FLAGS.output}')


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--input', type=str, required=True,
                        help='The target train pickle file')
    parser.add_argument('--output', type=str,
                        help='The model output')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.input = os.path.abspath(os.path.expanduser(FLAGS.input))
    if FLAGS.output is None:
        FLAGS.output = f'f2-model.pkl'
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))

    DEBUG = FLAGS.debug

    main()
