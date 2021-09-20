import os
import time
import statistics
import pickle

import numpy as np
import pandas as pd
from scipy.stats import moment

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def main():
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Parsed arguments {FLAGS}')
        print(f'[{int(time.time()-STIME)}] Unparsed arguments {_}')

    with open(FLAGS.model, 'rb') as f:
        clf = pickle.load(f)

    df = pd.read_csv(FLAGS.input)
    df_len = len(df)

    vectors = []
    for index, row in df.iterrows():
        tx = row['TxID']
        vector = row[df.columns[3:]]
        vectors.append(vector)
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] {index} / {df_len} ({index/df_len:.2%}) Loaded!', end='\r')
    print(f'[{int(time.time()-STIME)}] {index} / {df_len} ({index/df_len:.2%}) Loaded!')
    result = clf.predict(vectors)
    df['Predict'] = result

    df.to_csv(FLAGS.output, index=False)
    print(f'|---- Result ----|')
    print(f'Accuracy: {len(df[df["Class"] == df["Predict"]])/df_len:.2%}')
    print(f'Accuracy (Licit class): {len(df[(df["Class"] == df["Predict"]) & (df["Class"] == "Licit")])/len(df[df["Class"] == "Licit"]):.2%}')
    print(f'Accuracy (Illicit class): {len(df[(df["Class"] == df["Predict"]) & (df["Class"] == "Illicit")])/len(df[df["Class"] == "Illicit"]):.2%}')
    


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--model', type=str, required=True,
                        help='The model pickle path')
    parser.add_argument('--input', type=str, required=True,
                        help='The test csv file with "TxID", "Class" field')
    parser.add_argument('--output', type=str,
                        help='The feature dataframe output')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.input = os.path.abspath(os.path.expanduser(FLAGS.input))
    if FLAGS.output is None:
        FLAGS.output = f'f2-result.csv'
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))

    DEBUG = FLAGS.debug

    main()
