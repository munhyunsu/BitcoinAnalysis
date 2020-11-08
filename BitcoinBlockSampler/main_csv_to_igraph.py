import os
import time
import sqlite3
import collections
import statistics

import pandas as pd
import igraph

import db_manager

FLAGS = _ = None
DEBUG = False


def create_df(conn, cur, target):
    Q = '''SELECT Edge.src AS src, Edge.dst AS dst, SUM(Edge.btc) AS btc, COUNT(Edge.tx) AS cnt
           FROM Edge
           WHERE Edge.src = ?
              OR Edge.dst = ?
           GROUP BY Edge.src, Edge.dst;'''
    result = list()
    for addr_id in target:
        for res in cur.execute(Q, (addr_id, addr_id)):
            result.append(res)
    df = pd.DataFrame(result, columns=('src', 'dst', 'btc', 'cnt'))
    df = df.astype({'src': str, 'dst': str, 'btc': float, 'cnt': int})
    return df


def df_to_graph(df):
    vertices = set()
    edges = list()
    weights = list()
    for index, row in df.iterrows():
        src = row['src']
        dst = row['dst']
        weight = row['btc']
        if src not in vertices:
            vertices.add(src)
        if dst not in vertices:
            vertices.add(dst)
        edges.append((src, dst))
        weights.append(weight)
    graph = igraph.Graph(directed=False)
    graph.add_vertices(list(vertices))
    graph.add_edges(edges)
    graph.es['weights'] = weights
    return graph


def csv_to_target(path):
    df = pd.read_csv(path)
    df = df.astype({'addr_id': str})
    target = set()
    for addr_id in df['addr_id']:
        target.add(addr_id)
    return target


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn = sqlite3.connect(f'file:{FLAGS.util}?mode=ro', uri=True)
    cur = conn.cursor()

    target = csv_to_target(FLAGS.target)
    if DEBUG:
        print(f'target: {len(target)}')

    df = create_df(conn, cur, target)
    conn.close()
    
    graph = df_to_graph(df)
    n = len(graph.vs)
    m = len(graph.es)
    if DEBUG:
        print(f'n: {n}, m: {m}')
    
    graph.write_pickle(f'/tmp/{os.path.splitext(__file__)[0]}.igraph')


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--util', type=str, required=True,
                        help='The path for Edge table')
    parser.add_argument('--target', type=str, required=True,
                        help='The path for subgraph csv')
    
    FLAGS, _ = parser.parse_known_args()

    FLAGS.util = os.path.abspath(os.path.expanduser(FLAGS.util))
    FLAGS.target = os.path.abspath(os.path.expanduser(FLAGS.target))

    DEBUG = FLAGS.debug

    main()
