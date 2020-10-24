import os
import time
import sqlite3
import collections

import pandas as pd
import igraph

import db_manager

FLAGS = _ = None
DEBUG = False


def csv_to_df(path):
    df = pd.read_csv(path)
    return df


def df_to_graph(df):
    vertices = set()
    edges = list()
    weights = list()
    for index, row in df.iterrows():
        src = str(int(row['src']))
        dst = str(int(row['dst']))
        weight = float(row['btc'])
        if src not in vertices:
            vertices.add(src)
        if dst not in vertices:
            vertices.add(dst)
        edges.append((src, dst))
        weights.append(weight)
    graph = igraph.Graph(directed=False)
    graph.add_vertices(vertices)
    graph.add_edges([x for x in edges])
    graph.es['weight'] = weights
    return graph


def csv_to_target(path):
    df = pd.read_csv(path)
    target = set()
    for addr_id in df['addr_id']:
        target.add(addr_id)
    return target


def get_subgraph(graph, target):
    vs = graph.select(name_in=target).indices
    subgraph = graph.subgraph(vs)
    return subgraph


def calc_variables(df, s):
    ns = len(target)
    ms = len(df[df['src'].isin(s) & df['dst'].isin(s)])
    cs = len(df[(df['src'].isin(s) & ~df['dst'].isin(s)) &
                (~df['src'].isin(s) & df['dst'].isin(s))])
    return ns, ms, cs


def get_mambership(graph, target):
    membership = list(range(len(graph.vs)))
    vs = graph.select(name_in=target).indices
    union = min(vs)
    for idx in vs:
        membership[idx] = union
    return membership


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    df = csv_to_df(FLAGS.graph)
    for key in ('src', 'dst', 'btc'):
        if key not in df.columns.values:
            raise Exception(f'{key} not in df.columns.values')
    
    graph = df_to_graph(df)
    n = len(graph.vs)
    m = len(graph.es)
    if DEBUG:
        print(f'n: {n}, m: {m}')
        
    target = csv_to_target(FLAGS.target)
    if DEBUG:
        print(f'target: {len(target)}')
    
    subgraph = get_subgraph(graph, target)

    ns, ms, cs = calc_variables(df, target)
    if DEBUG:
        print(f'ns: {ns}, ms: {ms}, cs: {cs}')

    du = graph.degree()
    if DEBUG:
        print(f'Maximum degree: {max(du)}')
    du_median = statistics.median(du)

    metrics = dict()
    # Internal connectivity
    metrics['internal_density'] = ms/(ns*(ns-1)/2)
    metrics['edges_inside'] = ms
    metrics['average_degree'] = (2*ms)/ns
    metrics['fraction_over_median_degree'] = len([x for x in target if subgraph.vs.select(name=x).degree()[0] > du_median])/ns
    metrics['triangle_participation_ratio'] = len(subgraph.cliques(min=3, max=3))/ns
    # External connectivity
    metrics['expansion'] = cs/ns
    metrics['cut_ratio'] = cs/(ns*(n-ns))
    # Combine connectivity
    metrics['conductance'] = cs/(2*ms+cs)
    metrics['normalized_cut'] = cs/(2*ms+cs) + cs/(2*(m-ms)+cs)
    out_degree_fraction = [(graph.vs.select(name=x).degree()[0]-subgraph.vs.select(name=x).degree()[0])/graph.vs.select(name=x).degree()[0] for x in target]
    metrics['maximum_out_degree_fraction'] = max(out_degree_fraction)
    metrics['average_out_degree_fraction'] = sum(out_degree_fraction)/ns
    metrics['flake_out_degree_fraction'] = len([x for x in target if subgraph.vs.select(name=x).degree()[0] < graph.vs.select(name=x).degree()[0]/2])/ns
    # Network connectivity
    membership = get_mambership(graph, target)
    metrics['modularity'] = graph.modularity(membership)
    
    print(metrics)


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--graph', type=str, required=True,
                        help='The path for graph csv')
    parser.add_argument('--target', type=str, required=True,
                        help='The path for subgraph csv')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.graph = os.path.abspath(os.path.expanduser(FLAGS.graph))
    FLAGS.target = os.path.abspath(os.path.expanduser(FLAGS.target))

    DEBUG = FLAGS.debug

    main()
