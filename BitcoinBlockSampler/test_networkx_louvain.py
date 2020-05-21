import sys
import time

import community
import networkx as nx
import matplotlib.pyplot as plt

print(sys.argv)
for n in range(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])):
    p = 5/n

    stime = time.time()
    G = nx.fast_gnp_random_graph(n, p)
    etime = time.time()
    tgc = etime - stime

    stime = time.time()
    partition = community.best_partition(G)
    etime = time.time()
    tlv = etime - stime

    size = float(len(set(partition.values())))

    print(f'{n},{size},{tgc},{tlv}')
