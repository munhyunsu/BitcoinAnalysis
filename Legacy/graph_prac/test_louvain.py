import sys
import time

from igraph import *

print(sys.argv)
for n in range(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])):
    p = 5/n

    stime = time.time()
    G = Graph.Erdos_Renyi(n, p)
    etime = time.time()
    tgc = etime - stime

    stime = time.time()
    partition = G.community_multilevel()
    etime = time.time()
    tlv = etime - stime

    size = len(partition)

    print(f'{n},{size},{tgc},{tlv}')
