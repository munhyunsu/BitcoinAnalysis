import numpy as np


class UnionFind(object):
    def __init__(self, size=0):
        self.parent = np.full(size+1, range(size+1), dtype=np.int32)
        self.rank = np.full(size+1, 0, dtype=np.int32)

    def find(self, o):
        while o != self.parent[o]:
            p = self.parent[o]
            self.parent[o] = self.parent[p]
            o = p
        return o

    def union(self, x, y):
        xroot = self.find(x)
        yroot = self.find(y)
        if xroot != yroot:
            if self.rank[xroot] < self.rank[yroot]:
                self.parent[xroot] = yroot
            elif self.rank[xroot] > self.rank[yroot]:
                self.parent[yroot] = xroot
            else:
                low = min(xroot, yroot)
                high = max(xroot, yroot)
                self.parent[high] = low
                self.rank[low] = self.rank[low] + 1

    
if __name__ == '__main__':
    pass