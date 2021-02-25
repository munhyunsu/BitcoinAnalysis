import numpy as np


class UnionFind(object):
    def __init__(self, size=0):
        self.parent = np.full(size+1, range(size+1), dtype=np.int32)
    
    def __repr__(self):
        return f'Parent: {self.parent}'

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
            if self.parent[xroot] < self.parent[yroot]:
                self.parent[yroot] = xroot
            elif self.parent[xroot] > self.parent[yroot]:
                self.parent[xroot] = yroot
            else:
                low = min(xroot, yroot)
                high = max(xroot, yroot)
                self.parent[high] = low


if __name__ == '__main__':
    uf = UnionFind(10)
    print(uf)
    uf.union(2, 4)
    print(uf)
    uf.union(7, 10)
    print(uf)
    uf.union(2, 10)
    print(uf)
    print(uf.find(10))
    uf.union(2, 9)
    print(uf)
