import pickle
import pprint


def print_cc(path):
    ll = dict()
    with open(path, 'rb') as f:
        data = pickle.load(f)
    s = 0
    cc = 0
    for entry in data:
        l = len(entry)
        c = ll.get(l, 0) + 1
        ll[l] = c
        s = s + l
        cc = cc + 1
    print(f'{path} {s} with {cc} [Defail Below]:')
    pprint.pprint(ll)


path1 = ''

for path in [path1, path2, path3, path4]:
    print_cc(path)

