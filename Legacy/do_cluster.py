import pickle
import os


def get_files(path, ext='', recursive=False):
    """
    Read all files in path
    :param path: path for reading
    :return: absolute path of all files in directory list
    """
    path_list = [path]

    while len(path_list) > 0:
        cpath = path_list.pop()
        with os.scandir(cpath) as it:
            for entry in it:
                if not entry.name.startswith('.') and entry.is_file():
                    if entry.name.endswith(ext):
                        yield entry.path
                    else:
                        if recursive:
                            path_list.append(entry.path)


cluster = dict()
ke = set()
for path in get_files('/home/harny/Github/BitcoinAnalysis/pic/multi',
                      '.pickle'):
    with open(path, 'rb') as f:
        data = pickle.load(f)
    for key in data[1].keys():
        if key not in cluster.keys():
            cluster[key] = data[1][key]
        else:
            print('Update!', key, path)
            ke.add(key)
            print('Before', cluster[key])
            cluster[key].update(data[1][key])
            print('After', cluster[key])

#print(cluster)
#k = list(cluster.keys())[0]
#for key in cluster.keys():
#    if len(cluster[k]) < len(cluster[key]):
#        k = key
#print(key, cluster[k])
#print(cluster)
k = list(ke)[0]
for key in ke:
    if len(cluster[k]) < len(cluster[key]):
        k = key
print(key, cluster[k])
