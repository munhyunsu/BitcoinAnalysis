import blocksci
import csv


def get_addresses_same_cluster(address):
    result = set()
    cluster = cm.cluster_with_address(chain.address_from_string(address))
    for entry in cluster.addresses:
        try:
            result.add(entry.address_string)
        except AttributeError:
            pass
    return result


def get_target(path):
    result = set()
    with open(path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            result.add(row[0])
    return result


def cc():
    result = list()
    target = get_target('')
    while len(target) != 0:
        address = target.pop()
        target.add(address)
        cluster = get_addresses_same_cluster(address)
        inter_set = target.intersection(cluster)
        result.append(inter_set)
        target.difference_update(inter_set)
    return result
