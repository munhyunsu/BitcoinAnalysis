#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import library

import blocksci
import matplotlib.pyplot as plt
import matplotlib.ticker
import collections
import pandas as pd
import numpy as np
import csv


# In[3]:


# prepare data
chain = blocksci.Blockchain("./bitcoin-data/")
cm = blocksci.cluster.ClusterManager("./no_change_cluster/", chain)
print(cm)


# In[16]:


def get_addresses_same_cluster(address):
    result = set()
    try:
        cluster = cm.cluster_with_address(chain.address_from_string(address))
    except RuntimeError:
        return {address}
    for entry in cluster.addresses:
        try:
            result.add(entry.address_string)
        except AttributeError:
            pass
    return result


# In[17]:


def get_target(path):
    result = set()
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            result.add(row['Address'])
    return result


# In[21]:


def cc(path):
    result = list()
    target = get_target(path)
    count = 0
    while len(target) != 0:
        address = target.pop()
        target.add(address)
        cluster = get_addresses_same_cluster(address)
        inter_set = target.intersection(cluster)
        result.append(inter_set)
        target.difference_update(inter_set)
        count = count + 1
        if count % 50000 == 0:
            print(f'Pass: {count}')
    return result


# In[ ]:


path = ''
result = cc(path)

