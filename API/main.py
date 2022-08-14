import os
import sqlite3
from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel

import secret

conn = cur = None
app = FastAPI(root_path=secret.root_path)


class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[bool, None] = None


@app.on_event('startup')
async def startup_event():
    global cur
    global conn
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute(f'''ATTACH DATABASE '{secret.path_index}' AS DBINDEX;''')
    cur.execute(f'''ATTACH DATABASE '{secret.path_core}' AS DBCORE;''')
    cur.execute(f'''ATTACH DATABASE '{secret.path_util}' AS DBUTIL;''')
    cur.execute(f'''ATTACH DATABASE '{secret.path_service}' AS DBSERVICE;''')
    conn.commit()


@app.on_event('shutdown')
async def shutdown_event():
    global cur
    global conn
    conn.close()


@app.get('/', summary='Say hello to BitSQL API server')
async def read_root():
    return {'Say': 'Hello world!'}


@app.get('/address/{addr}', summary='Get address information')
async def address_info():
    response = []
    global cur
    global conn

    return response


@app.get('/clusters/search', summary='Search cluster information')
async def clusters_search(clustername: Union[str, None] = None):
    response = []
    global cur
    global conn
    if clustername is None:
        return response
    # Addr, TagID list
    clusters = []
    query = '''SELECT DBSERVICE.AddrTag.addr, DBSERVICE.AddrTag.tag
               FROM DBSERVICE.AddrTag
               WHERE DBSERVICE.AddrTag.tag IN (
                 SELECT DBSERVICE.TagID.id
                 FROM DBSERVICE.TagID
                 WHERE DBSERVICE.TagID.tag = ?);'''
    for row in cur.execute(query, (clustername,)):
        clusters.append({'addr': row[0], 'tagid': row[1]})
    # ClusterID list
    query = '''SELECT DBSERVICE.Cluster.cluster
               FROM DBSERVICE.Cluster
               WHERE DBSERVICE.Cluster.addr = ?;'''
    for cluster in clusters:
        for row in cur.execute(query, (cluster['addr'],)):
            cluster['clusterid'] = row[0]
    # Make responses!!
    query = '''SELECT Income.degree+Outcome.degree AS Degree, 
                      Income.value-Outcome.value AS Balance
               FROM (
                 SELECT COUNT(*) AS degree, SUM(DBCORE.TxOut.btc) AS value
                 FROM DBCORE.TxOut
                 WHERE DBCORE.TxOut.addr = ?) AS Income, (
                 SELECT COUNT(*) AS degree, SUM(DBCORE.TxOut.btc) AS value
                 FROM DBCORE.TxIn
                 INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND 
                            DBCORE.TxIn.pn = DBCORE.TxOut.n
                 WHERE DBCORE.TxOut.addr = ?) AS Outcome;'''
    for cluster in clusters:
        cur.execute(query, (cluster['addr'], cluster['addr']))
        row = cur.fetchone()
        response.append({'clusterID': cluster['clusterid'],
                         'clusterName': clustername,
                         'category': clustername,
                         'balance': row[1],
                         'transferCount': row[0],
                         'hasOsint': True})
    return response



"""
@app.get('/')
async def read_root():
    global cur
    global conn
    cur.execute('''SELECT MAX(DBINDEX.BlkID.id) FROM DBINDEX.BlkID;''')
    res = cur.fetchone()
    return {'Hello': 'World',
            'Latest Block Height': f'{res[0]}'}


@app.get('/items/{item_id}')
async def read_item(item_id: int, q: Union[str, None] = None):
    return {'item_id': item_id, 'q': q}


@app.put('/items/{item_id}')
async def update_item(item_id: int, item: Item):
    return {'item_name': item.name, 'item_id': item_id}
"""
