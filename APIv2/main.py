import os
import sqlite3
from typing import Union, List

from fastapi import FastAPI
from pydantic import BaseModel

import secret
import information

conn = cur = None
app = FastAPI(
  title=information.title,
  description=information.description,
  version=information.version,
  contact=information.contact,
  license_info=information.license_info,
  root_path=secret.root_path)


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
    global cur
    global conn
    # Get latest block information
    query = '''SELECT MAX(DBINDEX.BlkID.id), 
                      DBINDEX.BlkID.blkhash, 
                      strftime('%Y-%m-%d %H:%M:%S', 
                        DBCORE.BlkTime.unixtime, 'unixepoch')
               FROM DBINDEX.BlkID
               INNER JOIN DBCORE.BlkTime ON 
                 DBCORE.BlkTime.blk = DBINDEX.BlkID.id;'''
    cur.execute(query)
    row = cur.fetchone()
    return {'Say': 'Hello world!',
            'Latest block height': row[0],
            'Latest block hash': row[1],
            'Latest mining time (UTC)': row[2]}


@app.post('/clusters/clusterRelations')
async def cluster_relations(clusterId: int, nodeClusters: List[Union[int, None]]):
    global cur
    global conn
    result = {}

    # Real Cluster ID from Address ID
    query = '''SELECT MIN(DBSERVICE.Cluster.addr)
               FROM DBSERVICE.Cluster
               WHERE DBSERVICE.Cluster.cluster = (
                   SELECT DBSERVICE.Cluster.cluster
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.addr = ?);'''
    cur.execute(query, (clusterId,))
    real_id = cur.fetchone()[0]

    # Get Tags from clusters
    query = '''SELECT DBSERVICE.TagID.tag
               FROM DBSERVICE.Tag
               INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
               WHERE DBSERVICE.Tag.addr IN (SELECT DBSERVICE.Cluster.addr
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = ?);'''
    cur.execute(query, (real_id,))
    tags = set()
    for row in cur.fetchall():
        tags.add(row[0])
    tags = sorted(list(tags))

    # Outcomes
    query = '''SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.btc AS btc
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = ?);'''
    cur.execute(query, (real_id,))
    total_outcome_btc = 0.0
    tx_set = set()
    for row in cur.fetchall():
        tx_set.add(row[0])
        total_outcome_btc = total_outcome_btc + row[1]

    # Incomes
    query = '''SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.btc AS btc
               FROM DBCORE.TxOut
               WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = ?);'''
    cur.execute(query, (real_id,))
    total_income_btc = 0.0
    for row in cur.fetchall():
        tx_set.add(row[0])
        total_income_btc = total_income_btc + row[1]
    total_tx_cnt = len(tx_set)

    # result: Cluster
    result['Cluster'] = [{'clusterId': real_id,
                          'clusterName': tags[0] if len(tags) > 0 else 'Unknown',
                          'category': 'deposit',
                          'balance': total_income_btc-total_outcome_btc,
                          'transferCount': total_tx_cnt}]
    # Hard coding
    if result['Cluster'][0]['clusterName'].lower() in ['bithumb', 'upbit', 'coinone', 'korbit']:
        result['Cluster'][0]['category'] = 'exchange'
    
    # result: count
    result['count'] = len(nodeClusters)

    # result: Edge
    edges = []
    for lead_id in nodeClusters:
        # Real Cluster ID from Node Address ID
        query = '''SELECT MIN(DBSERVICE.Cluster.addr)
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = (
                       SELECT DBSERVICE.Cluster.cluster
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.addr = ?);'''
        cur.execute(query, (lead_id,))
        real_lead_id = cur.fetchone()[0]

        tx_set = set()
        first_timestamp = 4133980799 # 2100-12-23T23:59:59
        last_timestamp = 0 # 1970-01-01T12:00:00
        from_me_btc = 0.0
        to_me_btc = 0.0
        # From me to lead_id
        query = '''SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.btc AS btc, DBCORE.BlkTime.unixtime AS unixtime
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
                   WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.cluster = ?)
                   AND DBCORE.TxIn.tx IN (SELECT DBCORE.TxOut.tx
                       FROM DBCORE.TxOut
                       WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                           FROM DBSERVICE.Cluster
                           WHERE DBSERVICE.Cluster.cluster = ?));'''
        cur.execute(query, (real_id, real_lead_id))
        for row in cur.fetchall():
            tx_set.add(row[0])
            from_me_btc = from_me_btc + row[1]
            if first_timestamp > row[2]:
                first_timestamp = row[2]
            if last_timestamp < row[2]:
                last_timestamp = row[2]

        # From lead_id to me
        query = '''SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.btc AS btc, DBCORE.BlkTime.unixtime AS unixtime
                   FROM DBCORE.TxOut
                   INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
                   WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.cluster = ?)
                   AND DBCORE.TxOut.tx IN (SELECT DBCORE.TxIn.tx
                       FROM DBCORE.TxIn
                       INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                       WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                           FROM DBSERVICE.Cluster
                           WHERE DBSERVICE.Cluster.cluster = ?));'''
        cur.execute(query, (real_lead_id, real_id))
        for row in cur.fetchall():
            tx_set.add(row[0])
            to_me_btc = to_me_btc + row[1]
            if first_timestamp > row[2]:
                first_timestamp = row[2]
            if last_timestamp < row[2]:
                last_timestamp = row[2]
        
        # add edge
        edges.append({'leadClusterId': real_lead_id,
                      'transferCount': len(tx_set),
                      'sentTransferAmount': from_me_btc,
                      'receivedTransferAmount': to_me_btc,
                      'firstTransferTime': first_timestamp,
                      'lastTrasferTime': last_timestamp})
    result['Edge'] = edges

    return result


@app.get('/address/{addr}', summary='Get address information')
async def address_info(addr: str):
    response = []
    global cur
    global conn
    # Get transaction
    txes = []
    query = '''SELECT DISTINCT DBINDEX.TxID.txid AS txid
               FROM DBUTIL.Edge
               INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBUTIL.Edge.tx
               WHERE DBUTIL.Edge.src = (
                 SELECT DBINDEX.AddrID.id
                 FROM DBINDEX.AddrID
                 WHERE DBINDEX.AddrID.addr = ?)
               OR DBUTIL.Edge.dst = (
                 SELECT DBINDEX.AddrID.id
                 FROM DBINDEX.AddrID
                 WHERE DBINDEX.AddrID.addr = ?)
               ORDER BY DBINDEX.TxID.id DESC;'''
    for row in cur.execute(query, (addr, addr)):
        txes.append(row[0])
    # Get Income
    query = '''SELECT SUM(DBCORE.TxOut.btc) AS value
               FROM DBCORE.TxOut
               WHERE DBCORE.TxOut.addr = (
                 SELECT DBINDEX.AddrID.id
                 FROM DBINDEX.AddrID
                 WHERE DBINDEX.AddrID.addr = ?);'''
    cur.execute(query, (addr,))
    row = cur.fetchone()
    income = row[0]
    # Get Outcome
    query = '''SELECT SUM(DBCORE.TxOut.btc) AS Outcome
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx 
                 AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               WHERE TxOut.addr = (
                 SELECT DBINDEX.AddrID.id
                 FROM DBINDEX.AddrID
                 WHERE DBINDEX.AddrID.addr = ?);'''
    cur.execute(query, (addr,))
    row = cur.fetchone()
    outcome = row[0]
    # Balance
    balance = income - outcome

    return {'Address': addr,
            'TxCount': len(txes),
            'Income': income,
            'Outcome': outcome,
            'Balance': balance,
            'Txes': txes}


@app.get('/transaction/{txid}', summary='Get transaction information')
async def transaction_info(txid: str):
    response = []
    global cur
    global conn
    # Get block information
    query = '''SELECT DBINDEX.TxID.id,
                 DBINDEX.BlkID.id, 
                 DBINDEX.BlkID.blkhash, 
                 strftime('%Y-%m-%d %H:%M:%S', 
                   DBCORE.BlkTime.unixtime, 'unixepoch')
               FROM DBINDEX.TxID
               INNER JOIN DBCORE.BlkTx 
                 ON DBCORE.BlkTx.tx = DBINDEX.TxID.id
               INNER JOIN DBCORE.BlkTime 
                 ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
               INNER JOIN DBINDEX.BlkID 
                 ON DBINDEX.BlkID.id = DBCORE.BlkTx.blk
               WHERE DBINDEX.TxID.txid = ?;'''
    cur.execute(query, (txid,))
    row = cur.fetchone()
    tx = row[0]
    blockheight = row[1]
    blockhash = row[2]
    miningtime = row[3]
    # Get input information
    txincnt = 0
    txinbtc = 0
    txin = []
    query = '''SELECT DBINDEX.AddrID.addr, DBCORE.TxOut.btc
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut 
                 ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx 
                 AND DBCORE.TxOut.n = DBCORE.TxIn.pn
               INNER JOIn DBINDEX.AddrID 
                 ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
               WHERE DBCORE.TxIn.tx = ?
               ORDER BY DBCORE.TxOut.n ASC;'''
    for row in cur.execute(query, (tx,)):
        txin.append({'Address': row[0],
                     'BTC': row[1]})
        txincnt = txincnt + 1
        txinbtc = txinbtc + row[1]
    # Get output information
    txoutcnt = 0
    txoutbtc = 0
    txout = []
    query = '''SELECT DBINDEX.AddrID.addr, DBCORE.TxOut.btc
               FROM DBCORE.TxOut
               INNER JOIn DBINDEX.AddrID 
                 ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
               WHERE DBCORE.TxOut.tx = ?
               ORDER BY DBCORE.TxOut.n ASC;'''
    for row in cur.execute(query, (tx,)):
        txout.append({'Address': row[0],
                      'BTC': row[1]})
        txoutcnt = txoutcnt + 1
        txoutbtc = txoutbtc + row[1]
    # Calculate fee
    fee = txinbtc - txoutbtc
    
    return {'TxID': txid,
            'Block height': blockheight,
            'Block hash': blockhash,
            'Mining time': miningtime,
            'In count': txincnt,
            'In BTC': txinbtc,
            'Out count': txoutcnt,
            'Out BTC': txoutbtc,
            'Fee': fee,
            'In information': txin,
            'Out information': txout}

@app.get('/clusters', summary='Get cluster names')
async def clusters():
    global cur
    global conn
    query = '''SELECT DISTINCT DBINDEX.AddrID.addr, DBSERVICE.TagID.tag
               FROM DBSERVICE.Tag
               INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
               INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBSERVICE.Tag.addr
               ORDER BY DBINDEX.AddrID.id ASC;'''
    response = []
    for row in cur.execute(query):
        response.append({'Address': row[0],
                         'Tag': row[1]})
    return response


@app.get('/clusters/search', summary='Search cluster information')
async def clusters_search(clustername: str):
    response = []
    global cur
    global conn
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

@app.get('/clusters/transactions', summary='Search transactions of clusters')
async def clusters_transactions(clustername: str,
                                from_cluster: bool = True, to_cluster: bool = True,
                                counterparty: Union[str, None] = None):
    global cur
    global conn

    response = []
    if from_cluster and to_cluster:
        query = '''SELECT DBCORE.BlkTime.Blk,
                   strftime('%Y-%m-%d %H:%M:%S', DBCORE.BlkTime.unixtime, 'unixepoch'),
                   DBINDEX.TxID.txid, 
                   SRC.addr, DST.addr, DBUTIL.Edge.btc, 
                   DBUTIL.Edge.tx, DBUTIL.Edge.src, DBUTIL.Edge.dst
                   FROM DBUTIL.Edge
                   INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBUTIL.Edge.tx
                   INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = DBUTIL.Edge.src
                   INNER JOIN DBINDEX.AddrID AS DST ON DST.id = DBUTIL.Edge.dst
                   INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBUTIL.Edge.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
                   WHERE DBUTIL.Edge.src IN (
                     SELECT DBSERVICE.Cluster.addr
                     FROM DBSERVICE.Cluster
                     WHERE DBSERVICE.Cluster.cluster = (
                       SELECT DBSERVICE.Cluster.cluster
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.addr IN (
                         SELECT DBSERVICE.Tag.addr
                         FROM DBSERVICE.Tag
                         WHERE DBSERVICE.Tag.tag = (
                           SELECT DBSERVICE.TagID.id
                           FROM DBSERVICE.TagID
                           WHERE DBSERVICE.TagID.tag = ?
                         )
                       )
                     )
                   )
                   AND DBUTIL.Edge.dst IN (
                     SELECT DBSERVICE.Cluster.addr
                     FROM DBSERVICE.Cluster
                     WHERE DBSERVICE.Cluster.cluster = (
                       SELECT DBSERVICE.Cluster.cluster
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.addr IN (
                         SELECT DBSERVICE.Tag.addr
                         FROM DBSERVICE.Tag
                         WHERE DBSERVICE.Tag.tag = (
                           SELECT DBSERVICE.TagID.id
                           FROM DBSERVICE.TagID
                           WHERE DBSERVICE.TagID.tag = ?
                         )
                       )
                     )
                   )
                   ORDER BY SRC.id ASC;'''
    elif not from_cluster and to_cluster:
        query = '''SELECT DBCORE.BlkTime.Blk,
                   strftime('%Y-%m-%d %H:%M:%S', DBCORE.BlkTime.unixtime, 'unixepoch'),
                   DBINDEX.TxID.txid, 
                   SRC.addr, DST.addr, DBUTIL.Edge.btc, 
                   DBUTIL.Edge.tx, DBUTIL.Edge.src, DBUTIL.Edge.dst
                   FROM DBUTIL.Edge
                   INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBUTIL.Edge.tx
                   INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = DBUTIL.Edge.src
                   INNER JOIN DBINDEX.AddrID AS DST ON DST.id = DBUTIL.Edge.dst
                   INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBUTIL.Edge.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
                   WHERE DBUTIL.Edge.src NOT IN (
                     SELECT DBSERVICE.Cluster.addr
                     FROM DBSERVICE.Cluster
                     WHERE DBSERVICE.Cluster.cluster = (
                       SELECT DBSERVICE.Cluster.cluster
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.addr IN (
                         SELECT DBSERVICE.Tag.addr
                         FROM DBSERVICE.Tag
                         WHERE DBSERVICE.Tag.tag = (
                           SELECT DBSERVICE.TagID.id
                           FROM DBSERVICE.TagID
                           WHERE DBSERVICE.TagID.tag = ?
                         )
                       )
                     )
                   )
                   AND DBUTIL.Edge.dst IN (
                     SELECT DBSERVICE.Cluster.addr
                     FROM DBSERVICE.Cluster
                     WHERE DBSERVICE.Cluster.cluster = (
                       SELECT DBSERVICE.Cluster.cluster
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.addr IN (
                         SELECT DBSERVICE.Tag.addr
                         FROM DBSERVICE.Tag
                         WHERE DBSERVICE.Tag.tag = (
                           SELECT DBSERVICE.TagID.id
                           FROM DBSERVICE.TagID
                           WHERE DBSERVICE.TagID.tag = ?
                         )
                       )
                     )
                   )
                   ORDER BY SRC.id ASC;'''
    elif from_cluter and not to_cluster:
        query = '''SELECT DBCORE.BlkTime.Blk,
                   strftime('%Y-%m-%d %H:%M:%S', DBCORE.BlkTime.unixtime, 'unixepoch'),
                   DBINDEX.TxID.txid, 
                   SRC.addr, DST.addr, DBUTIL.Edge.btc, 
                   DBUTIL.Edge.tx, DBUTIL.Edge.src, DBUTIL.Edge.dst
                   FROM DBUTIL.Edge
                   INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBUTIL.Edge.tx
                   INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = DBUTIL.Edge.src
                   INNER JOIN DBINDEX.AddrID AS DST ON DST.id = DBUTIL.Edge.dst
                   INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBUTIL.Edge.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
                   WHERE DBUTIL.Edge.src IN (
                     SELECT DBSERVICE.Cluster.addr
                     FROM DBSERVICE.Cluster
                     WHERE DBSERVICE.Cluster.cluster = (
                       SELECT DBSERVICE.Cluster.cluster
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.addr IN (
                         SELECT DBSERVICE.Tag.addr
                         FROM DBSERVICE.Tag
                         WHERE DBSERVICE.Tag.tag = (
                           SELECT DBSERVICE.TagID.id
                           FROM DBSERVICE.TagID
                           WHERE DBSERVICE.TagID.tag = ?
                         )
                       )
                     )
                   )
                   AND DBUTIL.Edge.dst NOT IN (
                     SELECT DBSERVICE.Cluster.addr
                     FROM DBSERVICE.Cluster
                     WHERE DBSERVICE.Cluster.cluster = (
                       SELECT DBSERVICE.Cluster.cluster
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.addr IN (
                         SELECT DBSERVICE.Tag.addr
                         FROM DBSERVICE.Tag
                         WHERE DBSERVICE.Tag.tag = (
                           SELECT DBSERVICE.TagID.id
                           FROM DBSERVICE.TagID
                           WHERE DBSERVICE.TagID.tag = ?
                         )
                       )
                     )
                   )
                   ORDER BY SRC.id ASC;'''
    p1 = clustername
    if counterparty is None:
        p2 = clustername
    else:
        p2 = counterparty

    for row in cur.execute(query, (p1, p2)):
        response.append({'Mining time': row[1],
                         'TxID': row[2],
                         'Address from': row[3],
                         'Address to': row[4],
                         'BTC': row[5],
                        })
        
    return response

