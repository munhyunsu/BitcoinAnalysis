import os
import sqlite3
from typing import Union, List

from fastapi import FastAPI
from pydantic import BaseModel

import schemas
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
    #cur.execute(f'''ATTACH DATABASE '{secret.path_util}' AS DBUTIL;''')
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
async def cluster_relations(body: schemas.ClusterRelationsPost):
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
    cur.execute(query, (body.clusterId,))
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
    result['count'] = len(body.nodeClusters)

    # result: Edge
    edges = []
    for lead_id in body.nodeClusters:
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


@app.post('/clusters/edgeSelect')
async def edge_select(nodeClusters: List[int]):
    result = {}
    # Very dangerous assumtion! size of nodeClusters is 2!
    lead_id = nodeClusters[0]
    counter_id = nodeClusters[1]

    # Real Cluster ID from Lead ID
    query = '''SELECT MIN(DBSERVICE.Cluster.addr)
               FROM DBSERVICE.Cluster
               WHERE DBSERVICE.Cluster.cluster = (
                   SELECT DBSERVICE.Cluster.cluster
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.addr = ?);'''
    cur.execute(query, (lead_id,))
    real_lead_id = cur.fetchone()[0]
    # Real Cluster ID from Counter ID
    cur.execute(query, (counter_id,))
    real_counter_id = cur.fetchone()[0]

    # Get Tags from Lead ID
    query = '''SELECT DBSERVICE.TagID.tag
               FROM DBSERVICE.Tag
               INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
               WHERE DBSERVICE.Tag.addr IN (SELECT DBSERVICE.Cluster.addr
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = ?);'''
    cur.execute(query, (real_lead_id,))
    lead_tags = set()
    for row in cur.fetchall():
        lead_tags.add(row[0])
    lead_tags = sorted(list(lead_tags))
    lead_tag = lead_tags[0] if len(lead_tags) > 0 else 'Unknown'
    # Get Tags from Counter ID
    cur.execute(query, (real_counter_id,))
    counter_tags = set()
    for row in cur.fetchall():
        counter_tags.add(row[0])
    counter_tags = sorted(list(counter_tags))
    counter_tag = counter_tags[0] if len(counter_tags) > 0 else 'Unknown'

    # Categories
    # Hard coding!!!
    lead_category = 'deposit'
    if lead_tag.lower() in ['bithumb', 'upbit', 'coinone', 'korbit']:
        lead_category = 'exchange'
    counter_category = 'deposit'
    if counter_tag.lower() in ['bithumb', 'upbit', 'coinone', 'korbit']:
        counter_category = 'exchange'

    # Txes informations
    tx_set_lc = set()
    # From lead to counter
    query = '''SELECT DISTINCT DBCORE.TxOut.tx AS tx
               FROM DBCORE.TxOut
               WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = ?)
               AND DBCORE.TxOut.tx IN (SELECT DBCORE.TxIn.tx
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.cluster = ?));'''
    cur.execute(query, (real_counter_id, real_lead_id))
    for row in cur.fetchall():
        tx_set_lc.add(row[0])
    tx_list_lc = sorted(list(tx_set_lc))
    tx_set_cl = set()
    query = '''SELECT DISTINCT DBCORE.TxIn.tx AS tx
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = ?)
               AND DBCORE.TxIn.tx IN (SELECT DBCORE.TxOut.tx
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.cluster = ?));'''
    cur.execute(query, (real_counter_id, real_lead_id))
    for row in cur.fetchall():
        tx_set_cl.add(row[0])
    tx_list_cl = sorted(list(tx_set_cl))

    # result: cluster
    result['Cluster'] = [{'leadClusterId': real_lead_id,
                          'counterpartyClusterId': real_counter_id,
                          'leadClusterCategory': lead_category,
                          'counterpartyClusterCategory': counter_category,
                          'leadClusterName': lead_tag,
                          'counterpartyClusterName': counter_tag,
                          'transferCount': len(tx_list_lc) + len(tx_list_cl)}]
    result['count'] = len(tx_list_lc) + len(tx_list_cl)

    # Edges
    edges = []
    for idx, tx_list in enumerate([tx_list_lc, tx_list_cl]):
        for tx in tx_list:
            block_height = 0
            block_timestamp = 0
            sent_tag = lead_tag if idx == 0 else counter_tag
            received_tag = counter_tag if idx == 0 else lead_tag
            txid = ''
            sent_addresses = []
            sent_btc = 0.0
            received_addresses = []
            received_btc = 0.0
            fee = 0.0
            query = '''SELECT DBCORE.BlkTime.blk AS blk, DBCORE.BlkTime.unixtime AS unixtime, DBINDEX.TxID.txid AS txid, DBINDEX.AddrID.addr AS addr, DBCORE.TxOut.btc AS btc
                       FROM DBCORE.TxIn
                       INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                       INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
                       INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
                       INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
                       INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
                       WHERE DBCORE.TxIn.tx = ?
                       ORDER BY DBCORE.TxIn.n ASC;'''
            cur.execute(query, (tx,))
            for row in cur.fetchall():
                block_height = row[0]
                block_timestamp = row[1]
                txid = row[2]
                if row[3] not in sent_addresses:
                    sent_addresses.append(row[3])
                sent_btc = sent_btc + row[4]
            query = '''SELECT DBCORE.BlkTime.blk AS blk, DBCORE.BlkTime.unixtime AS unixtime, DBINDEX.TxID.txid AS txid, DBINDEX.AddrID.addr AS addr, DBCORE.TxOut.btc AS btc
                       FROM DBCORE.TxOut
                       INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
                       INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
                       INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
                       INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
                       WHERE DBCORE.TxOut.tx = ?
                       ORDER BY DBCORE.TxOut.n ASC;'''
            cur.execute(query, (tx,))
            for row in cur.fetchall():
                block_height = row[0]
                block_timestamp = row[1]
                txid = row[2]
                if row[3] not in received_addresses:
                    received_addresses.append(row[3])
                received_btc = received_btc + row[4]
            fee = sent_btc - received_btc
            edges.append({'txhashTime': block_timestamp,
                          'txhash': txid,
                          'txhashblock': block_height,
                          'txhashfee': fee,
                          'sentAddress': sent_addresses[0],
                          'sentClusterName': sent_tag,
                          'sentTransactionAmount': sent_btc,
                          'receivedAddress': received_addresses[0],
                          'receivedClusterName': received_tag,
                          'receivedTransactionAmount': received_btc})
    result['Edge'] = edges

    return result


@app.post('/clusters/clusterInfo')
async def cluster_info(clusterId: int):
    global cur
    global conn
    result = {}

    # Real Cluster ID from Cluster ID
    query = '''SELECT DBINDEX.AddrID.id, DBINDEX.AddrID.addr
               FROM DBINDEX.AddrID
               WHERE DBINDEX.AddrID.id = (SELECT MIN(DBSERVICE.Cluster.addr)
                  FROM DBSERVICE.Cluster
                  INNER JOIN DBINDEX.AddrID ON DBSERVICE.Cluster.addr = DBINDEX.AddrID.id
                  WHERE DBSERVICE.Cluster.cluster = (SELECT DBSERVICE.Cluster.cluster
                      FROM DBSERVICE.Cluster
                      WHERE DBSERVICE.Cluster.addr = ?));'''
    cur.execute(query, (clusterId,))
    real_id, root_address = cur.fetchone()

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
    tag = tags[0] if len(tags) > 0 else 'Unknown'

    # Category
    # Hard coding!!!
    category = 'deposit'
    if tag.lower() in ['bithumb', 'upbit', 'coinone', 'korbit']:
        category = 'exchange'

    # Address count
    query = '''SELECT COUNT(DBSERVICE.Cluster.cluster)
               FROM DBSERVICE.Cluster
               WHERE DBSERVICE.Cluster.cluster = ?
               GROUP BY DBSERVICE.Cluster.cluster;'''
    cur.execute(query, (real_id,))
    addr_cnt = cur.fetchone()[0]

    # Incomes
    income_btc = 0.0
    income_cnt = 0
    income_txes = set()
    first_timestamp = 4133980799 # 2100-12-23T23:59:59
    last_timestamp = 0 # 1970-01-01T12:00:00
    query = '''SELECT DBCORE.TxOut.tx, SUM(DBCORE.TxOut.btc), MIN(DBCORE.BlkTime.unixtime)
               FROM DBCORE.TxOut
               INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
               INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
               WHERE DBCORE.TxOut.tx IN (SELECT DISTINCT DBCORE.TxOut.tx
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.cluster = ?))
               GROUP BY DBCORE.TxOut.tx
               ORDER BY DBCORE.TxOut.tx ASC;'''
    cur.execute(query, (real_id,))
    for row in cur.fetchall():
        income_txes.add(row[0])
        income_btc = income_btc + row[1]
        income_cnt = income_cnt + 1
        if first_timestamp > row[2]:
            first_timestamp = row[2]
        if last_timestamp < row[2]:
            last_timestamp = row[2]

    # Outcomes
    outcome_btc = 0.0
    outcome_cnt = 0
    outcome_txes = set()
    query = '''SELECT DBCORE.TxIn.tx, SUM(DBCORE.TxOut.btc), MIN(DBCORE.BlkTime.unixtime)
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
               INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
               WHERE DBCORE.TxIn.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.cluster = ?))
               GROUP BY DBCORE.TxIn.tx
               ORDER BY DBCORE.TxIn.tx ASC;'''
    cur.execute(query, (real_id,))
    for row in cur.fetchall():
        outcome_txes.add(row[0])
        outcome_btc = outcome_btc + row[1]
        outcome_cnt = outcome_cnt + 1
        if first_timestamp > row[2]:
            first_timestamp = row[2]
        if last_timestamp < row[2]:
            last_timestamp = row[2]
    tx_cnt = len(income_txes | outcome_txes)

    # Fees
    fee = 0.0
    query = '''SELECT DBCORE.TxIn.tx AS tx, SUM(DBCORE.TxOut.btc)-T.btc AS fee
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               INNER JOIN (SELECT DBCORE.TxOut.tx AS tx, SUM(DBCORE.TxOut.btc) AS btc
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
                       FROM DBCORE.TxIn
                       INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                       WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                           FROM DBSERVICE.Cluster
                           WHERE DBSERVICE.Cluster.cluster = ?)
                       UNION
                       SELECT DISTINCT DBCORE.TxOut.tx
                       FROM DBCORE.TxOut
                       WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                           FROM DBSERVICE.Cluster
                           WHERE DBSERVICE.Cluster.cluster = ?))
                   GROUP BY DBCORE.TxOut.tx) AS T ON DBCORE.TxIn.tx = T.tx
               WHERE DBCORE.TxIn.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.cluster = ?)
                   UNION
                   SELECT DISTINCT DBCORE.TxOut.tx
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                       FROM DBSERVICE.Cluster
                       WHERE DBSERVICE.Cluster.cluster = ?))
               GROUP BY DBCORE.TxIn.tx;'''
    cur.execute(query, (real_id, real_id, real_id, real_id))
    for row in cur.fetchall():
        fee = fee + row[1]
    
    result['Cluster'] = {'clusterName': tag,
                         'category': category,
                         'root_address': root_address,
                         'balance': income_btc-outcome_btc,
                         'sentTransferAmount': outcome_btc,
                         'receivedTransferAmount': income_btc,
                         'transactionFee': fee,
                         'trasferCount': tx_cnt,
                         'sentTransferCount': len(outcome_txes),
                         'receivedTransferCount': len(income_txes),
                         'addressCount': addr_cnt,
                         'firstTransferTime': first_timestamp,
                         'lastTransferTime': last_timestamp}
    
    return result


@app.post('/clusters/transferInfo')
async def transfer_info(clusterId: int):
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

    # Trainsactions
    query = '''SELECT DBINDEX.TxID.txid AS tx, DBCORE.BlkTime.unixtime as unixtime, -SUM(DBCORE.TxOut.btc) AS btc, DBINDEX.AddrID.addr AS addr, DBSERVICE.Cluster.cluster AS clusterid
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
               INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
               INNER JOIN DBSERVICE.Cluster ON DBCORE.TxOut.addr = DBSERVICE.Cluster.addr
               INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
               INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
               WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = ?)
               GROUP BY DBCORE.TxIn.tx
               UNION
               SELECT DBINDEX.TxID.txid AS tx, DBCORE.BlkTime.unixtime as unixtime, SUM(DBCORE.TxOut.btc) AS btc, DBINDEX.AddrID.addr AS addr, DBSERVICE.Cluster.cluster AS clusterid
               FROM DBCORE.TxOut
               INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
               INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
               INNER JOIN DBSERVICE.Cluster ON DBCORE.TxOut.addr = DBSERVICE.Cluster.addr
               INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
               INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
               WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
                   FROM DBSERVICE.Cluster
                   WHERE DBSERVICE.Cluster.cluster = ?)
               GROUP BY DBCORE.TxOut.tx
               ORDER BY unixtime ASC;'''
    cur.execute(query, (real_id, real_id))
    tx_cnt = 0
    txes = []
    for row in cur.fetchall():
        tx_cnt = tx_cnt + 1
        txes.append({'txhashTime': row[1],
                     'txhash': row[0],
                     'transferAmount': row[2],
                     'receivedAddress': row[3],
                     'counterpartyClusterId': row[4],
                     'counterpartyClusterName': row[4], # Temporary
                    })
    result['count'] = tx_cnt

    cache = {}
    for idx in range(len(txes)):
        cluster_id = txes[idx]['counterpartyClusterId']
        if not cluster_id in cache.keys():
            # Get Tags from clusters
            query = '''SELECT DBSERVICE.TagID.tag
                       FROM DBSERVICE.Tag
                       INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
                       WHERE DBSERVICE.Tag.addr IN (SELECT DBSERVICE.Cluster.addr
                           FROM DBSERVICE.Cluster
                           WHERE DBSERVICE.Cluster.cluster = ?);'''
            cur.execute(query, (cluster_id,))
            tags = set()
            for row in cur.fetchall():
                tags.add(row[0])
            tags = sorted(list(tags))
            tag = tags[0] if len(tags) > 0 else 'Unknown'
            cache[cluster_id] = tag

        txes[idx]['counterpartyClusterName'] = cache[cluster_id]

    result['ClusterTransfer'] = txes

    return result


@app.post('/clusters/transferInfo/detailTransfer')
async def detail_transfer(clusterId: int, transferTxhash: str):
    global cur
    global conn
    result = {}
    # Get transaction information
    query = '''SELECT DBCORE.BlkTx.blk as height, DBINDEX.TxID.id as txid, DBCORE.BlkTime.unixtime AS unixtime
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
               INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
               INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
               INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
               WHERE DBINDEX.TxID.txid = ?
               GROUP BY DBCORE.TxIn.tx;'''
    cur.execute(query, (transferTxhash,))
    block_height, txid, block_timestamp = cur.fetchone()
    # Get inputs
    query = '''SELECT DBINDEX.AddrID.addr AS addr, DBSERVICE.Cluster.cluster AS cluster, DBCORE.TxOut.btc AS btc
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
               INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
               INNER JOIN DBSERVICE.Cluster ON DBCORE.TxOut.addr = DBSERVICE.Cluster.addr
               WHERE DBCORE.TxIn.tx = ?
               ORDER BY DBCORE.TxIn.tx, DBCORE.TxIn.n;'''
    cur.execute(query, (txid,))
    sents = []
    sent_btc = 0.0
    for row in cur.fetchall():
        sents.append({'sentAddress': row[0],
                      'sentClusterId': row[1],
                      'sentTransferAmount': row[2]})
        sent_btc = sent_btc + row[2]
    # Get outputs
    query = '''SELECT DBINDEX.AddrID.addr AS addr, DBSERVICE.Cluster.cluster AS cluster, DBCORE.TxOut.btc AS btc
               FROM DBCORE.TxOut
               INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
               INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
               INNER JOIN DBSERVICE.Cluster ON DBCORE.TxOut.addr = DBSERVICE.Cluster.addr
               WHERE DBCORE.TxOut.tx = ?
               ORDER BY DBCORE.TxOut.tx, DBCORE.TxOut.n;'''
    cur.execute(query, (txid,))
    receiveds = []
    received_btc = 0.0
    for row in cur.fetchall():
        receiveds.append({'receivedAddress': row[0],
                          'receivedClusterId': row[1],
                          'receivedTransferAmount': row[2]})
        received_btc = received_btc + row[2]
    # result
    result['ClusterTransfer'] = [{'txhash': transferTxhash,
                                  'txhashTime': block_timestamp,
                                  'txhashFee': sent_btc-received_btc,
                                  'txhashBlock': block_height}]
    result['sentCount'] = len(sents)
    result['sentClusterTransfer'] = sents
    result['receivedCount'] = len(receiveds)
    result['receivedClusterTransfer'] = receiveds

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

