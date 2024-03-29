import argparse
import sys
import os
import time
import multiprocessing
import operator

import mariadb
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

import secret
import utils

FLAGS = _ = None
DEBUG = False
STIME = time.time()
RPCM = None
CONN = None
CUR = None


## TODO(LuHa): Need to update for schema changing
def get_block(height):
    global RPCM
    global CONN
    global CUR
    
    try:
        if CONN is None:
            raise mariadb.Error
        CUR.execute('''SHOW TABLES;''')
        res = CUR.fetchall()
    except mariadb.Error as e:
        CONN, CUR = utils.connectdb(user=secret.dbuser,
                                password=secret.dbpassword,
                                host=secret.dbhost,
                                port=secret.dbport,
                                database=secret.dbdatabase)
    finally:
        conn = CONN
        cur = CUR
    
    list_blockhash = []
    list_blktime = []
    
    blockhash = None
    block = None
    try:
        if RPCM is None:
            raise BrokenPipeError
        rpc = RPCM
        blockhash = rpc.getblockhash(height)
        block = rpc.getblock(blockhash, 2)
    except (BrokenPipeError, JSONRPCException):
        RPCM = AuthServiceProxy((f'http://{secret.rpcuser}:{secret.rpcpassword}@'
                                 f'{secret.rpchost}:{secret.rpcport}'),
                                timeout=FLAGS.rpctimeout)
        rpc = RPCM
    finally:
        if blockhash is None:
            blockhash = rpc.getblockhash(height)
        if block is None:
            block = rpc.getblock(blockhash, 2)
    blktime = block['time']

    txes = []
    for btx in block['tx']:
        tx = btx['txid']
        cur.execute('''SELECT id FROM txid
                         WHERE tx = ?;''', (tx,))
        res = cur.fetchall()
        if len(res) != 0:
            tx = res[0][0]
        list_vout = []
        for n, vout in enumerate(btx['vout'], start=0):
            list_addr_btc = []
            try:
                for addr, btc in utils.addr_btc_from_vout(vout):
                    cur.execute('''SELECT id FROM addrid
                                     WHERE addr = ?;''', (addr,))
                    res = cur.fetchall()
                    if len(res) != 0:
                        addr = res[0][0]
                    list_addr_btc.append((addr, btc))
            except Exception:
                raise Exception(f'For debug! {tx}:{n}:{vout}')
            list_vout.append(list_addr_btc)
        list_vin = []
        for n, vin in enumerate(btx['vin'], start=0):
            if 'coinbase' in vin:
                list_vin.append((-1, 0))
                continue
            ptx = vin['txid']
            cur.execute('''SELECT id FROM txid
                             WHERE tx = ?;''', (ptx,))
            res = cur.fetchall()
            if len(res) != 0:
                ptx = res[0][0]
            pn = vin['vout']
            list_vin.append((ptx, pn))
        txes.append({'tx': tx,
                     'vout': list_vout,
                     'vin': list_vin})
    
    data = {'height': height,
            'blkhash': blockhash,
            'blktime': blktime,
            'txes': txes
           }

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Data read: {height:<7d}', end='\r')

    return data


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    conn, cur = utils.connectdb(user=secret.dbuser,
                                password=secret.dbpassword,
                                host=secret.dbhost,
                                port=secret.dbport,
                                database=secret.dbdatabase)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Connect to database')

    rpc = AuthServiceProxy((f'http://{secret.rpcuser}:{secret.rpcpassword}@'
                            f'{secret.rpchost}:{secret.rpcport}'),
                           timeout=FLAGS.rpctimeout)

    cur.execute('''SELECT MAX(id) FROM blkid;''')
    next_blkid = cur.fetchall()[0][0]
    if next_blkid is None:
        next_blkid = 0
    else:
        next_blkid = next_blkid + 1
    cur.execute('''SELECT MAX(id) FROM txid;''')
    next_txid = cur.fetchall()[0][0]
    if next_txid is None:
        next_txid = 0
    else:
        next_txid = next_txid + 1
    cur.execute('''SELECT MAX(id) FROM addrid;''')
    next_addrid = cur.fetchall()[0][0]
    if next_addrid is None:
        next_addrid = 0
    else:
        next_addrid = next_addrid + 1
    
    height_start = next_blkid
    bestblockhash = rpc.getbestblockhash()
    bestblock = rpc.getblock(bestblockhash)
    height_end = bestblock['height'] - FLAGS.untrust
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Best Block Heights: {bestblock["height"]}')
        print(f'[{int(time.time()-STIME)}] Time: {utils.gettime(bestblock["time"]).isoformat()}')

    print(f'[{int(time.time()-STIME)}] Start from {height_start} to {height_end}')
    print(f'[{int(time.time()-STIME)}] with starting blkid: {next_blkid}, txid: {next_txid}, addrid: {next_addrid}')

    cache_block = {}
    data_blkid = []
    map_blkid = {}
    data_txid = []
    map_txid = {}
    data_addrid = []
    map_addrid = {}
    data_blktime = []
    map_blktime = {}
    data_blktx = []
    map_blktx = {}
    data_txout = []
    map_txout = {}
    data_txin = []
    map_txin = {}
    for height in range(height_start, height_end+1):
        if height not in cache_block.keys():
            if DEBUG:
                print(f'[{int(time.time()-STIME)}] Cache update: {height} ~ {min(height_end+1, height+FLAGS.bulk)-1}')
            with multiprocessing.Pool(FLAGS.process) as p:
                results = p.imap(get_block, range(height, min(height_end+1, height+FLAGS.bulk)), FLAGS.chunksize)
                for data in results:
                    cache_block[data['height']] = data
            if DEBUG:
                print(f'[{int(time.time()-STIME)}] Cache update done')
        if cache_block[height]['height'] != next_blkid:
            print(f'Something is wrong: {block["height"]} != {next_blkid}')
            conn.close()
            sys.exit(0)
        blockhash = cache_block[height]['blkhash']
        blkid = next_blkid
        next_blkid = next_blkid + 1
        data_blkid.append((blkid, blockhash))
        blktime = cache_block[height]['blktime']
        data_blktime.append((blkid, blktime))
        for txes in cache_block[height]['txes']:
            tx = txes['tx']
            if type(tx) == int:
                txid = tx
            elif tx not in map_txid.keys():
                txid = next_txid
                next_txid = next_txid + 1
                map_txid[tx] = txid
            else:
                txid = map_txid[tx]
            data_blktx.append((blkid, txid))
            for n, elements in enumerate(txes['vout'], start=0):
                for addr, btc in elements:
                    if type(addr) == int:
                        addrid = addr
                    elif addr not in map_addrid.keys():
                        addrid = next_addrid
                        next_addrid = next_addrid + 1
                        map_addrid[addr] = addrid
                    else:
                        addrid = map_addrid[addr]
                    data_txout.append((txid, n, addrid, btc))
            for n, elements in enumerate(txes['vin'], start=0):
                ptx = elements[0]
                pn = elements[1]
                if type(ptx) == int:
                    ptxid = ptx
                elif ptx not in map_txid.keys():
                    raise Exception(f'ptx not found! {tx}:{n}:{ptx}')
                else:
                    ptxid = map_txid[ptx]
                data_txin.append((txid, n, ptxid, pn))

        if height % FLAGS.bulk == 1:
            for key, value in map_blkid.items():
                data_blkid.append((value, key))
            data_blkid.sort(key=operator.itemgetter(0))
            for key, value in map_txid.items():
                data_txid.append((value, key))
            data_txid.sort(key=operator.itemgetter(0))
            for key, value in map_addrid.items():
                data_addrid.append((value, key))
            data_addrid.sort(key=operator.itemgetter(0))

            if DEBUG:
                print(f'[{int(time.time()-STIME)}] Ready to transaction {height}')
            try:
                cur.execute('''START TRANSACTION;''')
                for blkid_id, blkid_blkhash in data_blkid:
                    cur.execute('''INSERT INTO blkid (id, blkhash)
                                     VALUES (?, ?);''', (blkid_id, blkid_blkhash))
                for txid_id, txid_tx in data_txid:
                    cur.execute('''INSERT INTO txid (id, tx)
                                     VALUES (?, ?);''', (txid_id, txid_tx))
                for addrid_id, addrid_addr in data_addrid:
                    cur.execute('''INSERT INTO addrid (id, addr)
                                     VALUES (?, ?);''', (addrid_id, addrid_addr))
                for blktime_blk, blktime_miningtime in data_blktime:
                    cur.execute('''INSERT INTO blktime (blk, miningtime)
                                     VALUES (?, FROM_UNIXTIME(?));''', (blktime_blk, blktime_miningtime))
                for blktx_blk, blktx_tx in data_blktx:
                    cur.execute('''INSERT INTO blktx (blk, tx)
                                     VALUES (?, ?);''', (blktx_blk, blktx_tx))
                for txout_tx, txout_n, txout_addr, txout_btc in data_txout:
                    cur.execute('''INSERT INTO txout (tx, n, addr, btc)
                                     VALUES (?, ?, ?, ?);''', (txout_tx, txout_n, txout_addr, txout_btc))
                for txin_tx, txin_n, txin_ptx, txin_pn in data_txin:
                    cur.execute('''INSERT INTO txin (tx, n, ptx, pn)
                                     VALUES (?, ?, ?, ?);''', (txin_tx, txin_n, txin_ptx, txin_pn))
                cur.execute('''COMMIT;''')
                conn.commit()
            except mariadb.OperationalError as e:
                print(f'[{int(time.time()-STIME)}] MariaDB Error: {e}')
                sys.exit(1)
            if DEBUG:
                print(f'[{int(time.time()-STIME)}] Job  done {height}')

            cache_block = {}
            data_blkid = []
            map_blkid = {}
            data_txid = []
            map_txid = {}
            data_addrid = []
            map_addrid = {}
            data_blktime = []
            map_blktime = {}
            data_blktx = []
            map_blktx = {}
            data_txout = []
            map_txout = {}
            data_txin = []
            map_txin = {}
        else:
            if DEBUG:
                print(f'[{int(time.time()-STIME)}] Load done {height}', end='\r')

    for key, value in map_blkid.items():
        data_blkid.append((value, key))
    data_blkid.sort(key=operator.itemgetter(0))
    for key, value in map_txid.items():
        data_txid.append((value, key))
    data_txid.sort(key=operator.itemgetter(0))
    for key, value in map_addrid.items():
        data_addrid.append((value, key))
    data_addrid.sort(key=operator.itemgetter(0))

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Ready to transaction {height}')
    try:
        cur.execute('''START TRANSACTION;''')
        for blkid_id, blkid_blkhash in data_blkid:
            cur.execute('''INSERT INTO blkid (id, blkhash)
                             VALUES (?, ?);''', (blkid_id, blkid_blkhash))
        for txid_id, txid_tx in data_txid:
            cur.execute('''INSERT INTO txid (id, tx)
                             VALUES (?, ?);''', (txid_id, txid_tx))
        for addrid_id, addrid_addr in data_addrid:
            cur.execute('''INSERT INTO addrid (id, addr)
                             VALUES (?, ?);''', (addrid_id, addrid_addr))
        for blktime_blk, blktime_miningtime in data_blktime:
            cur.execute('''INSERT INTO blktime (blk, miningtime)
                             VALUES (?, FROM_UNIXTIME(?));''', (blktime_blk, blktime_miningtime))
        for blktx_blk, blktx_tx in data_blktx:
            cur.execute('''INSERT INTO blktx (blk, tx)
                             VALUES (?, ?);''', (blktx_blk, blktx_tx))
        for txout_tx, txout_n, txout_addr, txout_btc in data_txout:
            cur.execute('''INSERT INTO txout (tx, n, addr, btc)
                             VALUES (?, ?, ?, ?);''', (txout_tx, txout_n, txout_addr, txout_btc))
        for txin_tx, txin_n, txin_ptx, txin_pn in data_txin:
            cur.execute('''INSERT INTO txin (tx, n, ptx, pn)
                             VALUES (?, ?, ?, ?);''', (txin_tx, txin_n, txin_ptx, txin_pn))
        cur.execute('''COMMIT;''')
        conn.commit()
    except mariadb.OperationalError as e:
        print(f'[{int(time.time()-STIME)}] MariaDB Error: {e}')
        sys.exit(1)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Job done {height}')
  
    print(f'[{int(time.time()-STIME)}] All job completed from {height_start} to {height_end}')
    
    conn.commit()
    conn.close()


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true',
                         help='The present debug message')
    parser.add_argument('--rpctimeout', type=int, default=60,
                        help='The rpc timeout secounds')
    parser.add_argument('--untrust', type=int, default=100,
                        help='The block height that untrusted')
    parser.add_argument('--bulk', type=int, default=10000,
                        help='The bulk insert block height')
    parser.add_argument('--process', type=int, 
                        default=min(multiprocessing.cpu_count()//2, 16),
                        help='The number of multiprocess')
    parser.add_argument('--chunksize', type=int, default=32,
                        help='The multiprocess chunksize')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    main()
