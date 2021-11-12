import argparse
import sys
import os
import time
import multiprocessing
import operator

import sqlite3
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

import secret
import utils

FLAGS = _ = None
DEBUG = False
STIME = time.time()
RPCM = None
CONN = None
CUR = None


def get_block(height):
    global FLAGS
    global DEBUG
    global RPCM
    global CONN
    global CUR
    
    try:
        if CONN is None or CUR is None:
            raise sqlite3.Error:
        CUR.execute('''SELECT * FROM sqlite_master LIMIT 1;''')
        res = CUR.fetchall()
    except sqlite3.Error as e:
        if DEBUG:
            print(f'SQLite3 Error {e}')
        os.makedirs(FLAGS.output, exist_ok=True)
        dbpath = os.path.join(FLAGS.output, 'checkpoint.db')
        CONN = sqlite3.connect(f'{dbpath}')
        CUR = CONN.cursor()
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

    os.shutil.rmtree(FLAGS.output)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Remove directory {FLAGS.output}')

    os.makedirs(FLAGS.output, exist_ok=True)
    dbpath = os.path.join(FLAGS.output, 'checkpoint.db')
    conn = sqlite3.connect(f'{dbpath}')
    cur = CONN.cursor()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Connect to SQLite3 database: {dbpath}')

    rpc = AuthServiceProxy((f'http://{secret.rpcuser}:{secret.rpcpassword}@'
                            f'{secret.rpchost}:{secret.rpcport}'),
                           timeout=FLAGS.rpctimeout)

    cur.execute('''CREATE TABLE blkid (
                     id INTEGER PRIMARY KEY,
                     blkhash TEXT NOT NULL UNIQUE
                   );''')
    cur.execute('''CREATE TABLE txid (
                     id INTEGER PRIMARY KEY,
                     tx TEXT NOT NULL UNIQUE
                   );''')
    cur.execute('''CREATE TABLE addrid (
                     id INTEGER PRIMARY KEY,
                     addr TEXT NOT NULL
                   );''')
    cur.execute('''CREATE TABLE blktime (
                     blk INTETER NOT NULL,
                     miningtime INTEGER NOT NULL,
                     UNIQUE (blk, miningtime)
                   );''')
    cur.execute('''CREATE TABLE blktx (
                     blk INTEGER NOT NULL,
                     tx INTEGER NOT NULL,
                     UNIQUE (blk, tx)
                   );''')
    cur.execute('''CREATE TABLE txout (
                     tx INTEGER NOT NULL,
                     n INTEGER NOT NULL,
                     addr INTEGER NOT NULL,
                     btc REAL NOT NULL,
                     UNIQUE (tx, n, addr)
                   );''')
    cur.execute('''CREATE TABLE txin (
                     tx INTEGER NOT NULL,
                     n INTEGER NOT NULL,
                     ptx INTEGER NOT NULL,
                     pn INTEGER NOT NULL,
                     UNIQUE(tx, n)
                   );''')
    conn.commit()
    cur.execute(f'''PRAGMA journal_mode = OFF;''')
    cur.execute(f'''PRAGMA synchronous = OFF;''')
    cur.execute(f'''PRAGMA cache_size = {FLAGS.cachesize};''')
    cur.execute(f'''PRAGMA page_size = {FLAGS.pagesize};''')
    cur.execute(f'''VACUUM;''')
    conn.commit()

    height_start = 0
    bestblockhash = rpc.getbestblockhash()
    bestblock = rpc.getblock(bestblockhash)
    height_end = min(FLAGS.maxheight, bestblock['height'] - FLAGS.untrust)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Best Block Heights: {bestblock["height"]}')
        print(f'[{int(time.time()-STIME)}] Time: {utils.gettime(bestblock["time"]).isoformat()}')

    print(f'[{int(time.time()-STIME)}] Start from {height_start} to {height_end}')
    print(f'[{int(time.time()-STIME)}] with starting blkid: {next_blkid}, txid: {next_txid}, addrid: {next_addrid}')

    cache_block = {}
    cache_start = 0
    cache_end = 0
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
            cache_start = height
            cache_end = min(height_end+1, height+FLAGS.bulk)
            if DEBUG:
                print(f'[{int(time.time()-STIME)}] Cache update: {cache_start} ~ {cache_end-1}')
            with multiprocessing.Pool(FLAGS.process) as p:
                results = p.map(get_block, range(cache_start, cache_end))
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

        if height == cache_end-1:
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
                cur.execute('''BEGIN TRANSACTION;''')
                cur.executemany('''INSERT INTO blkid (id, blkhash)
                                     VALUES (?, ?);''', data_blkid)
                cur.executemany('''INSERT INTO txid (id, tx)
                                     VALUES (?, ?);''', data_txid)
                cur.executemany('''INSERT INTO addrid (id, addr)
                                     VALUES (?, ?);''', data_addrid)
                cur.executemany('''INSERT INTO blktime (blk, miningtime)
                                     VALUES (?, ?);''', data_blktime)
                cur.executemany('''INSERT INTO blktx (blk, tx)
                                     VALUES (?, ?);''', data_blktx)
                cur.executemany('''INSERT INTO txout (tx, n, addr, btc)
                                     VALUES (?, ?, ?, ?);''', data_txout)
                cur.executemany('''INSERT INTO txin (tx, n, ptx, pn)
                                     VALUES (?, ?, ?, ?);''', data_txin)
                cur.execute('''COMMIT TRANSACTION;''')
                conn.commit()
            except sqlite3.Error as e:
                print(f'[{int(time.time()-STIME)}] SQLite3 Error: {e}')
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
    parser.add_argument('--endheight', type=int, default=float('inf'),
                        help='The max height to create checkpoint')
    parser.add_argument('--bulk', type=int, default=100000,
                        help='The bulk process height')
    parser.add_argument('--process', type=int, 
                        default=min(multiprocessing.cpu_count()//2, 16),
                        help='The number of multiprocess')
    parser.add_argument('--pagesize', type=int, default=64*1024,
                        help='The page size of database (Max: 64×1024)')
    parser.add_argument('--cachesize', type=int, default=4194304,
                        help='The cache size of page (GB×1024×1024×1024÷(64×1024))')
    parser.add_argument('--output', type=str, default='./checkpoints',
                        help='The prefix for checkpoint csv files')

    FLAGS, _ = parser.parse_known_args()
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))
    DEBUG = FLAGS.debug

    main()
