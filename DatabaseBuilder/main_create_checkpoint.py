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


## TODO(LuHa): Need to update
def get_block_index(height):
    global FLAGS
    global DEBUG
    global RPCM

    blkids = []
    txids = []
    addrids = []

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
    
    blkids.append((blockhash,))
    for btx in block['tx']:
        tx = btx['txid']
        txids.append((tx,))
        for n, vout in enumerate(btx['vout'], start=0):
            try:
                for addr, btc in utils.addr_btc_from_vout(vout):
                    addrids.append((addr,))
            except Exception:
                raise Exception(f'For debug! {tx}:{n}:{vout}')

    data = {'height': height,
            'blkids': blkids,
            'txids': txids,
            'addrids': addrids
           }

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Index read: {height:<7d}', end='\r')

    return data


def get_block_data(height):
    global FLAGS
    global DEBUG
    global RPCM
    global CONN
    global CUR
    
    try:
        if CONN is None or CUR is None:
            raise sqlite3.Error
        CUR.execute('''SELECT * FROM sqlite_master LIMIT 1;''')
        res = CUR.fetchall()
    except sqlite3.Error as e:
        os.makedirs(FLAGS.output, exist_ok=True)
        dbpath = os.path.join(FLAGS.output, 'checkpoint.db')
        CONN = sqlite3.connect(f'{dbpath}')
        CUR = CONN.cursor()
    finally:
        conn = CONN
        cur = CUR
    
    list_blktime = []
    list_blktx = []
    list_txout = []
    list_txin = []
    
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

    cur.execute('''SELECT id FROM blkid
                     WHERE blkhash = ?;''', (blockhash,))
    blkid = cur.fetchone()[0]
    blktime = block['time']
    list_blktime.append((blkid, blktime))

    txes = []
    for btx in block['tx']:
        tx = btx['txid']
        cur.execute('''SELECT id FROM txid
                         WHERE tx = ?;''', (tx,))
        txid = cur.fetchone()[0]
        list_blktx.append((blkid, txid))
        for n, vout in enumerate(btx['vout'], start=0):
            try:
                for addr, btc in utils.addr_btc_from_vout(vout):
                    cur.execute('''SELECT id FROM addrid
                                     WHERE addr = ?;''', (addr,))
                    addrid = cur.fetchone()[0]
                    list_txout.append((txid, n, addrid, btc))
            except Exception:
                raise Exception(f'For debug! {tx}:{n}:{vout}')
        for n, vin in enumerate(btx['vin'], start=0):
            if 'coinbase' in vin:
                list_txin.append((txid, n, 0, 0))
                continue
            ptx = vin['txid']
            cur.execute('''SELECT id FROM txid
                             WHERE tx = ?;''', (ptx,))
            ptxid = cur.fetchone()[0]
            pn = vin['vout']
            list_txin.append((txid, n, ptxid, pn))
    
    data = {'height': height,
            'blktime': list_blktime,
            'blktx': list_blktx,
            'txout': list_txout,
            'txin': list_txin
           }

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Data read: {height:<7d}', end='\r')

    return data


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    with os.scandir(FLAGS.output) as it:
        for entry in it:
            if not entry.name.startswith('.') and entry.is_file():
                os.remove(entry.path)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Remove directory {FLAGS.output}')

    os.makedirs(FLAGS.output, exist_ok=True)
    dbpath = os.path.join(FLAGS.output, 'checkpoint.db')
    conn = sqlite3.connect(f'{dbpath}')
    cur = conn.cursor()
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Connect to SQLite3 database: {dbpath}')

    rpc = AuthServiceProxy((f'http://{secret.rpcuser}:{secret.rpcpassword}@'
                            f'{secret.rpchost}:{secret.rpcport}'),
                           timeout=FLAGS.rpctimeout)

    cur.execute('''CREATE TABLE blkid (
                     id INTEGER PRIMARY KEY,
                     blkhash TEXT NOT NULL UNIQUE,
                     miningtime INTEGER NOT NULL
                   );''')
    cur.execute('''CREATE TABLE txid (
                     id INTEGER PRIMARY KEY,
                     tx TEXT NOT NULL UNIQUE,
                     blk INTEGER NOT NULL
                   );''')
    cur.execute('''CREATE TABLE addrid (
                     id INTEGER PRIMARY KEY,
                     addr TEXT NOT NULL UNIQUE,
                     tx INTEGER NOT NULL
                   );''')
    cur.execute('''CREATE TABLE txout (
                     tx INTEGER NOT NULL,
                     n INTEGER NOT NULL,
                     addr INTEGER NOT NULL,
                     btc REAL NOT NULL
                   );''')
    cur.execute('''CREATE TABLE txin (
                     tx INTEGER NOT NULL,
                     n INTEGER NOT NULL,
                     ptx INTEGER NOT NULL,
                     pn INTEGER NOT NULL
                   );''')
    conn.commit()
    cur.execute(f'''PRAGMA journal_mode = OFF;''')
    cur.execute(f'''PRAGMA synchronous = OFF;''')
    cur.execute(f'''PRAGMA cache_size = {FLAGS.cachesize};''')
    cur.execute(f'''PRAGMA page_size = {FLAGS.pagesize};''')
    cur.execute(f'''VACUUM;''')
    conn.commit()

    ## TODO(LuHa): Need to update for schema!!
    height_start = 0
    bestblockhash = rpc.getbestblockhash()
    bestblock = rpc.getblock(bestblockhash)
    height_end = min(FLAGS.maxheight, bestblock['height'] - FLAGS.untrust)
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Best Block Heights: {bestblock["height"]}')
        print(f'[{int(time.time()-STIME)}] Time: {utils.gettime(bestblock["time"]).isoformat()}')

    print(f'[{int(time.time()-STIME)}] Begin build index database from {height_start} to {height_end}')

    for start, stop in utils.get_range(height_start, height_end+1, FLAGS.bulk):
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Index insert {start} ~ {stop}')
        cur.execute('''BEGIN TRANSACTION;''')
        with multiprocessing.Pool(FLAGS.process) as p:
            results = p.imap(get_block_index, range(start, stop), FLAGS.chunksize)
            for result in results:
                cur.executemany('''INSERT OR IGNORE INTO blkid (blkhash)
                                     VALUES (?);''', result['blkids'])
                cur.executemany('''INSERT OR IGNORE INTO txid (tx)
                                     VALUES (?);''', result['txids'])
                cur.executemany('''INSERT OR IGNORE INTO addrid (addr)
                                     VALUES (?);''', result['addrids'])
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Ready to transaction {start} ~ {stop}')
        cur.execute('''COMMIT TRANSACTION;''')
        conn.commit()
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Index insert transaction done {start} ~ {stop}')

    for start, stop in utils.get_range(height_start, height_end+1, FLAGS.bulk):
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Data insert {start} ~ {stop}')
        cur.execute('''BEGIN TRANSACTION;''')
        with multiprocessing.Pool(FLAGS.process) as p:
            results = p.imap(get_block_data, range(start, stop), FLAGS.chunksize)
            for result in results:
                cur.executemany('''INSERT INTO blktime (blk, miningtime)
                                     VALUES (?, ?);''', result['blktime'])
                cur.executemany('''INSERT INTO blktx (blk, tx)
                                     VALUES (?, ?);''', result['blktx'])
                cur.executemany('''INSERT INTO txout (tx, n, addr, btc)
                                     VALUES (?, ?, ?, ?);''', result['txout'])
                cur.executemany('''INSERT INTO txin (tx, n, ptx, pn)
                                     VALUES (?, ?, ?, ?);''', result['txin'])
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Ready to transaction {start} ~ {stop}')
        cur.execute('''COMMIT TRANSACTION;''')
        conn.commit()
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Data insert transaction done {start} ~ {stop}')

    print(f'[{int(time.time()-STIME)}] End build database from {height_start} to {height_end}')

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
    parser.add_argument('--maxheight', type=int, default=float('inf'),
                        help='The max height to create checkpoint')
    parser.add_argument('--bulk', type=int, default=100000,
                        help='The bulk process height')
    parser.add_argument('--process', type=int, 
                        default=min(multiprocessing.cpu_count()//2, 16),
                        help='The number of multiprocess')
    parser.add_argument('--chunksize', type=int, default=1,
                        help='The multiprocess chunksize')
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
