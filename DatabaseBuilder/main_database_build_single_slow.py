import argparse
import os
import time
import multiprocessing

import mariadb
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

import secret
import utils

FLAGS = _ = None
DEBUG = False
STIME = time.time()


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
        next_blkid = 1
    else:
        next_blkid = next_blkid + 1
    cur.execute('''SELECT MAX(id) FROM txid;''')
    next_txid = cur.fetchall()[0][0]
    if next_txid is None:
        next_txid = 1
    else:
        next_txid = next_txid + 1
    cur.execute('''SELECT MAX(id) FROM addrid;''')
    next_addrid = cur.fetchall()[0][0]
    if next_addrid is None:
        next_addrid = 1
    else:
        next_addrid = next_addrid + 1
    
    height_start = next_blkid
    bestblockhash = rpc.getbestblockhash()
    bestblock = rpc.getblock(bestblockhash)
    height_end = bestblock['height'] - FLAGS.untrust
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Best Block Heights: {bestblock["height"]}')
        print(f'[{int(time.time()-STIME)}] Time: {utils.gettime(bestblock["height"]).isoformat()}')

    print(f'[{int(time.time()-STIME)}] Start from {height_start} to {height_end}')
    print(f'[{int(time.time()-STIME)}] with starting blkid: {next_blkid}, txid: {next_txid}, addrid: {next_addrid}')

    for height in range(height_start, height_end+1):
        data_blkid = []
        data_txid = []
        data_addrid = []
        data_blktime = []
        data_blktx = []
        data_txout = []
        data_txin = []
        
        blockhash = rpc.getblockhash(height)
        block = rpc.getblock(blockhash, 2)
        if block['height'] != next_blkid:
            print(f'Something is wrong: {block["height"]} != {next_blkid}')
            conn.close()
            sys.exit(0)
        blkid = next_blkid
        next_blkid = next_blkid + 1
        cur.execute('''INSERT INTO blkid (id, blkhash)
                         VALUES (?, ?);''', (blkid, blockhash))
        blktime = block['time']
        cur.execute('''INSERT INTO blktime (blk, miningtime)
                         VALUES (?, FROM_UNIXTIME(?));''', (blkid, blktime))
        for btx in block['tx']:
            tx = btx['txid']
            cur.execute('''SELECT id FROM txid
                             WHERE tx = ?;''', (tx,))
            res = cur.fetchall()
            if len(res) == 0:
                txid = next_txid
                next_txid = next_txid + 1
                cur.execute('''INSERT INTO txid (id, tx)
                                 VALUES (?, ?);''', (txid, tx))
                cur.execute('''INSERT INTO blktx (blk, tx)
                                 VALUES (?, ?);''', (blkid, txid))
            else:
                txid = res[0][0]
            for n, vout in enumerate(btx['vout'], start=0):
                try:
                    for addr, btc in utils.addr_btc_from_vout(vout):
                        cur.execute('''SELECT id FROM addrid
                                         WHERE addr = ?;''', (addr,))
                        res = cur.fetchall()
                        if len(res) == 0:
                            addrid = next_addrid
                            next_addrid = next_addrid + 1
                            cur.execute('''INSERT INTO addrid (id, addr)
                                             VALUES (?, ?);''', (addrid, addr))
                        else:
                            addrid = res[0][0]
                        cur.execute('''INSERT INTO txout (tx, n, addr, btc)
                                         VALUES (?, ?, ?, ?);''', (txid, n, addrid, btc))
                except Exception:
                    raise Exception(f'For debug! {tx}:{n}:{vout}')
            for n, vin in enumerate(btx['vin'], start=0):
                if 'coinbase' in vin:
                    cur.execute('''INSERT INTO txin (tx, n, ptx, pn)
                                     VALUES (?, ?, ?, ?);''', (txid, n, 0, 0))
                    continue
                ptx = vin['txid']
                cur.execute('''SELECT id FROM txid
                                 WHERE tx = ?;''', (ptx,))
                res = cur.fetchall()
                if len(res) == 0:
                    ptxid = txid
                else:
                    ptxid = res[0][0]
                pn = vin['vout']
                cur.execute('''INSERT INTO txin (tx, n, ptx, pn)
                                 VALUES (?, ?, ?, ?);''', (txid, n, ptxid, pn))

        if height % 10000 == 1:
            conn.commit()
            if DEBUG:
                print(f'[{int(time.time()-STIME)}] Job done {height}')
        else:
            if DEBUG:
                print(f'[{int(time.time()-STIME)}] Job done {height}', end='\r')
                
    conn.commit()
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
    parser.add_argument('--process', type=int, 
                        default=min(multiprocessing.cpu_count()//2, 16),
                        help='The number of multiprocess')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    main()