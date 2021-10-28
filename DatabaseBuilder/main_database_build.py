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
    res = cur.fetchall()
    if len(res) == 0:
        next_blkid = 1
    else:
        next_blkid = res[0][0] + 1
    cur.execute('''SELECT MAX(id) FROM txid;''')
    res = cur.fetchall()
    if len(res) == 0:
        next_txid = 1
    else:
        next_txid = res[0][0] + 1
    cur.execute('''SELECT MAX(id) FROM addrid;''')
    res = cur.fetchall()
    if len(res) == 0:
        next_addrid = 1
    else:
        next_addrid = res[0][0] + 1
    
    height_start = next_blkid
    bestblockhash = rpc.getbestblockhash()
    bestblock = rpc.getblock(bestblockhash)
    height_end = bestblock['height'] - FLAGS.untrust
    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Best Block Heights: {bestblock["height"]}')
        print(f'[{int(time.time()-STIME)}] Time: {utils.gettime(bestblock["height"]).isoformat()}')



    print(f'[{int(time.time()-STIME)}] Start from {height_start} to {height_end}')
    print(f'[{int(time.time()-STIME)}] with starting blkid: {blkid_next}, txid: {txid_next}, addrid: {addrid_next}')

    for height in range(height_start, height_end+1):
        data_blkid = []
        data_txid = []
        data_addrid = []
        data_blktime = []
        data_blktx = []
        data_txout = []
        data_txin = []
        
        blockhash = rpc.getblockhash(height)
        if block['height'] != next_blkid:
            print(f'Something is wrong: {block["height"]} != {next_blkid}')
            conn.close()
            sys.exit(0)
        blkid = next_blkid
        next_blkid = next_blkid + 1
        data_blkid.append((blkid, blockhash))
        block = rpc.getblock(blockhash, 2)
        blktime = block['time']
        data_blktime.append((blkid, blktime))
        for btx in block['tx']:
            tx = btx['txid']
            cur.execute('''SELECT id FROM txid
                             WHERE tx = ?;''', (tx,))
            res = cur.fetchall()
            if len(res) == 0:
                txid = next_txid
                next_txid = next_txid + 1
                data_txid.append((tx, txid))
                data_blktx.append((blkid, txid))
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
                            data_addrid.append((addr, addrid))
                        else:
                            addrid = res[0][0]
                        data_txout.append((txid, n, addrid, btc))
                except Exception:
                    raise Exception(f'For debug! {tx}:{n}:{vout}')
            for n, vin in enumerate(btx['vin'], start=0):
                if 'coinbase' in vin:
                    data_txin.append((txid, n, 0, 0))
                    continue
                ptx = vin['txid']
                cur.execute('''SELECT id FROM txid
                                 WHERE tx = ?;''', (ptx,))
                res = cur.fetchall()
                if len(res) == 0:
                    raise Exception(f'ptx not found {tx}:{n}:{vin}')
                else:
                    ptxid = res[0][0]
                pn = vin['vout']
                data_txin.append((txid, n, ptx, pn))
        
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
        cur.commit()
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
    parser.add_argument('--process', type=int, 
                        default=min(multiprocessing.cpu_count()//2, 16),
                        help='The number of multiprocess')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    main()