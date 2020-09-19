import os
import time
import sqlite3
import multiprocessing

from secret import rpc_user, rpc_password
from time_manager import get_time
from db_manager import QUERY, DBBuilder, DBReader
from rpc_manager import RPCManager
from json_parser import addr_btc_from_vout

FLAGS = None
_ = None
DEBUG = False
RESUME = True
RPCM = None
INDEX = None


def get_data_index(height):
    global RPCM
    blks = list()
    txes = list()
    addrs = list()
    if RPCM is not None:
        rpcm = RPCM
    else:
        rpcm = RPCManager(rpc_user, rpc_password)
    block_hash = rpcm.call('getblockhash', height)
    block = rpcm.call('getblock', block_hash, 2)
    blks.append((height, block_hash))
    for tx in block['tx']:
        txes.append((tx['txid'],))
        for n, vout in enumerate(tx['vout']):
            for addr, btc in addr_btc_from_vout(tx['txid'], vout):
                addrs.append((addr,))
    RPCM = rpcm
    return blks, txes, addrs


def get_data_core(height):
    global INDEX
    global RPCM
    blktime = list()
    blktx = list()
    txin = list()
    txout = list()
    if RPCM is not None:
        rpcm = RPCM
    else:
        rpcm = RPCManager(rpc_user, rpc_password)
    block_hash = rpcm.call('getblockhash', height)
    block = rpcm.call('getblock', block_hash, 2)
    blkid = INDEX.select('SELECT_BLKID', (block['hash'],))
    blktime.append((blkid, block['time']))
    for tx in block['tx']:
        txid = INDEX.select('SELECT_TXID', (tx['txid'],))
        blktx.append((blkid, txid))
        for n, vin in enumerate(tx['vin']):
            if 'coinbase' in vin:
                txin.append((txid, n, 0, 0))
                continue
            ptxid = INDEX.select('SELECT_TXID', (vin['txid'],))
            pn = vin['vout']
            txin.append((txid, n, ptxid, pn))
        for n, vout in enumerate(tx['vout']):
            for addr, btc in addr_btc_from_vout(tx['txid'], vout):
                addrid = INDEX.select('SELECT_ADDRID', (addr,))
                txout.append((txid, n, addrid, btc))
    RPCM = rpcm
    return blktime, blktx, txin, txout


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
        
    dbb = DBBuilder(FLAGS.type, FLAGS.output)
    dbb.cur.execute(f'''PRAGMA journal_mode = OFF;''')
    dbb.cur.execute(f'''PRAGMA synchronous = OFF;''')
    dbb.cur.execute(f'''PRAGMA cache_size = {FLAGS.cachesize};''')
    if not RESUME:
        dbb.cur.execute(f'''PRAGMA page_size = {FLAGS.pagesize};''')
        dbb.cur.execute(f'''VACUUM;''')
    dbb.conn.commit()
    rpcm = RPCManager(rpc_user, rpc_password)
    
    if FLAGS.type == 'index':
        term = FLAGS.term
        start_height = dbb.select('SELECT_MAX_BLKID')
        if start_height is None:
            start_height = 0
        else:
            start_height = int(start_height)
        best_block_hash = rpcm.call('getbestblockhash')
        best_block = rpcm.call('getblock', best_block_hash)
        end_height = best_block['height'] - FLAGS.untrusted
        if DEBUG:
            print((f'Best Block Heights: {best_block["height"]}, '
                   f'Time: {get_time(best_block["time"]).isoformat()}'))
        print(f'Start from {start_height} to {end_height}')
        pool_num = FLAGS.process

        stime = time.time()
        mtime = time.time()
        for sheight, eheight in zip(range(start_height, end_height, term), 
                                    range(start_height+term, end_height+term, term)):
            if eheight >= end_height:
                eheight = end_height + 1
            dbb.begin()
            with multiprocessing.Pool(pool_num) as p:
                try:
                    results = p.imap(get_data_index, range(sheight, eheight))
                    for blks, txes, addrs in results:
                        dbb.insertmany('INSERT_BLKID', blks)
                        dbb.insertmany('INSERT_TXID', txes)
                        dbb.insertmany('INSERT_ADDRID', addrs)
                except KeyboardInterrupt:
                    print(f'KeyboardInterrupt detected. Terminate child processes.')
                    p.terminate()
                    p.join(60)
                    raise KeyboardInterrupt
            dbb.commit()
            if DEBUG:
                print(f'Job done from {sheight} to {eheight-1} during {time.time()-stime}')
        if DEBUG:
            print(f'All job completed {start_height} to {end_height} during {time.time()-stime}')
    elif FLAGS.type == 'core':
        if FLAGS.index is None:
            raise Exception('Need index database path. (--index)')
        global INDEX
        INDEX = DBReader(FLAGS.index)

        term = FLAGS.term
        start_height = dbb.getmeta('ProcessedBlockHeight')
        if start_height is None:
            start_height = 0
        else:
            start_height = int(start_height)
        end_height = INDEX.select('SELECT_MAX_BLKID')
        print(f'Start from {start_height} to {end_height}')
        pool_num = FLAGS.process

        stime = time.time()
        mtime = time.time()
        for sheight, eheight in zip(range(start_height, end_height, term), 
                                    range(start_height+term, end_height+term, term)):
            if eheight >= end_height:
                eheight = end_height + 1
            dbb.begin()
            with multiprocessing.Pool(pool_num) as p:
                try:
                    results = p.imap(get_data_core, range(sheight, eheight))
                    for blktime, blktx, txin, txout in results:
                        dbb.insertmany('INSERT_BLKTIME', blktime)
                        dbb.insertmany('INSERT_BLKTX', blktx)
                        dbb.insertmany('INSERT_TXIN', txin)
                        dbb.insertmany('INSERT_TXOUT', txout)
                except KeyboardInterrupt:
                    print(f'KeyboardInterrupt detected. Terminate child processes.')
                    p.terminate()
                    p.join(60)
                    raise KeyboardInterrupt
            dbb.putmeta('ProcessedBlockHeight', eheight-1)
            dbb.commit()
            if DEBUG:
                print(f'Job done from {sheight} to {eheight-1} during {time.time()-stime}')
        if not RESUME:
            dbb.cur.execute(f'''CREATE INDEX idx_BlkTime_2 ON BlkTime(unixtime);''')
            dbb.cur.execute(f'''CREATE INDEX idx_BlkTx_2 ON BlkTx(tx);''')
            dbb.cur.execute(f'''CREATE INDEX idx_TxIn_3_4 ON TxIn(ptx, pn);''')
            dbb.cur.execute(f'''CREATE INDEX idx_TxOut_3 ON TxOut(addr);''')
            dbb.conn.commit()
        if DEBUG:
            print(f'All job completed {start_height} to {end_height} during {time.time()-stime}')
        INDEX.close()

    dbb.cur.execute(f'''PRAGMA cache_size = -2000;''')
    if not RESUME:
        dbb.cur.execute(f'''PRAGMA page_size = 4096;''')
        dbb.cur.execute(f'''VACUUM;''')
    dbb.conn.commit()
    dbb.cur.execute(f'''PRAGMA journal_mode = WAL;''')
    dbb.cur.execute(f'''PRAGMA synchronous = NORMAL;''')
    dbb.conn.commit()
    dbb.close()


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--type', type=str, required=True,
                        choices=('index', 'core'),
                        help='The type for building database')
    parser.add_argument('--index', type=str,
                        help='The path for index database')
    parser.add_argument('--resume', action='store_true',
                        help='The resume or not')
    parser.add_argument('--prefix', type=str, default='dbv3',
                        help='The prefix of output database')
    parser.add_argument('--untrusted', type=int, default=100,
                        help='The block height that untrusted')
    parser.add_argument('--term', type=int, default=10000,
                        help='The term of block height')
    parser.add_argument('--process', type=int, 
                        default=min(multiprocessing.cpu_count()//2, 16),
                        help='The number of multiprocess')
    parser.add_argument('--pagesize', type=int, default=1024*4,
                        help='The page size of database (Max: 1024*64)')
    parser.add_argument('--cachesize', type=int, default=-1*2000,
                        help='The cache size of page (GBx1024×1024×1024÷(64×1024))')
    
    FLAGS, _ = parser.parse_known_args()

    FLAGS.output = os.path.abspath(
        os.path.expanduser(
            f'./{FLAGS.prefix}-{FLAGS.type}.db'))
    if FLAGS.index is not None:
        FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
    DEBUG = FLAGS.debug
    RESUME = FLAGS.resume

    main()
