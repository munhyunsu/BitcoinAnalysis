import os
import time
import sqlite3
import multiprocessing

from secret import rpc_user, rpc_password
from time_manager import get_time
from db_manager import QUERY, DBBuilder
from rpc_manager import RPCManager
from json_parser import vout_addrs_from_tx

FLAGS = None
_ = None
DEBUG = False
RPCM = None


def get_data(height):
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
        for addr in vout_addrs_from_tx(tx):
            addrs.append((addr,))
    RPCM = rpcm
    return blks, txes, addrs


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    dbb = DBBuilder(FLAGS.type, FLAGS.output)
    dbb.cur.execute(f'''PRAGMA journal_mode = OFF;''')
    dbb.cur.execute(f'''PRAGMA synchronous = OFF;''')
    dbb.cur.execute(f'''PRAGMA page_size = {FLAGS.pagesize};''')
    dbb.cur.execute(f'''PRAGMA page_size = {FLAGS.pagesize};''')
    dbb.cur.execute(f'''PRAGMA cache_size = {FLAGS.cachesize};''')
    dbb.cur.execute(f'''VACUUM;''')
    dbb.conn.commit()
    rpcm = RPCManager(rpc_user, rpc_password)

    term = FLAGS.term
    start_height = dbb.select('SELECT_MAX_BLKID')
    if start_height is None:
        start_height = 0
    start_height = max(start_height - FLAGS.untrusted, 0)
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
    try:
        for sheight, eheight in zip(range(start_height, end_height, term), 
                                    range(start_height+term, end_height+term, term)):
            if eheight >= end_height:
                eheight = end_height + 1
            dbb.begin()
            with multiprocessing.Pool(pool_num) as p:
                try:
                    results = p.imap(get_data, range(sheight, eheight))
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
    except KeyboardInterrupt:
        print(f'KeyboardInterrupt detected. Commit transactions.')
        try:
            dbb.commit()
        except sqlite3.OperationalError:
            pass
    finally:
        try:
            dbb.commit()
        except sqlite3.OperationalError:
            pass
    if DEBUG:
        print(f'All job completed {start_height} to {end_height} during {time.time()-stime}')

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
    parser.add_argument('--type', type=str, required=True,
                        choices=('index', 'core', 'util'),
                        help='The type for building database')
    parser.add_argument('--prefix', type=str, default='dbv3',
                        help='The prefix of output database')
    parser.add_argument('--debug', action='store_true',
                        help='The present debug message')
    parser.add_argument('--untrusted', type=int, default=100,
                        help='The block height that untrusted')
    parser.add_argument('--term', type=int, default=10000,
                        help='The term of block height')
    parser.add_argument('--process', type=int, 
                        default=min(multiprocessing.cpu_count()//2, 4),
                        help='The number of multiprocess')
    parser.add_argument('--pagesize', type=int, default=1024*64,
                        help='The page size of database')
    parser.add_argument('--cachesize', type=int, default=-1*1024*16,
                        help='The cache size of page')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.output = os.path.abspath(
        os.path.expanduser(
            f'./{FLAGS.prefix}-{FLAGS.type}.db'))
    DEBUG = FLAGS.debug

    main()
