import os
import time
import sqlite3

from secret import rpc_user, rpc_password
from time_manager import get_time
from db_manager import QUERY, DBBuilder
from rpc_manager import RPCManager
from json_parser import vout_addrs_from_tx

FLAGS = None
_ = None
DEBUG = False


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    dbb = DBBuilder(FLAGS.type, FLAGS.output)
    rpcm = RPCManager(rpc_user, rpc_password)

    start_height = dbb.select('SELECT_MAX_BLKID')
    if start_height is None:
        start_height = 0
    start_height = max(start_height - FLAGS.untrusted, 0)
    best_block_hash = rpcm.call('getbestblockhash')
    best_block = rpcm.call('getblock', best_block_hash)
    end_height = best_block['height'] - FLAGS.untrusted + 1
    if DEBUG:
        print((f'Best Block Heights: {best_block["height"]}, '
               f'Time: {get_time(best_block["time"]).isoformat()}'))
        print(f'Start from {start_height} to {end_height}')

    stime = time.time()
    mtime = time.time()
    blks = list()
    txes = list()
    addrs = list()
    try:
        for height in range(start_height, end_height):
            if height%FLAGS.chunk == 0:
                dbb.insertmany('INSERT_BLKID', blks)
                dbb.insertmany('INSERT_TXID', txes)
                dbb.insertmany('INSERT_ADDRID', addrs)
                try:
                    dbb.commit()
                except sqlite3.OperationalError:
                    pass
                blks.clear()
                txes.clear()
                addrs.clear()
                print(f'Job done {height} during {time.time()-mtime}')
                mtime = time.time()
                dbb.begin()
            block_hash = rpcm.call('getblockhash', height)
            block = rpcm.call('getblock', block_hash, 2)
            blks.append((height, block_hash))
            for tx in block['tx']:
                txes.append((tx['txid'],))
                for addr in vout_addrs_from_tx(tx):
                    addrs.append((addr,))
            if DEBUG:
                print(f'Processing {height} after {time.time()-stime}', 
                      end='\r')
    except KeyboardInterrupt:
        print(f'\nKeyboardInterrupt detected. Commit transactions.')
        try:
            dbb.commit()
        except sqlite3.OperationalError:
            pass
    finally:
        dbb.insertmany('INSERT_BLKID', blks)
        dbb.insertmany('INSERT_TXID', txes)
        dbb.insertmany('INSERT_ADDRID', addrs)
        try:
            dbb.commit()
        except sqlite3.OperationalError:
            pass

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
    parser.add_argument('--chunk', type=int, default=10000,
                        help='The chunk size that commit')

    FLAGS, _ = parser.parse_known_args()

    FLAGS.output = os.path.abspath(
        os.path.expanduser(
            f'./{FLAGS.prefix}-{FLAGS.type}.db'))
    DEBUG = FLAGS.debug

    main()
