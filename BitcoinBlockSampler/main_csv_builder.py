import os
import time
import csv

from secret import rpc_user, rpc_password
from time_manager import get_time
from rpc_manager import RPCManager
from json_parser import vout_addrs_from_tx

FLAGS = None
_ = None
DEBUG = False


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    rpcm = RPCManager(rpc_user, rpc_password)

    start_height = 0
    best_block_hash = rpcm.call('getbestblockhash')
    best_block = rpcm.call('getblock', best_block_hash)
    end_height = best_block['height'] - FLAGS.untrusted + 1
    if DEBUG:
        print((f'Best Block Heights: {best_block["height"]}, '
               f'Time: {get_time(best_block["time"]).isoformat()}'))
        print(f'Start from {start_height} to {end_height}')

    stime = time.time()
    mtime = time.time()
    blk_file = open(FLAGS.blk, 'w')
    tx_file = open(FLAGS.tx, 'w')
    addr_file = open(FLAGS.addr, 'w')
    blk_writer = csv.writer(blk_file, lineterminator=os.linesep)
    tx_writer = csv.writer(tx_file, lineterminator=os.linesep)
    addr_writer = csv.writer(addr_file, lineterminator=os.linesep)
    try:
        for height in range(start_height, end_height):
            block_hash = rpcm.call('getblockhash', height)
            block = rpcm.call('getblock', block_hash, 2)
            blk_writer.writerow((height, block_hash))
            for tx in block['tx']:
                tx_writer.writerow((tx['txid']))
                for addr in vout_addrs_from_tx(tx):
                    addr_writer.writerow((addr,))
            if DEBUG:
                print(f'Processing {height} after {time.time()-stime}', 
                      end='\r')
    except KeyboardInterrupt:
        print(f'\nKeyboardInterrupt detected. Commit transactions.')
        if DEBUG:
            print(f'Processing from {start_height} to {end_height} during {time.time()-stime}')
    finally:
        blk_file.close()
        tx_file.close()
        addr_file.close()


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

    FLAGS, _ = parser.parse_known_args()

    FLAGS.blk = os.path.abspath(
        os.path.expanduser(
            f'./{FLAGS.prefix}-{FLAGS.type}-blk.csv'))
    FLAGS.tx = os.path.abspath(
        os.path.expanduser(
            f'./{FLAGS.prefix}-{FLAGS.type}-tx.csv'))
    FLAGS.addr = os.path.abspath(
        os.path.expanduser(
            f'./{FLAGS.prefix}-{FLAGS.type}-addr.csv'))
    
    DEBUG = FLAGS.debug

    main()
