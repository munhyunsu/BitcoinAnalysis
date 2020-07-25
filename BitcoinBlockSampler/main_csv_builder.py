import os
import time
import csv
import multiprocessing

from secret import rpc_user, rpc_password
from time_manager import get_time
from rpc_manager import RPCManager
from json_parser import vout_addrs_from_tx

FLAGS = None
_ = None
DEBUG = False
RPCM = dict()


def get_data(height):
    pid = os.getpid()
    blks = list()
    txes = list()
    addrs = list()
    rpcm = RPCM.get(pid, RPCManager(rpc_user, rpc_password))
    block_hash = rpcm.call('getblockhash', height)
    block = rpcm.call('getblock', block_hash, 2)
    blks.append((height, block_hash))
    for tx in block['tx']:
        txes.append((tx['txid'],))
        for addr in vout_addrs_from_tx(tx):
            addrs.append((addr,))
    RPCM[pid] = rpcm
    return blks, txes, addrs


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    rpcm = RPCManager(rpc_user, rpc_password)
    blk_file = open(FLAGS.blk, 'w')
    tx_file = open(FLAGS.tx, 'w')
    addr_file = open(FLAGS.addr, 'w')
    blk_writer = csv.writer(blk_file, lineterminator=os.linesep)
    tx_writer = csv.writer(tx_file, lineterminator=os.linesep)
    addr_writer = csv.writer(addr_file, lineterminator=os.linesep)
    blk_writer.writerow(('height', 'blkhash'))
    tx_writer.writerow(('txid',))
    addr_writer.writerow(('addr',))
    
    term = FLAGS.term
    start_height = 0
    best_block_hash = rpcm.call('getbestblockhash')
    best_block = rpcm.call('getblock', best_block_hash)
    end_height = best_block['height'] - FLAGS.untrusted
    if DEBUG:
        print((f'Best Block Heights: {best_block["height"]}, '
               f'Time: {get_time(best_block["time"]).isoformat()}'))
        print(f'Start from {start_height} to {end_height}')
    pool_num = min(multiprocessing.cpu_count()//2, 4)

    stime = time.time()
    try:
        for sheight, eheight in zip(range(start_height, end_height, term), 
                                    range(start_height+term, end_height+term, term)):
            if eheight >= end_height:
                eheight = end_height+1
            with multiprocessing.Pool(pool_num) as p:
                try:
                    results = p.imap(get_data, range(sheight, eheight))
                    for blks, txes, addrs in results:
                        blk_writer.writerows(blks)
                        tx_writer.writerows(txes)
                        addr_writer.writerows(addrs)
                except KeyboardInterrupt:
                    print(f'KeyboardInterrupt detected. Terminate child processes.')
                    p.terminate()
                    p.join(10)
                    raise KeyboardInterrupt
            if DEBUG:
                print(f'Job done from {sheight} to {eheight-1} during {time.time()-stime}')
    except KeyboardInterrupt:
        print(f'Closing...')
    finally:
        blk_file.close()
        tx_file.close()
        addr_file.close()
        if DEBUG:
            print(f'All job completed {start_height} to {end_height} during {time.time()-stime}')


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
    parser.add_argument('--basedb', type=str, 
                        help='The base index database (in core / util type db)')

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
