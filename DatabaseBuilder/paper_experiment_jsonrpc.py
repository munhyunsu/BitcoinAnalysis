import argparse
import os
import time
import multiprocessing
import csv

import pandas as pd

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

import secret

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def get_block(height):
    global FLAGS
    global DEBUG
    global RPCM

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
    
    return height

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Index read: {height:<7d}', end='\r')

    return data


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Job start')

    bestblockhash = rpc.getbestblockhash()
    bestblock = rpc.getblock(bestblockhash, 2)
    
    data = []
    for start, stop in utils.get_range(0, bestblock['height']+1, FLAGS.bulk):
        if DEBUG:
            print(f'[{int(time.time()-STIME)}] Index insert {start} ~ {stop}')
        with multiprocessing.Pool(FLAGS.process) as p:
            results = p.imap(get_block, range(start, stop), FLAGS.chunksize)
            for result in results:
                continue
        data.append((start, stop, int(time.time()-STIME)))
    df = pd.DataFrame(data, columns=['Start', 'End', 'Time'])

    os.makedirs(FLAGS.output, exist_ok=True)
    path = os.path.join(FLAGS.output, f'{int(STIME)}_{bestblock["height"]}_{FLAGS.process}_{FLAGS.chunksize}.csv')
    df.to_csv(path)

    if DEBUG:
        print(f'[{int(time.time()-STIME)}] Job done')


if __name__ == '__main__':
    root_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(root_path)
    os.chdir(root_dir)

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true',
                         help='The present debug message')
    parser.add_argument('--rpctimeout', type=int, default=60,
                        help='The rpc timeout secounds')
    parser.add_argument('--bulk', type=int, default=100000,
                        help='The bulk process height')
    parser.add_argument('--process', type=int,
                        default=min(multiprocessing.cpu_count()//2, 16),
                        help='The number of multiprocess')
    parser.add_argument('--chunksize', type=int, default=1,
                        help='The multiprocess chunksize')
    parser.add_argument('--outputdir', type=str,
                        default='./experiment_jsonrpc',
                        help='The jsonrpc experiment directory')

    FLAGS, _ = parser.parse_known_args()
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))
    DEBUG = FLAGS.debug

    main()
