import argparse
import os
import time
import csv

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

import secret

FLAGS = _ = None
DEBUG = False
STIME = time.time()


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    csvfile = open('rpc_experiment.csv', 'w')
    csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL,
                           lineterminator=os.linesep)

    csvwriter.writerow(['Height', 'Time'])
    rpc = AuthServiceProxy((f'http://{secret.rpcuser}:{secret.rpcpassword}@'
                            f'{secret.rpchost}:{secret.rpcport}'),
                           timeout=FLAGS.rpctimeout)

    bestblockhash = rpc.getbestblockhash()
    bestblock = rpc.getblock(bestblockhash, 2)
    
    for i in range(0, bestblock['height']+1):
        time1 = time.time()
        blockhash = rpc.getblockhash(i)
        block = rpc.getblock(blockhash, 2)
        time2 = time.time()
        csvwriter.writerow([i, time2-time1])
        if i % FLAGS.bulk == 0:
            print(f'[{int(time.time()-STIME)}] JSON RPC Call {i}')
        else:
            print(f'[{int(time.time()-STIME)}] JSON RPC Call {i}', end='\r')
    print(f'[{int(time.time()-STIME)}] JSON RPC Call {i}')
    
    csvfile.close()


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


    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    main()
