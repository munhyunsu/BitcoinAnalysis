import os
import time


from secret import rpc_user, rpc_password
from db_manager import QUERY, DBBuilder
from rpc_manager import RPCManager

FLAGS = None
_ = None
DEBUG = False


def main():
    if DEBUG:
        print(f'Parsed arguments {FLAGS}')
        print(f'Unparsed arguments {_}')
    dbb = DBBuilder(FLAGS.type, FLAGS.output)
    rpcm = RPCManager(rpc_user, rpc_password)

    for h in range(1, 10):
        blkhash = rpcm.getblockhash(h)
        blk = rpcm.getblock(blkhash)
        for tx in blk['tx']:
            print(blk['hash'][-8:], len(tx['vin']), len(tx['vout']))

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

    FLAGS, _ = parser.parse_known_args()

    FLAGS.output = os.path.abspath(
        os.path.expanduser(
            f'./{FLAGS.prefix}-{FLAGS.type}.db'))
    DEBUG = FLAGS.debug

    main()
