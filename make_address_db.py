import os
import sqlite3

from blockchain_parser.blockchain import Blockchain

FLAGS = None
DEBUG = True


def dprint(data):
    if DEBUG:
        print(data)


def get_connector(db):
    conn = sqlite3.connect(db)
    return conn


def create_db(conn, drop=False):
    c = conn.cursor()

    if drop:
        sql = ('DROP TABLE transactions;')
        c.execute(sql)
    sql = ('CREATE TABLE IF NOT EXISTS transactions (\n'
           ' block INTEGER NOT NULL,\n'
           ' txhash TEXT NOT NULL,\n'
           ' idx INTEGER NOT NULL,\n'
           ' address TEXT NOT NULL,\n'
           ' balance REAL NOT NULL,\n'
           ' utxo INTEGER DEFAULT False);')
    c.execute(sql)
    conn.commit()


def get_blocks(block_path, index_path, start=0, end=None):
    blockchain = Blockchain(block_path)

    for block in blockchain.get_ordered_blocks(index_path, start, end):
        yield block


def process_tx(block, tx):
    


def main(_):
    dprint('Parsed args: {0}'.format(FLAGS))
    dprint('Unparsed args: {0}'.format(_))

    # connect connect
    conn = get_connector(FLAGS.output)

    # prepare database
    create_db(conn, drop=True)

    # loop blocks
    block_path = os.path.join(FLAGS.input, 'blocks')
    index_path = os.path.join(block_path, 'index')
    for block in get_blocks(block_path, index_path, start=200000, end=200010):
        for tx in block.transactions:
            tx_data = process_tx(block, tx)
            print(tx_data)



if __name__ == '__main__':
    import argparse

    # parse intent
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str,
                        default='~/.bitcoin')
    parser.add_argument('--output', type=str,
                        default='./address.db')
    parser.add_argument('--debug', action='store_true')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    # standarization path
    FLAGS.input = os.path.abspath(os.path.expanduser(FLAGS.input))
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))

    main(_)

