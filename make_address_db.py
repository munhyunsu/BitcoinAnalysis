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
           ' balance REAL NOT NULL);')
    c.execute(sql)
    conn.commit()


def get_blocks(block_path, index_path, start=0, end=None):
    blockchain = Blockchain(block_path)

    for block in blockchain.get_ordered_blocks(index_path, start, end):
        yield block


def process_tx(block, tx):
    """
    {'block': None, 'txhash': None,
     'idx': None, 'address': None,
     'balance': None}
    """
    result = list()
    for index in range(0, len(tx.outputs)):
        try:
            out = tx.outputs[index]
            data = dict()
            data['block'] = block.height
            data['txhash'] = tx.hash
            data['idx'] = index
            data['address'] = out.addresses[0].address
            data['balance'] = out.value
        except IndexError:
            continue
        result.append(data)
    return result


def write_txs(conn, tx_data):
    c = conn.cursor()

    sql = ('INSERT OR REPLACE INTO transactions '
           ' (block, txhash, idx, address, balance)'
           ' VALUES'
           ' (:block, :txhash, :idx, :address, :balance);')
    c.executemany(sql, tx_data)
    conn.commit()


def get_max_height(conn):
    c = conn.cursor()
    sql = 'SELECT MAX(block) FROM transactions;'
    c.execute(sql)
    max_height = c.fetchone()[0]
    if type(max_height) != int:
        max_height = 0
    return max_height


def main(_):
    dprint('Parsed args: {0}'.format(FLAGS))
    dprint('Unparsed args: {0}'.format(_))

    # connect connect
    conn = get_connector(FLAGS.output)

    # prepare database
    create_db(conn, drop=FLAGS.reset)

    # restore inserted block
    pheight = max(0, get_max_height(conn)-1)
    dprint('Restored block height: {0}'.format(pheight))

    # loop blocks
    block_path = os.path.join(FLAGS.input, 'blocks')
    index_path = os.path.join(block_path, 'index')
    for block in get_blocks(block_path, index_path, start=pheight, 
                                                    end=100000):
        for tx in block.transactions:
            tx_data = process_tx(block, tx)
            # write data into db
            write_txs(conn, tx_data)
        dprint('Complete block height: {0}'.format(block.height))


if __name__ == '__main__':
    import argparse

    # parse intent
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str,
                        default='~/.bitcoin')
    parser.add_argument('--output', type=str,
                        default='./address.db')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--reset', action='store_true')

    FLAGS, _ = parser.parse_known_args()
    DEBUG = FLAGS.debug

    # standarization path
    FLAGS.input = os.path.abspath(os.path.expanduser(FLAGS.input))
    FLAGS.output = os.path.abspath(os.path.expanduser(FLAGS.output))

    main(_)

