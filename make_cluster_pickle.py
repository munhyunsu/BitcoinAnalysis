import sys 
import os 
from blockchain_parser.blockchain import Blockchain
import pickle
import sqlite3

BLOCK = os.path.expanduser('~/.bitcoin/blocks')
INDEX = os.path.expanduser('~/.bitcoin/blocks/index')
OUTDIR = os.path.expanduser('./pic/multi')
ADDRESSDB = os.path.expanduser('./pic/addresses.pickle')
ADB = os.path.expanduser('./address.db')


conn = sqlite3.connect(ADB)
c = conn.cursor()


if os.path.exists(ADDRESSDB):
    with open(ADDRESSDB, 'rb') as f:
        ADDR = pickle.load(f)
else:
    ADDR = [0, dict()]


def prepare_dirs():
    os.makedirs(OUTDIR, exist_ok=True)


def archive_result(data, name):
    path = os.path.join(OUTDIR, '{0}.pickle'.format(name))
    with open(path, 'wb') as f:
        pickle.dump(data, f)


def build_address_db(blockchain, index, end, start=0):
    for block in blockchain.get_ordered_blocks(index, start, end):
        if ADDR[0] >= block.height:
            continue
        for tx in block.transactions:
            txout = ADDR[1].get(tx.hash, list())
            for out in tx.outputs:
                try:
                    txout.append((out.addresses[0].address, out.value))
                except IndexError:
                    pass
                    #print(block.height, tx)
                    #raise KeyboardInterrupt
            ADDR[1][tx.hash] = txout
        ADDR[0] = block.height
        print('Builing ', ADDR[0])

def get_txin_address(conn, txhash, idx):
    pass


def is_std_tx(tx):
    if len(tx.inputs) == 0:
        return False
    if len(tx.inputs) == 2:
        return False
    if len(tx.outputs) != 1:
        return False
    return True

def get_addr(txhash, txidx):
    sql = ('SELECT address FROM transactions '
           'WHERE txhash == "{0}" AND '
           'idx == "{1}";').format(txhash, txidx)
    print(sql)
    c.execute(sql)
    value = c.fetchone()
    if value is None:
        raise KeyError
    return value[0]


def process_block(block):
    candy = set()
    cluster = dict()
    for tx in block.transactions:
        if not is_std_tx(tx):
            continue

        ads = set()
        for ins in tx.inputs:
            try:
                #ad = ADDR[1][ins.transaction_hash][ins.transaction_index][0]
                ad = get_addr(ins.transaction_hash, ins.transaction_index)
            except KeyError:
                continue
            ads.add(ad)

        for ad in ads:
            if ad not in cluster.keys():
                cluster[ad] = ads
            else:
                cluster[ad].update(ads)

        candy.update(ads)

    return candy, cluster


def main():
    prepare_dirs()

    blockchain = Blockchain(BLOCK)
    #build_address_db(blockchain, INDEX, end=200001)
    #print('Building Address DB done')

    for block in blockchain.get_ordered_blocks(INDEX, start=200000, end=210000):
        result = process_block(block)
        archive_result(result, block.height)
        print('Done: {0:d} - {1:s}'.format(block.height, block.hash))


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        with open(ADDRESSDB, 'wb') as f:
            pickle.dump(ADDR, f)

