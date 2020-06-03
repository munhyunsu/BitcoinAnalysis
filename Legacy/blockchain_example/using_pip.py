import os
import sys
from blockchain_parser.blockchain import Blockchain


def main():
    blockchain = Blockchain(os.path.expanduser('./blocks'))
    for block in blockchain.get_unordered_blocks():
        for tx in block.transactions:
            for no, output in enumerate(tx.outputs):
                print(tx.hash, output.addresses, output.value)
                sys.exit(0)


if __name__ == '__main__':
    main()
