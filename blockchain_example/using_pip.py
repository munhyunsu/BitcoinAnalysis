import os
from blockchain_parser.blockchain import Blockchain


def main():
    blockchain = Blockchain(os.path.expanduser('~/SharedFolder/.bitcoin/blocks'))
    for block in blockchain.get_unordered_blocks():
        for tx in block.transactions:
            for no, output in enumerate(tx.outputs):
                if len(output.addresses) > 1:
                    print(tx.hash, output.addresses, output.value)


if __name__ == '__main__':
    main()
