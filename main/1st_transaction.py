#!/usr/bin/env python3

# exit, argv
import sys
# csv
import csv
# interpret
import struct
# binascii
import binascii
# datetime
import datetime
# hashlib
import hashlib
# base58: https://github.com/keis/base58
#import base58
# traceback
import traceback

# base58, address_from_pubkey
import utils

def main(argv):
    # TODO(LuHa): check argument about analyze target blk file
    if len(argv) < 2:
        print('[BP] Need block file from bitcoin')
        return

    # TODO(LuHa): load blk file
    blk_input = open(argv[1], 'rb')

    # TODO(LuHa): open and ready for output csv
    #blk_output = open('output.csv', 'w')
    #blk_csv = csv.DictWriter(blk_output,
    #                   fieldnames = ['timestamp', 'from', 'to', 'btc'],
    #                   )

    num = 1
    try:
        while read_block(blk_input):
            print('\x1B[37;5;3m[BP] Block number of this file:\x1B[0m', num)
            num = num+1
    except:
        traceback.print_exc()
        print('Error line:', hex(blk_input.tell()))


def read_block(blk):
    # TODO(LuHa): check magic number
    magic_no = read_bytes(blk, 4, reverse = True)
    magic_no = magic_no.hex().upper()
    # bitcoins magic number 0xD9B4BEF9, byte ordering: F9BEB4D9
    if magic_no != 'D9B4BEF9':
        print('[BP] Not bitcoin blk')
        return False
    print('[BP] Magic number: 0x{0}'.format(magic_no))

    # TODO(LuHa): block size
    block_size = read_bytes(blk, 4)
    block_size = int.from_bytes(block_size, byteorder = 'little')
    print('[BP] Block size:', block_size)

    ### Begin of block header: 80 bytes
    # TODO(LuHa): version
    version = read_bytes(blk, 4, reverse = True)
    version = version.hex()
    print('[BP] Version: 0x{0}'.format(version))

    # TODO(LuHa): previous hash
    hash_prev_block = read_bytes(blk, 32, reverse = True)
    hash_prev_block = hash_prev_block.hex()
    print('[BP] Previous hash:', hash_prev_block)

    # TODO(LuHa): merkle hash
    hash_merkle_root = read_bytes(blk, 32, reverse = True)
    hash_merkle_root = hash_merkle_root.hex()
    print('[BP] Merkle hash:', hash_merkle_root)

    # TODO(LuHa): timestamp
    timestamp = read_bytes(blk, 4)
    timestamp = int.from_bytes(timestamp, byteorder = 'little')
    print('[BP] Timestamp:', timestamp,
                  datetime.datetime.fromtimestamp(timestamp,
                  datetime.timezone.utc).strftime('%Y.%m.%d %H:%M:%S'))

    # TODO(LuHa): bits
    bits = read_bytes(blk, 4)
    bits = int.from_bytes(bits, byteorder = 'little')
    print('[BP] Bits:', bits)

    # TODO(LuHa): nonce
    nonce = read_bytes(blk, 4)
    nonce = int.from_bytes(nonce, byteorder = 'little')
    print('[BP] Nonce:', nonce)
    ### End of block header

    # TODO(LuHa): transaction counter
    transaction_counter = read_var_int(blk)
    print('[BP] Transaction counter:', transaction_counter)

    # TODO(LuHa): read codebase
    #read_codebase(blk)

    # TODO(LuHa): read transacion
    for index in range(0, transaction_counter):
        read_transaction(blk)

    return True



def read_transaction(blk):
    ### begin of raw transaction: 4+v+v+v+v+4 Bytes
    # TODO(LuHa): version
    version = read_bytes(blk, 4, reverse = True)
    version = version.hex()
    print('[BP] T Version: 0x{0} {1}'.format(version, hex(blk.tell()-4)))

    # TODO(LuHa): split point
    #             https://github.com/bitcoin/bips/blob/master/bip-0141.mediawiki
    in_counter = read_var_int(blk)
    # TODO(LuHa): split inputs's form to original
    #             or the witness the soft fork
    if in_counter == 0:
        read_witness(blk)
        return

    # TODO(LuHa): in counter of the original
    print('[BP] T In counter:', in_counter)
    for index in range(0, in_counter):
        read_inputs(blk)

    # TODO(LuHa): out counter
    out_counter = read_var_int(blk)
    print('[BP] T Out counter:', out_counter)

    # TODO(LuHa): outputs
    for index in range(0, out_counter):
        read_outputs(blk)

    # TODO(LuHa): locktime
    locktime = read_bytes(blk, 4)
    locktime = int.from_bytes(locktime, byteorder = 'little')
    print('[BP] T Locktime:', locktime, hex(blk.tell()-4))
    ### end of raw transaction


def read_witness(blk):
    # TODO(LuHa): flag
    flag = read_bytes(blk, 1)
    flag = flag.hex()
    print('[BP] TW Flag: 0x{0}'.format(flag))

    # TODO(LuHa): in counter
    in_counter = read_var_int(blk)
    print('[BP] TW In counter:', in_counter)

    # TODO(LuHa): in counter of the original
    for index in range(0, in_counter):
        read_inputs(blk)

    # TODO(LuHa): out counter
    out_counter = read_var_int(blk)
    print('[BP] TW Out counter:', out_counter)

    # TODO(LuHa): outputs
    for index in range(0, out_counter):
        read_outputs(blk)

    # TODO(LuHa): witness data for txin
    for index in range(0, in_counter):
        # TODO(LuHa): witness counter
        witness_counter = read_var_int(blk)
        print('[BP] TW Witness counter:', witness_counter)
        # TODO(LuHa): witness data
        for index2 in range(0, witness_counter):
            witness_length = read_var_int(blk)
            print('[BP] TW Witness length:', witness_length)
            witness_data = read_bytes(blk, witness_length)
            witness_data = witness_data.hex()
            print('[BP] TW Witness data: 0x{0}'.format(witness_data))

    # TODO(LuHa): locktime
    locktime = read_bytes(blk, 4)
    locktime = int.from_bytes(locktime, byteorder = 'little')
    print('[BP] T Locktime:', locktime, hex(blk.tell()-4))



def read_inputs(blk):
    ### begin of TxIn: 36+v+v+4 Bytes
    # TODO(LuHa): Previous transaction hash
    prev_tx_hash = read_bytes(blk, 36)
    prev_tx_hash = prev_tx_hash.hex()
    print('[BP] TI Previous Txin hash: 0x{0}'.format(prev_tx_hash))

    # TODO(LuHa): Txin script length
    txin_script_length = read_var_int(blk)
    print('[BP] TI Txin script length:', txin_script_length)

    # TODO(LuHa): Txin script
    txin_script = read_bytes(blk, txin_script_length)
    txin_script = txin_script.hex()
    print('[BP] TI Txin script:', txin_script)

    # TODO(LuHa): Txin address
    if len(txin_script) > 0:
        txin_address = utils.get_address_from_pubkey(txin_script)
        print('[BP] TI Txin address:', txin_address)

    # TODO(LuHa): sequence number
    sequence_no = read_bytes(blk, 4)
    sequence_no = sequence_no.hex()
    print('[BP] TI Sequence number:', sequence_no)



def read_outputs(blk):
    # TODO(LuHa): value
    value = read_bytes(blk, 8)
    value = int.from_bytes(value, byteorder = 'little')
    value = value / (100000000.0)
    print('[BP] TO Value:', value)

    # TODO(LuHa): Txout script length
    txout_script_length = read_var_int(blk)
    print('[BP] TO Txout script length:', txout_script_length)

    # TODO(LuHa): Txout script
    txout_script = read_bytes(blk, txout_script_length)
    txout_script = txout_script.hex()
    print('[BP] TO Txout script:', txout_script)

    # TODO(LuHa): Txout address
    txout_address = utils.get_address_from_pubkey(txout_script)
    print('[BP] TO Txout address:', txout_address)



def read_bytes(blk, size, reverse = False):
    result = blk.read(size)
    if reverse:
        result = bytearray(result)
        result.reverse()
        result = bytes(result)
    return result



def read_var_int(blk):
    var_int = blk.read(1)
    var_int = ord(var_int)
    result = 0
    if var_int < 0xFD:
        result = var_int
    elif var_int == 0xFD:
        result = read_bytes(blk, 2)
        result = int.from_bytes(result, byteorder = 'little')
    elif var_int == 0xFE:
        result = read_bytes(blk, 4)
        result = int.from_bytes(result, byteorder = 'little')
    elif var_int == 0xFF:
        result = read_bytes(blk, 8)
        result = int.from_bytes(result, byteorder = 'little')

    return result



# Maybe it is good, right?
if __name__ == '__main__':
    sys.exit(main(sys.argv))
