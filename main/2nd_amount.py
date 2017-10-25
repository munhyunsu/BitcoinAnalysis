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
import base58

otime = 0
oinputs = list()
ooutputs = list()


def main(argv):
    global otime
    global oinputs
    global ooutputs
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

    try:
        while read_block(blk_input):
            print('{0} {1}->{2}'.format(otime, oinputs, ooutputs))
            otime = 0
            oinputs.clear()
            ooutputs.clear()
    except Exception:
        traceback.print_exception()


def read_block(blk):
    global otime
    # TODO(LuHa): check magic number
    magic_no = read_bytes(blk, 4, True)
    magic_no = magic_no.hex().upper()
    # bitcoins magic number 0xD9B4BEF9, byte ordering: F9BEB4D9
    if magic_no != 'D9B4BEF9':
    #    print('[BP] Not bitcoin blk')
        return False
    #print('[BP] Magic number:', magic_no)

    # TODO(LuHa): block size
    block_size = read_bytes(blk, 4)
    block_size = int.from_bytes(block_size, byteorder = 'little')
    #print('[BP] Block size:', block_size)

    # TODO(LuHa): version
    version = read_bytes(blk, 4)
    version = int.from_bytes(version, byteorder = 'little')
    #print('[BP] Version:', version)

    # TODO(LuHa): previous hash
    hash_prev_block = read_bytes(blk, 32)
    hash_prev_block = hash_prev_block.hex()
    #print('[BP] Previous hash:', hash_prev_block)

    # TODO(LuHa): merkle hash
    hash_merkle_root = read_bytes(blk, 32)
    hash_merkle_root = hash_merkle_root.hex()
    #print('[BP] Merkle hash:', hash_merkle_root)

    # TODO(LuHa): timestamp
    timestamp = read_bytes(blk, 4)
    timestamp = int.from_bytes(timestamp, byteorder = 'little')
    #print('[BP] Timestamp:', timestamp, 
    #              datetime.datetime.fromtimestamp(timestamp).strftime(
    #                      '%d.%m.%Y %H:%M'))
    otime = timestamp

    # TODO(LuHa): bits
    bits = read_bytes(blk, 4)
    bits = int.from_bytes(bits, byteorder = 'little')
    #print('[BP] Bits:', bits)

    # TODO(LuHa): nonce
    nonce = read_bytes(blk, 4)
    nonce = int.from_bytes(nonce, byteorder = 'little')
    #print('[BP] Nonce:', nonce)

    # TODO(LuHa): transaction counter
    transaction_counter = read_var_int(blk)
    #print('[BP] Transaction counter:', transaction_counter)

    for index in range(0, transaction_counter):
        read_transaction(blk)

    return True



def read_transaction(blk):
    # TODO(LuHa): version
    version = read_bytes(blk, 4)
    version = int.from_bytes(version, byteorder = 'little')
    #print('[BP] T Version:', version)

    # TODO(LuHa): in counter
    in_counter = read_var_int(blk)
    #print('[BP] T In counter:', in_counter)

    # TODO(LuHa): inputs
    for index in range(0, in_counter):
        read_inputs(blk)

    # TODO(LuHa): out counter
    out_counter = read_var_int(blk)
    #print('[BP] T Out counter:', out_counter)

    # TODO(LuHa): outputs
    for index in range(0, out_counter):
        read_outputs(blk)

    # TODO(LuHa): locktime
    locktime = read_bytes(blk, 4)
    locktime = int.from_bytes(locktime, byteorder = 'little')
    #print('[BP] T Locktime:', locktime)


def read_inputs(blk):
    global oinputs
    # TODO(LuHa): Previous transaction hash
    prev_tx_hash = read_bytes(blk, 32)
    prev_tx_hash = prev_tx_hash.hex()
    #print('[BP] TI Previous Txin hash:', prev_tx_hash)

    # TODO(LuHa): Previous txout index
    prev_tx_index = read_bytes(blk, 4)
    prev_tx_index = prev_tx_index.hex()
    #print('[BP] TI Previous Txout index:', prev_tx_index)

    # TODO(LuHa): Txin script length
    txin_script_length = read_var_int(blk)
    #print('[BP] TI Txin script length:', txin_script_length)

    # TODO(LuHa): Txin script
    txin_script = read_bytes(blk, txin_script_length)
    txin_script = txin_script.hex()
    #print('[BP] TI Txin script:', txin_script)

    # TODO(LuHa): Txin address
    txin_address = get_address_from_pubkey(txin_script)
    oinputs.append(txin_address)
    #print('[BP] TI Txin address:', txin_address)

    # TODO(LuHa): sequence number
    sequence_no = read_bytes(blk, 4)
    sequence_no = sequence_no.hex()
    #print('[BP] TI Sequence number:', sequence_no)



def read_outputs(blk):
    global ooutputs
    # TODO(LuHa): value
    value = read_bytes(blk, 8)
    value = int.from_bytes(value, byteorder = 'little')
    value = value / (100000000.0)
    #print('[BP] TO Value:', value)

    # TODO(LuHa): Txout script length
    txout_script_length = read_var_int(blk)
    #print('[BP] TO Txout script length:', txout_script_length)

    # TODO(LuHa): Txout script
    txout_script = read_bytes(blk, txout_script_length)
    txout_script = txout_script.hex()
    #print('[BP] TO Txout script:', txout_script)

    # TODO(LuHa): Txout address
    txout_address = get_address_from_pubkey(txout_script)
    #print('[BP] TO Txout address:', txout_address)
    ooutputs.append((txout_address, value))


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



def get_address_from_pubkey(pubkey):
    result = None
    op_code = pubkey[0:2]
    op_code = int(op_code, 16)
    if 1 <= op_code and op_code <= 75:
        pub = pubkey[2:-2]
        h3 = hashlib.sha256(binascii.unhexlify(pub))
        h4 = hashlib.new('ripemd160', h3.digest())
        result = (b'\x00') + h4.digest()
        h5 = hashlib.sha256(result)
        h6 = hashlib.sha256(h5.digest())
        result += h6.digest()[:4]
        result = base58.b58encode(result)
    if pubkey.lower().startswith('76a914'):
        pub = pubkey[6:-4]
        result = (b'\x00') + binascii.unhexlify(pub)
        h5 = hashlib.sha256(result)
        h6 = hashlib.sha256(h5.digest())
        result += h6.digest()[:4]
        result = base58.b58encode(result)
        
    return result



def b58encode(data):
    """ encode v, which is a string of bytes, to base58.        
    """
    __b58chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    __b58base = len(__b58chars)


    long_value = int(data.hex(), 16)

    result = ''
    while long_value >= __b58base:
        div, mod = divmod(long_value, __b58base)
        result = __b58chars[mod] + result
        long_value = div
    result = __b58chars[long_value] + result

    # Bitcoin does a little leading-zero-compression:
    # leading 0-bytes in the input become leading-1s
    nPad = 0
    for c in data:
        if c == '\0': nPad += 1
        else: break

    return (__b58chars[0]*nPad) + result



# Maybe it is good, right?
if __name__ == '__main__':
    sys.exit(main(sys.argv))
