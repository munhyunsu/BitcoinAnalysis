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
# traceback
import traceback


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
            print('[BP] Block number of this file:', num)
            num = num+2
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
    print('[BP] Magic number:', magic_no)

    # TODO(LuHa): block size
    block_size = read_bytes(blk, 4)
    block_size = int.from_bytes(block_size, byteorder = 'little')
    print('[BP] Block size:', block_size)

    # Begin of block header: 80 bytes
    # TODO(LuHa): version
    #version = read_bytes(blk, 4)
    #version = int.from_bytes(version, byteorder = 'little')
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
    # End of block header

    # TODO(LuHa): transaction counter
    transaction_counter = read_var_int(blk)
    print('[BP] Transaction counter:', transaction_counter)

    # TODO(LuHa): first transacion of block
    #read_codebase(blk)

    # TODO(LuHa): read transacion
    for index in range(0, transaction_counter):
        read_transaction(blk)

    return True



def read_transaction(blk):
    # TODO(LuHa): version
    #version = read_bytes(blk, 4)
    #version = int.from_bytes(version, byteorder = 'little')
    version = read_bytes(blk, 4, reverse = True)
    version = version.hex()
    #print('[BP] T Version: 0x{0}'.format(version))
    print('[BP] T Version: 0x{0} {1}'.format(version, hex(blk.tell())))

    # TODO(LuHa): in counter
    in_counter = read_var_int(blk)
    print('[BP] T In counter:', in_counter)

    # TODO(LuHa): in_counter zero exception
#    if in_counter == 0:
#        junction = read_bytes(blk, 1)
#        junction = read_bytes(blk, 1)
#        junction = junction.hex()
#        if junction == '01':
#            read_inzero(blk)
#        #elif junction == '03':
#        else:
#            read_inscript(blk)
#        return

    # TODO(LuHa): inputs
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
    print('[BP] T Locktime:', locktime)


def read_inscript(blk):
    # TODO(LuHa): script counter
    script_counter = read_var_int(blk)
    print('[BP] IS script counter:', script_counter)

    # TODO(LuHa): script
    for index in range(0, script_counter):
        script_length = read_var_int(blk)
        print('[BP] IS script length:', script_length)
        script = read_bytes(blk, script_length)
        script = script.hex()
        print('[BP] IS script: 0x{0}'.format(script))

    # TODO(LuHa): sequence
    sequence = read_bytes(blk, 4)
    sequence = sequence.hex()
    print('[BP] IS Sequence: 0x{0}'.format(sequence))
        
    # TODO(LuHa): out counter
    out_counter = read_var_int(blk)
    print('[BP] IZ Out counter:', out_counter)

    # TODO(LuHa): outputs
    for index in range(0, out_counter):
        read_outputs(blk)

    # TODO(LuHa): unknown script counter
    unknown_counter = read_var_int(blk)
    print('[BP] IZ Unknown script counter:', unknown_counter)

    # TODO(LuHa): unknown script
    for index in range(0, unknown_counter):
        script_size = read_var_int(blk)
        print('[BP] IZ Unknown script size:', script_size)
        script = read_bytes(blk, script_size)
        script = script.hex()
        print('[BP] IZ Unknown script 0x{0}'.format(script))

    # TODO(LuHa): unknown locktime
    locktime = read_bytes(blk, 4)
    locktime = int.from_bytes(locktime, byteorder = 'little')
    print('[BP] iz Unknown locktime:', locktime)
    


def read_inzero(blk):
    # TODO(LuHa): i dont know
    unknown = read_bytes(blk, 1)
    unknown = unknown.hex()
    print('[BP] IZ unknown: 0x{0}'.format(unknown))
    
    # TODO(LuHa): hash
    hash_cb = read_bytes(blk, 32)
    hash_cb = hash_cb.hex()
    print('[BP] IZ unknown hash: 0x{0}'.format(hash_cb))

    # TODO(LuHa): index
    index = read_bytes(blk, 5)
    index = index.hex()
    print('[BP] IZ Index: 0x{0}'.format(index))


    # TODO(LuHa): height size
    #height_size = read_bytes(blk, 1)
    #height_size = int.from_bytes(height_size, byteorder = 'little')
    #print('[BP] IZ Height size:', height_size)

    # TODO(LuHa): height
    #height = read_bytes(blk, height_size) # temporary
    #height = read_bytes(blk, 2)
    #height = int.from_bytes(height, byteorder = 'little')
    #print('[BP] IZ Height:', height)

    # TODO(LuHa): data
    #data = read_bytes(blk, height_size)
    #data = data.hex()
    #print('[BP] IZ Data: 0x{0}'.format(data))

    # TODO(LuHa): sequence
    sequence = read_bytes(blk, 4)
    sequence = sequence.hex()
    print('[BP] IZ Sequence: 0x{0}'.format(sequence))

    # TODO(LuHa): out counter
    out_counter = read_var_int(blk)
    print('[BP] IZ Out counter:', out_counter)

    # TODO(LuHa): outputs
    for index in range(0, out_counter):
        read_outputs(blk)

    # TODO(LuHa): unknown script counter
    unknown_counter = read_var_int(blk)
    print('[BP] IZ Unknown script counter:', unknown_counter)

    # TODO(LuHa): unknown script
    for index in range(0, unknown_counter):
        script_size = read_var_int(blk)
        print('[BP] IZ Unknown script size:', script_size)
        script = read_bytes(blk, script_size)
        script = script.hex()
        print('[BP] IZ Unknown script 0x{0}'.format(script))

    # TODO(LuHa): unknown locktime
    locktime = read_bytes(blk, 4)
    locktime = int.from_bytes(locktime, byteorder = 'little')
    print('[BP] iz Unknown locktime:', locktime)
    


def read_codebase(blk):
    # TODO(LuHa): version
    #version = read_bytes(blk, 4)
    #version = int.from_bytes(version, byteorder = 'little')
    version = read_bytes(blk, 4, reverse = True)
    version = version.hex()
    print('[BP] T Version: 0x{0}'.format(version))

    # TODO(LuHa): in counter
    in_counter = read_var_int(blk)
    print('[BP] T In counter:', in_counter)

    # TODO(LuHa): i dont know
    unknown = read_bytes(blk, 2)
    unknown = unknown.hex()
    print('[BP] CB unknown: 0x{0}'.format(unknown))
    
    # TODO(LuHa): hash
    hash_cb = read_bytes(blk, 32)
    hash_cb = hash_cb.hex()
    print('[BP] CB Codebase hash: 0x{0}'.format(hash_cb))

    # TODO(LuHa): index
    index = read_bytes(blk, 4)
    index = index.hex()
    print('[BP] CB Index: 0x{0}'.format(index))

    # TODO(LuHa): script bytes
    script_bytes = read_var_int(blk)
    print('[BP] CB Script bytes:', script_bytes)

    # TODO(LuHa): height size
    height_size = read_bytes(blk, 1)
    height_size = int.from_bytes(height_size, byteorder = 'little')
    print('[BP] CB Height size:', height_size)

    # TODO(LuHa): height
    #height = read_bytes(blk, height_size) # temporary
    height = read_bytes(blk, 3)
    height = int.from_bytes(height, byteorder = 'little')
    print('[BP] CB Height:', height)

    # TODO(LuHa): data
    data = read_bytes(blk, script_bytes - 4)
    data = data.hex()
    print('[BP] CB Data: 0x{0}'.format(data))

    # TODO(LuHa): sequence
    sequence = read_bytes(blk, 4)
    sequence = sequence.hex()
    print('[BP] CB Sequence: 0x{0}'.format(sequence))

    # TODO(LuHa): out counter
    out_counter = read_var_int(blk)
    print('[BP] CB Out counter:', out_counter)

    # TODO(LuHa): outputs
    for index in range(0, out_counter):
        read_outputs(blk)

    # TODO(LuHa): unknown script counter
    unknown_counter = read_var_int(blk)
    print('[BP] CB Unknown script counter:', unknown_counter)

    # TODO(LuHa): unknown script
    for index in range(0, unknown_counter):
        script_size = read_var_int(blk)
        print('[BP] CB Unknown script size:', script_size)
        script = read_bytes(blk, script_size)
        script = script.hex()
        print('[BP] CB Unknown script 0x{0}'.format(script))

    # TODO(LuHa): unknown locktime
    locktime = read_bytes(blk, 4)
    locktime = int.from_bytes(locktime, byteorder = 'little')
    print('[BP] CB Unknown locktime:', locktime)
    



    


def read_inputs(blk):
    # TODO(LuHa): Previous transaction hash
    prev_tx_hash = read_bytes(blk, 32)
    prev_tx_hash = prev_tx_hash.hex()
    print('[BP] TI Previous Txin hash:', prev_tx_hash)

    # TODO(LuHa): Previous txout index
    prev_tx_index = read_bytes(blk, 4)
    prev_tx_index = prev_tx_index.hex()
    print('[BP] TI Previous Txout index:', prev_tx_index)

    # TODO(LuHa): Txin script length
    txin_script_length = read_var_int(blk)
    print('[BP] TI Txin script length:', txin_script_length)

    # TODO(LuHa): Txin script
    txin_script = read_bytes(blk, txin_script_length)
    txin_script = txin_script.hex()
    print('[BP] TI Txin script:', txin_script)

    # TODO(LuHa): Txin address
    txin_address = get_address_from_pubkey(txin_script)
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
    txout_address = get_address_from_pubkey(txout_script)
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
