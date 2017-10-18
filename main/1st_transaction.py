#!/usr/bin/env python3

# exit, argv
import sys
# csv
import csv
# interpret
import struct


def main(argv):
    # TODO(LuHa): check argument about analyze target blk file
    if len(argv) < 2:
        print('[BP] Need block file from bitcoin')
        return

    # TODO(LuHa): load blk file
    blk_input = open('blk00000.dat', 'rb').read()

    # TODO(LuHa): open and ready for output csv
    blk_output = open('output.csv', 'w')
    blk_csv = csv.DictWriter(blk_output, 
                       fieldnames = ['timestamp', 'from', 'to', 'btc'],
                       )

    # TODO(LuHa): check magic number
    c_ptr = 0
    result = struct.unpack('4s', blk_input[c_ptr:c_ptr+4])[0]
    result = bytearray(result)
    result.reverse()
    result = bytes(result)
    c_ptr = c_ptr+4
    # bitcoins magic number 0xD9B4BEF9, byte ordering: F9BEB4D9
    if result.hex().lower() != 'd9b4bef9':
        print('[BP] Not bitcoin blk')
        return
    print('[BP] Magic number', result.hex())

    # TODO(LuHa): block size
    result = struct.unpack('4s', blk_input[c_ptr:c_ptr+4])[0]
    result = bytearray(result)
    result.reverse()
    result = bytes(result)
    c_ptr = c_ptr+4
    print('[BP] Block size', result.hex())

    # TODO(LuHa): get transaction counter
    c_ptr = c_ptr+80 # pass blockheader
    result = struct.unpack('4s', blk_input[c_ptr:c_ptr+4])[0]
    result = bytearray(result)
    result.reverse()
    result = bytes(result)
    c_ptr = c_ptr+4
    print('[BP] Transaction counter', result.hex())





# Maybe it is good, right?
if __name__ == '__main__':
    sys.exit(main(sys.argv))
