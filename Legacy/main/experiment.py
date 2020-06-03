#!/usr/bin/env python3

import utils
import base58


string = bytes('문현수', encoding = 'utf-8')
string = b'\x00\x00\x01' + string + b'\x00\x00\x01'

print(utils.b58encode(string))
print(base58.b58encode(string))
