#!/usr/bin/env python3

# TODO(LuHa): re-coding base58encode function
#             reference: https://bitcoin.org/en/developer-reference#address-conversion
def b58encode(data):
    """
    encode data with base58 rule
    """
    # prepare encode
    code_string = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    x = int.from_bytes(data, byteorder = 'big')
    # get zero pading length
    pad = len(data) - len(data.lstrip(b'\x00'))

    output_string = ''

    while x > 0:
        (x, remainder) = divmod(x, 58)
        output_string = code_string[remainder] + output_string

    return (code_string[0] * pad + output_string)



def b58decode(data):
    """
    decode base58 data to bytes
    """
    pass
