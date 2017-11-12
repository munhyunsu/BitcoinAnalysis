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

# TODO(LuHa): re-coding address translation function
#             In now, this function decode some script not all
#             reference: https://bitcoin.org/en/developer-reference#address-conversion
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
