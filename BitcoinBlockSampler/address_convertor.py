# Wow, It if very very difficulty
# 578755 block processing: 3141d7e07774f6e9d2d9266bd2070d996b0ae946b464acd5dbe47007da4faf31
## 0 index txid of above: d663ed61f6320d768edf518ae272f96ac52b95a9417368e6c01565d2bcbe9be2
## the block hash of above: 00000000000000000015f2df4e3fdbafa3b12726de3308064c51fed3dd85e1da
# https://github.com/bitcoinjs/bitcoinjs-lib/issues/1000
## https://en.bitcoin.it/wiki/File:PubKeyToAddr.png

import hashlib

def pubkey_to_address(pubkey):
    def b58check_encode(s):
        def divide(x, a):
            return x//a, x%a
        
        code_string = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
        x = int(s.hex(), 16)

        output_string = ''

        while x:
            x, remainder = divide(x, 58)
            output_string = output_string + code_string[remainder]

        pad_len = len(s)
        s0 = s.lstrip(b'\x00')
        pad_len -= len(s0)

        for i in range(pad_len):
            output_string = output_string + code_string[0]

        return output_string[::-1]
        
    h1 = hashlib.new('sha256')
    h1.update(bytes.fromhex(pubkey))
    h2 = hashlib.new('ripemd160')
    h2.update(hashlib.sha256(bytes.fromhex(pubkey)).digest())
    a = '00' + h2.hexdigest()
    
    h3 = hashlib.new('sha256')
    h3.update(bytes.fromhex(a))
    h4 = hashlib.new('sha256')
    h4.update(h3.digest())
    b = h4.hexdigest()[:8]
    c = bytes.fromhex(a+b)

    return b58check_encode(c)

def get_pubkey(script):
    h = bytes.fromhex(script)
    for i in range(len(h)):
        if 0x01 <= h[i] <= 0x4b:
            p = i
            break
    l = h[p]
    
    return h[p+1:p+l+1].hex()