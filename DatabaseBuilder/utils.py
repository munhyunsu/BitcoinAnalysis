import sys
import datetime
import hashlib

import mariadb

TZUTC = datetime.timezone(datetime.timedelta())
TZSEOUL = datetime.timezone(datetime.timedelta(hours=9))


def connectdb(user, password, host, port, database):
    try:
        conn = mariadb.connect(user=user,
                               password=password,
                               host=host,
                               port=port,
                               database=database)
    except mariadb.Error as e:
        print(f'Error connecting to MariaDB: {e}')
        sys.exit(1)
    cur = conn.cursor()
    
    return conn, cur

def gettime(timestamp, tz=TZUTC):
    return datetime.datetime.fromtimestamp(timestamp, tz=tz)

def getunixtime(timestr):
    return int(datetime.datetime.fromisoformat(timestr).timestamp())

# Wow, It if very very difficulty
# 578755 block processing: 3141d7e07774f6e9d2d9266bd2070d996b0ae946b464acd5dbe47007da4faf31
## 0 index txid of above: d663ed61f6320d768edf518ae272f96ac52b95a9417368e6c01565d2bcbe9be2
## the block hash of above: 00000000000000000015f2df4e3fdbafa3b12726de3308064c51fed3dd85e1da
## https://en.bitcoin.it/wiki/File:PubKeyToAddr.png
### Ref1: https://en.bitcoin.it/wiki/Technical_background_of_version_1_Bitcoin_addresses
### Ref2: https://en.bitcoin.it/wiki/Base58Check_encoding
### Ref3: https://en.bitcoin.it/wiki/Script
### Ref4: https://github.com/bitcoinjs/bitcoinjs-lib/issues/1000
### Ref5(Minor): https://stackoverflow.com/questions/46328870/getting-hash160-bitcoin-address-in-python
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

def addr_btc_from_vout(vout):
    results = list()
    if vout['scriptPubKey']['type'] in ('pubkeyhash', 'scripthash', 
                                        'witness_v0_keyhash', 'witness_v0_scripthash',
                                        'witness_v1_taproot',
                                        'witness_unknown', 'multisig'):
        for addr in vout['scriptPubKey']['addresses']:
            results.append((addr, float(vout['value'])))
    elif vout['scriptPubKey']['type'] in ('pubkey', 'nonstandard', 'nulldata'):
        try:
            for addr in [pubkey_to_address(get_pubkey(vout['scriptPubKey']['hex']))]:
                results.append((addr, float(vout['value'])))
        except UnboundLocalError:
            for addr in [f'{vout["n"]}']:
                results.append((addr, float(vout['value'])))
    else:
        raise Exception(f'BUG!! \n{vout}')
    return results