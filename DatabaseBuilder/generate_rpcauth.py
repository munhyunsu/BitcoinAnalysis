'''
## Bitcoin core's RPC user credential creation source code
- [Bitcoin Core Github](https://github.com/bitcoin/bitcoin)
- [rpcuser](https://github.com/bitcoin/bitcoin/blob/master/share/rpcauth/rpcauth.py)

### LISENSE
- Copyright (c) 2015-2018 The Bitcoin Core developers
- Distributed under the MIT software license, see the accompanying
- file COPYING or http://www.opensource.org/licenses/mit-license.php.
'''

# Library setup
from base64 import urlsafe_b64encode
from binascii import hexlify
from getpass import getpass
from os import urandom
import hmac

# Function defind
def generate_salt(size):
    """Create size byte hex salt"""
    return hexlify(urandom(size)).decode()

def generate_password():
    """Create 32 byte b64 password"""
    return urlsafe_b64encode(urandom(32)).decode('utf-8')

def password_to_hmac(salt, password):
    m = hmac.new(bytearray(salt, 'utf-8'), bytearray(password, 'utf-8'), 'SHA256')
    return m.hexdigest()


def main():
    username = input('Username: ')
    print('Password: ', end='')
    password = getpass()

    if password == '':
        password = generate_password()

    salt = generate_salt(16)
    password_hmac = password_to_hmac(salt, password)

    print('String to be appended to bitcoin.conf:')
    print(f'rpcauth={username}:{salt}${password_hmac}')
    print(f'Your username:\n{username}')
    print(f'Your password:\n{password}')
    print(f'If you want to use other source code in this repository then you shoud to create secret.py with rpcauth')
    print(f'String to be appended to secret.py:')
    print(f"rpc_username = '{username}'\nrpc_password = '{password}'")


if __name__ == '__main__':
    main()
