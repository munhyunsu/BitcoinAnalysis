import hashlib


def get_nonce(hash_value):
    nonce = 0
    while True:
        test_hash = hash_value.copy()
        nonce_bytes = int(nonce).to_bytes(32, byteorder='big')
        test_hash.update(nonce_bytes)
        if test_hash.hexdigest()[0:4] == '0000':
            print('{0}번 반복'.format(nonce+1))
            return int(nonce).to_bytes(32, byteorder='big')
        nonce = nonce + 1


def print_block(data):
    for index in range(0, (len(data)//32)+1):
        print(data[index*32:(index+1)*32].hex())


def main():
    print('블록체인에 넣을 값을 입력하세요.')
    data = input('값: ')
    data_utf8 = data.encode('utf-8')

    hash_value_init = hashlib.new('sha256')

    hash_value_init.update(b'\x00'*32)
    hash_value_init.update(data_utf8)

    print('Init Strings: {0}'.format(hash_value_init.hexdigest()))

    hash_value = hash_value_init.copy()

    for index in range(1, 6):
        data_bytes = hash_value.digest()
        hash_value = hashlib.sha256()
        hash_value.update(data_bytes)
        block = data_bytes
        print('블록체인에 넣을 값을 입력하세요.')
        data = input('값: ')
        data_utf8 = data.encode('utf-8')
        hash_value.update(data_utf8)
        block = block + data_utf8
        nonce = get_nonce(hash_value)
        hash_value.update(nonce)
        block = block + nonce
        print('{0} Strings: {1}'.format(index, hash_value.hexdigest()))
        print_block(block)


if __name__ == '__main__':
    main()

