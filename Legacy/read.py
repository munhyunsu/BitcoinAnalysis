def read_bytes(blk, size, reverse = False):
    result = blk.read(size)
    if reverse:
        result = bytearray(result)
        result.reverse()
        result = bytes(result)
    return result


def main():
    blk = open('./blocks/blk00000.dat', 'rb')

    magic_no = read_bytes(blk, 4, reverse=True)
    magic_no = magic_no.hex().upper()
    print(magic_no)


if __name__ == '__main__':
    main()
