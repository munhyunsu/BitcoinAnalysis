import rocksdb


def main():
    column_family = dict()
    options = rocksdb.Options()

    column_family[b'nonstandard_nested'] = rocksdb.ColumnFamilyOptions()
    column_family[b'pubkey_nested'] = rocksdb.ColumnFamilyOptions()
    column_family[b'multisig_pubkey_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'scripthash_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'multisig_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'nulldata_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'witness_scripthash_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'witness_pubkeyhash_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'pubkeyhash_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'pubkey_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'nonstandard_output'] = rocksdb.ColumnFamilyOptions()
    column_family[b'witness_scripthash_nested'] = rocksdb.ColumnFamilyOptions()
    column_family[b'witness_pubkeyhash_nested'] = rocksdb.ColumnFamilyOptions()
    column_family[b'nulldata_nested'] = rocksdb.ColumnFamilyOptions()
    column_family[b'multisig_nested'] = rocksdb.ColumnFamilyOptions()
    column_family[b'scripthash_nested'] = rocksdb.ColumnFamilyOptions()
    column_family[b'multisig_pubkey_nested'] = rocksdb.ColumnFamilyOptions()
    column_family[b'pubkeyhash_nested'] = rocksdb.ColumnFamilyOptions()

    db = rocksdb.DB('/home/harny/ResearchData/bitcoin-data/addressesDb', options, column_families=column_family)


if __name__ == '__main__':
    main()

