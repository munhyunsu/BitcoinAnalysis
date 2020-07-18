from address_convertor import pubkey_to_address, get_pubkey

def vout_addrs_from_tx(tx):
    addrs = list()
    n = -1
    for vout in tx['vout']:
        if vout['scriptPubKey']['type'] in ('pubkeyhash', 'scripthash', 
                                            'witness_v0_keyhash', 'witness_v0_scripthash',
                                            'witness_unknown', 'multisig'):
            for addr in vout['scriptPubKey']['addresses']:
                addrs.append(addr)
        elif vout['scriptPubKey']['type'] in ('pubkey', 'nonstandard', 'nulldata'):
            try:
                for addr in [pubkey_to_address(get_pubkey(vout['scriptPubKey']['hex']))]:
                    addrs.append(addr)
            except UnboundLocalError:
                for addr in [f'{tx["txid"]}{n}']:
                    addrs.append(addr)
        else:
            raise Exception(f'BUG!! {theight}\n{tx}\n{vout}')

    return addrs
