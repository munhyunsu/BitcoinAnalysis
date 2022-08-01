from address_convertor import pubkey_to_address, get_pubkey


def addr_btc_from_vout(txid, vout):
    results = list()
    if vout['scriptPubKey']['type'] in ('pubkeyhash', 'scripthash', 
                                        'witness_v0_keyhash', 'witness_v0_scripthash',
                                        'witness_v1_taproot',
                                        'witness_unknown', 'multisig'):
        if 'address' in vout['scriptPubKey']: # compatibility Bitcoin-Core
            results.append((vout['scriptPubKey']['address'], float(vout['value'])))
        else:
            try:
                for addr in vout['scriptPubKey']['addresses']:
                    results.append((addr, float(vout['value'])))
            except KeyError:
                raise Exception(f'Exception {txid}\n{vout}')
    elif vout['scriptPubKey']['type'] in ('pubkey', 'nonstandard', 'nulldata'):
        try:
            for addr in [pubkey_to_address(get_pubkey(vout['scriptPubKey']['hex']))]:
                results.append((addr, float(vout['value'])))
        except UnboundLocalError:
            for addr in [f'{txid}{vout["n"]}']:
                results.append((addr, float(vout['value'])))
    else:
        raise Exception(f'BUG!! {txid}\n{vout}')
    return results
