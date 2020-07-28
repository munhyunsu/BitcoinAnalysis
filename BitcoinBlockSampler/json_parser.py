from address_convertor import pubkey_to_address, get_pubkey

def vin_tx_n_from_tx(tx):
    tx_n = list()
    n = -1
    for vin in tx['vin']:
        tx = vin['txid']
        n = vin['vout']
        tx_n.append((txid, n))

    return tx_n


def addr_btc_from_tx_n(tx, n):
    results = list()
    vout = tx['vout'][n]
    for addr, value in addrs_from_vout(vout):
        results.append(addr, value)
    return results


def addrs_from_vout(txid, vout):
    addrs = list()
    if vout['scriptPubKey']['type'] in ('pubkeyhash', 'scripthash', 
                                        'witness_v0_keyhash', 'witness_v0_scripthash',
                                        'witness_unknown', 'multisig'):
        for addr in vout['scriptPubKey']['addresses']:
            addrs.append((addr, float(vout['value'])))
    elif vout['scriptPubKey']['type'] in ('pubkey', 'nonstandard', 'nulldata'):
        try:
            for addr in [pubkey_to_address(get_pubkey(vout['scriptPubKey']['hex']))]:
                addrs.append((addr, float(vout['value'])))
        except UnboundLocalError:
            for addr in [f'{txid}{vout["n"]}']:
                addrs.append((addr, float(vout['value'])))
    else:
        raise Exception(f'BUG!! {theight}\n{tx}\n{vout}')
    return addrs

def vout_tx_n_from_tx(tx):
    tx_n = list()
    n = -1
    for vin in tx['vin']:
        tx = vin['txid']
        n = vin['vout']
        tx_n.append((txid, n))

    return tx_n

def vout_addrs_from_tx(tx):
    results = list()
    n = -1
    txid = tx['txid']
    for vout in tx['vout']:
        for addr, value in addrs_from_vout(txid, vout):
            results.append((addr, value))

    return results
