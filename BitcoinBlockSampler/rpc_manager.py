import collections

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

TIMEOUT = 60

class RPCManager(object):
    def __init__(self, rpc_user: str, rpc_password: str,
                 rpc_ip='127.0.0.1', rpc_port='8332',
                 timeout=60, retry=5,
                 cache=1024):
        self.rpc_user = rpc_user
        self.rpc_password = rpc_password
        self.rpc_ip = rpc_ip
        self.rpc_port = rpc_port
        self.rpc_endpoint = (f'http://{rpc_user}:{rpc_password}@'
                             f'{rpc_ip}:{rpc_port}')
        self.timeout = timeout
        self.retry = retry
        self.cache = cache
        self.blkhash = collections.OrderedDict()
        self.blk = collections.OrderedDict()
        self.tx = collections.OrderedDict()
        self.getconn()

    def getconn(self):
        self.rpc = AuthServiceProxy(self.rpc_endpoint, 
                                    timeout=self.timeout)
        return self.rpc

    def call(self, funcname, *args):
        t = 0
        while True:
            t = t + 1
            if t > self.retry:
                raise Exception(f'Exceed retry number! {func} {args}')
            try:
                func = getattr(self.rpc, funcname)
                return func(*args)
            except BrokenPipeError:
                self.getconn()
