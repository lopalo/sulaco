import zlib

from bisect import bisect
from tornado.gen import coroutine
from tornado.concurrent import return_future
from tornadoredis import (
    Client as BasicClient,
    ConnectionPool as BasicConnectionPool,
    Connection as BasicConnection)


class Connection(BasicConnection):

    def on_stream_close(self):
        if self._stream:
            self._stream = None
            callbacks = self.read_callbacks
            self.read_callbacks = []
            for callback in callbacks:
                # fixes bug when callback takes no argument
                arg_count = callback.__code__.co_argcount
                if arg_count == 1:
                    callback(None)
                else:
                    callback()


class ConnectionPool(BasicConnectionPool):

    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            return None
        self._created_connections += 1
        return Connection(**self.connection_kwargs)


class Client(BasicClient):

    get = return_future(BasicClient.get)
    set = return_future(BasicClient.set)
    setnx = return_future(BasicClient.setnx)
    exists = return_future(BasicClient.exists)

    hget = return_future(BasicClient.hget)
    hset = return_future(BasicClient.hset)

    #TODO: use hiredis-py for parsing replies


class RedisNodes(object):
    client_cls = Client # should be subclass of Client
    conn_poop_cls = ConnectionPool

    def __init__(self, *, nodes, default_max_connections=100,
                                 default_replicas=100,
                                 wait_for_available=True):
        self._hash_to_nodeinfo = {}
        self._hash_to_client = {}
        self.nodes = []
        for n in nodes:
            cli = self.client_cls(
                connection_pool=self.conn_poop_cls(
                    host=n.get('host'),
                    port=n.get('port'),
                    unix_socket_path=n.get('unix_socket_path'),
                    max_connections=n.get('max_connections',
                                    default_max_connections),
                    wait_for_available=wait_for_available),
                selected_db=n['db'],
                password=n.get('password'))
            self.nodes.append((n, cli))
            for num in range(n.get('replicas', default_replicas)):
                _hash = self.hash_func('{}: {}'.format(n['name'], num))
                self._hash_to_nodeinfo[_hash] = n
                self._hash_to_client[_hash] = cli

        self._hash_list = sorted(self._hash_to_client)

    def hash_func(self, string):
        return zlib.crc32(string.encode('utf-8'))

    def _get_node_hash(self, key):
        _hash = self.hash_func(key)
        idx = bisect(self._hash_list, _hash)
        if idx == len(self._hash_list):
            idx = 0
        return self._hash_list[idx]

    def get_client(self, key):
        _node_hash = self._get_node_hash(key)
        return self._hash_to_client[_node_hash]

    def get_info(self, key):
        _node_hash = self._get_node_hash(key)
        return self._hash_to_nodeinfo[_node_hash]

    def __getitem__(self, key):
        return self.get_client(key)

    @coroutine
    def check_nodes(self):
        """ Checks connections with all nodes and setups db_name """

        for info, cli in self.nodes:
            yield cli.setnx('db_name', info['name'])

#TODO: replace json using msgpack
