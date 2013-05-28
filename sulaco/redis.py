import zlib

from bisect import bisect
from functools import partial
from tornado.gen import coroutine
from tornado.concurrent import return_future
from tornadoredis import (
    Client as BasicClient,
    ConnectionPool as BasicConnectionPool,
    Connection as BasicConnection)
from tornadoredis.client import Pipeline as BasicPipeline

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
    conn_cls = Connection

    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            return None
        self._created_connections += 1
        return self.conn_cls(**self.connection_kwargs)


class Client(BasicClient):
    pipeline_cls = None

    future_methods = (
        # keys, strings
        'get',
        'set',
        'delete',
        'setnx',
        'exists',
        'ttl',
        # hashes
        'hget',
        'hset')

    for mname in future_methods:
        locals()[mname] = return_future(getattr(BasicClient, mname))

    #TODO: use hiredis-py for parsing replies

    def pipeline(self, transactional=False):
        if not self._pipeline:
            self._pipeline = self.pipeline_cls(
                transactional=transactional,
                selected_db=self.selected_db,
                password=self.password,
                io_loop=self._io_loop,
            )
            self._pipeline.connection = self.connection
        return self._pipeline

    def __getattribute__(self, item):
        a = object.__getattribute__(self, item)
        if callable(a) and getattr(a, '__self__', None) is not None:
            a = partial(a.__func__, self._weak)
        return a


class Pipeline(Client, BasicPipeline):

    execute = return_future(BasicPipeline.execute)


Client.pipeline_cls = Pipeline


class RedisNodes(object):
    client_cls = Client # should be subclass of Client
    conn_pool_cls = ConnectionPool

    def __init__(self, *, nodes, default_max_connections=100,
                                 default_replicas=100,
                                 wait_for_available=True):
        self._hash_to_nodeinfo = {}
        self._hash_to_client = {}
        self.nodes = []
        for n in nodes:
            cli = self.client_cls(
                connection_pool=self.conn_pool_cls(
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
