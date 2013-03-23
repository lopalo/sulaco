import subprocess
import socket
import unittest
from time import time, sleep
from os import path

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from sulaco.tcp_server import SimpleProtocol
from sulaco.utils.receiver import Sender


class TimeoutError(Exception):
    pass


class BlockingClient(SimpleProtocol):

    def __init__(self):
        self._result = None
        self._timeout_error = None
        self._kwargs_contain = None
        self._path_prefix = None
        self._loop = IOLoop.instance()
        self.s = Sender(self)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        stream = IOStream(sock)
        super(BlockingClient, self).__init__(stream)

    def connect(self, port, seconds=5):
        super(BlockingClient, self).connect(('', port))
        return self._wait(seconds)

    def recv(self, seconds=5, path_prefix='', kwargs_contain={}):
        self._path_prefix = path_prefix
        self._kwargs_contain = kwargs_contain
        return self._wait(seconds)

    def send(self, msg, seconds=5):
        super(BlockingClient, self).send(msg)
        return self._wait(seconds)

    def _wait(self, seconds):
        loop = self._loop
        def on_timeout():
            self._timeout_error = TimeoutError('{} seconds expired'.
                                                    format(seconds))
            loop.stop()
        timeout = loop.add_timeout(time() + seconds, on_timeout)
        loop.start()
        loop.remove_timeout(timeout)
        error = self._timeout_error
        result = self._result
        self._timeout_error = None
        self._result = None
        self._kwargs_contain = None
        self._path_prefix = None
        if error is not None:
            raise error
        return result

    def on_open(self, *args):
        super(BlockingClient, self).on_open(*args)
        self._loop.stop()

    def on_sent(self):
        super(BlockingClient, self).on_sent()
        self._loop.stop()

    def on_message(self, msg):
        super(BlockingClient, self).on_message(msg)
        if not msg['path'].startswith(self._path_prefix):
            return
        for k, v in self._kwargs_contain.items():
            if msg['kwargs'].get(k) != v:
                return
        self._result = msg
        self._loop.stop()


class BasicFuncTest(unittest.TestCase):

    def setUp(self):
        self._servers = {}
        self._clients = []

    def tearDown(self):
        for s in self._servers.values():
            s.terminate()
        for c in self._clients:
            c.close()

    def run_server(self, port, max_conn=5):
        assert port not in self._servers
        port = str(port)
        max_conn = str(max_conn)
        p = path.join(path.dirname(path.abspath(__file__)), 'test_server.py')
        args = ['python', p, '-p', port, '-c', max_conn]
        s = subprocess.Popen(args)
        self._servers[port] = s
        sleep(0.1)

    def client(self):
        c = BlockingClient()
        self._clients.append(c)
        return c


