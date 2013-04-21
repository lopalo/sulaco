import subprocess
import socket
import unittest
from collections import deque
from time import time, sleep
from os import path

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from sulaco.outer_server.tcp_server import SimpleProtocol
from sulaco.utils import Sender


class TimeoutError(Exception):
    pass


class BlockingClient(SimpleProtocol):

    def __init__(self):
        self._result = None
        self._timeout_error = None
        self._kwargs_contain = None
        self._path_prefix = None
        self._buffer = deque()
        self._loop = IOLoop.instance()
        self.s = Sender(self.send)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        stream = IOStream(sock)
        super(BlockingClient, self).__init__(stream)

    def connect(self, port, seconds=5):
        super(BlockingClient, self).connect(('', port))
        return self._wait(seconds)

    def recv(self, seconds=5, path_prefix='', kwargs_contain={}):
        msg = self._check_buffer(path_prefix, kwargs_contain)
        if msg is not None:
            return msg
        self._path_prefix = path_prefix
        self._kwargs_contain = kwargs_contain
        return self._wait(seconds)

    def _check_buffer(self, path_prefix, kwargs_contain):
        while self._buffer:
            msg = self._buffer.popleft()
            if not msg['path'].startswith(path_prefix):
                continue
            for k, v in kwargs_contain.items():
                if msg['kwargs'].get(k) != v:
                    continue
            return msg

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
        if self._path_prefix is None:
            self._buffer.append(msg)
            return
        if not msg['path'].startswith(self._path_prefix):
            return
        for k, v in self._kwargs_contain.items():
            if msg['kwargs'].get(k) != v:
                return
        self._result = msg
        self._loop.stop()

    def flush(self):
        self._buffer = deque()


class BasicFuncTest(unittest.TestCase):
    dirname = path.dirname(path.abspath(__file__))
    config = path.join(dirname, 'config.yaml')

    def setUp(self):
        self._servers = []
        self._clients = []
        self._locations = {}

        # setup broker
        p = path.join(self.dirname, '..', 'outer_server', 'message_broker.py')
        args = ['python', p, '-c', self.config]
        self._broker = subprocess.Popen(args)

        # setup location manager
        p = path.join(self.dirname, '..', 'location_server',
                                        'location_manager.py')
        args = ['python', p, '-c', self.config]
        self._locman = subprocess.Popen(args)

    def tearDown(self):
        for s in self._servers:
            s.terminate()
        for l in self._locations.values():
            l.terminate()
        self._broker.terminate()
        self._locman.terminate()
        for c in self._clients:
            c.close()

    def run_server(self, port, max_conn):
        self.run_servers((port, max_conn))

    def run_servers(self, *infos):
        for port, max_conn in infos:
            port = str(port)
            max_conn = str(max_conn)
            p = path.join(self.dirname, 'server.py')
            args = ['python', p, '-p', port, '-mc',
                    max_conn, '-c', self.config]
            s = subprocess.Popen(args)
            self._servers.append(s)
        sleep(.3)

    def run_location(self, ident, pub, pull):
        self.run_locations((ident, pub, pull))

    def run_locations(self, *infos):
        for ident, pub, pull in infos:
            assert ident not in self._locations
            p = path.join(self.dirname, 'location.py')
            args = ['python', p,
                    '-pub', pub,
                    '-pull', pull,
                    '-ident', ident,
                    '-c', self.config]
            l = subprocess.Popen(args)
            self._locations[ident] = l
        sleep(.3)

    def shutdown_location(self, ident):
        self._locations.pop(ident).terminate()

    def client(self):
        c = BlockingClient()
        self._clients.append(c)
        return c


