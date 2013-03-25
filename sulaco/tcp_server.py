from abc import ABCMeta, abstractmethod
import json
from tornado.netutil import TCPServer as BasicTCPServer

from sulaco.connection_manager import ConnectionHandler, MAX_CONNECTION_ERROR


class TCPServer(BasicTCPServer):

    def setup(self, protocol, connman, root, max_conn=None):
        if not issubclass(protocol, ConnectionHandler):
            raise Exception('Should be a subclass of ConnectionHandler')
        if not issubclass(protocol, ABCProtocol):
            raise Exception('Should be a subclass of ABCProtocol')
        self._protocol = protocol
        self._connman = connman
        self._root = root
        self._max_conn = max_conn

    def handle_stream(self, stream, address):
        conn = self._protocol(stream)
        conn.setup(self._connman, self._root)
        conn.on_open()
        max_conn = self._max_conn
        if max_conn is not None:
            if max_conn + 1 == self._connman.connections_count:
                conn.s.error(msg=MAX_CONNECTION_ERROR)
                conn.send_and_close()


class ABCProtocol(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def send_and_close(self):
        pass


class SimpleProtocol(ABCProtocol):
    _header_bytes = 10

    def __init__(self, stream):
        self._send_and_close = False
        self._stream = stream
        stream.set_close_callback(self.on_close)

    def _on_header(self, data):
        content_size = int(data)
        self._stream.read_bytes(content_size, self._on_body)

    def _on_body(self, data):
        if not self._stream.closed():
            self._stream.read_bytes(self._header_bytes, self._on_header)
        self.on_message(json.loads(data))

    def send(self, message):
        data = json.dumps(message)
        dlen = str(len(data))
        data = (self._header_bytes - len(dlen)) * '0' + dlen + data
        self._stream.write(data, self.on_sent)

    def connect(self, address):
        self._stream.connect(address, self.on_open)

    def send_and_close(self):
        self._send_and_close = True

    def close(self):
        self._stream.close()

    def on_open(self, *args):
        self._stream.read_bytes(self._header_bytes, self._on_header)

    def on_sent(self):
        if self._send_and_close:
            self.close()

    def on_message(self, mesage):
        pass

    def on_close(self):
        pass


