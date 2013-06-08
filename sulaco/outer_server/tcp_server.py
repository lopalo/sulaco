import msgpack
import logging

from abc import ABCMeta, abstractmethod
from tornado.tcpserver import TCPServer as BasicTCPServer

from sulaco.outer_server.connection_manager import ConnectionHandler
from sulaco.utils import SubclassError


logger = logging.getLogger(__name__)


class TCPServer(BasicTCPServer):

    def setup(self, protocol, connman, root, max_conn=None):
        if not issubclass(protocol, ABCProtocol):
            raise SubclassError('protocol', ABCProtocol)
        if not issubclass(protocol, ConnectionHandler):
            raise SubclassError('protocol', ConnectionHandler)
        self._protocol = protocol
        self._connman = connman
        self._root = root
        self._max_conn = max_conn

    def handle_stream(self, stream, address):
        conn = self._protocol(stream)
        max_conn = self._max_conn
        if max_conn is not None and max_conn == self._connman.connection_count:
            logger.warning('Maximum of connection count was achieved')
            return
        conn.setup(self._connman, self._root)
        conn.on_open()


class ABCProtocol(object, metaclass=ABCMeta):

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def closed(self):
        return

    @abstractmethod
    def on_open(self):
        pass

    @abstractmethod
    def on_message(self, message):
        pass

    @abstractmethod
    def on_close(self):
        pass


class SimpleProtocol(ABCProtocol):
    _header_bytes = 10

    def __init__(self, stream):
        self._stream = stream
        stream.set_close_callback(self.on_close)

    def _on_header(self, data):
        content_size = int(data)
        self._stream.read_bytes(content_size, self._on_body)

    def _on_body(self, data):
        if not self._stream.closed():
            self._stream.read_bytes(self._header_bytes, self._on_header)
        self.on_message(msgpack.loads(data, encoding='utf-8'))

    def send(self, message):
        data = msgpack.dumps(message)
        dlen = str(len(data)).encode('utf-8')
        data = (self._header_bytes - len(dlen)) * b'0' + dlen + data
        self._stream.write(data, self.on_sent)

    def connect(self, address):
        self._stream.connect(address, self.on_open)

    def close(self):
        self._stream.close()

    def on_open(self, *args):
        self._stream.read_bytes(self._header_bytes, self._on_header)

    def on_sent(self):
        pass

    def closed(self):
        return self._stream.closed()

