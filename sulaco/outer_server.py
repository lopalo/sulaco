from collections import defaultdict
from tornado.netutil import TCPServer as BasicTCPServer

from sulaco.utils.receiver import Sender, dispatch

class TCPServer(BasicTCPServer):

    def setup(self, protocol, connman, root, max_conn=None):
        #TODO: check if protocol is sublclass of abstract
        #protocol with methods 'send', 'close' and 'send_and_close'
        if not issubclass(protocol, ConnectionHandler):
            raise Exception('Should be a subclass of ConnectionHandler')
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
                conn.sender.error(msg='max_connections')
                conn.send_and_close()


class ConnectionHandler(object):
    """
    Should be first in list of basic classes, if used as mixin
    """

    def setup(self, connman, root):
        self._connman = connman
        self._root = root
        self.sender = Sender(self)

    def on_open(self, *args):
        super(ConnectionHandler, self).on_open(*args)
        self._connman.add_connection(self)

    def on_message(self, message):
        super(ConnectionHandler, self).on_message(message)
        path = message.pop('path').split('.')
        message['connman'] = self._connman
        message['conn'] = self
        dispatch(self._root, 0, path, message)

    def on_close(self):
        super(ConnectionHandler, self).on_close()
        self._connman.remove_connection(self)
        #TODO: maybe notify all connected rooms and do final actions


class ConnectionManager(object):
    _connections = set()

    _uid_to_connection = {}
    _connection_to_uid = {}

    _rooms_to_connection = defaultdict(set)
    _connection_to_rooms = defaultdict(set)

    def add_connection(self, conn):
        assert conn not in self._connections
        self._connections.add(conn)

    def bind_connection_to_uid(self, conn, uid):
        self._connection_to_uid[conn] = uid
        self._uid_to_connection[uid] = conn

    def add_connection_to_room(self, conn, room):
        self._connection_to_rooms[conn].add(room)
        self._rooms_to_connection[room].add(conn)

    def remove_connection(self, conn):
        self._connections.remove(conn)
        uid = self._connection_to_uid.pop(conn, None)
        if uid is not None:
            del self._uid_to_connection[uid]
        rooms = self._connection_to_rooms.pop(conn, [])
        for r in rooms:
            r.remove(conn)

    @property
    def connections_count(self):
        return len(self._connections)


