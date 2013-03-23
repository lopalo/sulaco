from collections import defaultdict
from sulaco.utils.receiver import Sender, dispatch, SignError

SIGN_ERROR = 'sign_error'
MAX_CONNECTION_ERROR = 'max_connections_error'

class ConnectionHandler(object):
    """
    Should be first in list of basic classes, if used as mixin
    """

    def setup(self, connman, root):
        self._connman = connman
        self._root = root
        self.s = Sender(self)

    def on_open(self, *args):
        super(ConnectionHandler, self).on_open(*args)
        self._connman.add_connection(self)

    def on_message(self, message):
        super(ConnectionHandler, self).on_message(message)
        path = message['path'].split('.')
        uid = self._connman.get_uid(self)
        signed = uid is not None
        kwargs = message['kwargs']
        kwargs['connman'] = self._connman
        kwargs['conn'] = self
        if uid is not None:
            kwargs['uid'] = uid
        try:
            dispatch(self._root, 0, path, signed, kwargs)
        except SignError:
            self._on_sign_error()

    def on_close(self):
        super(ConnectionHandler, self).on_close()
        self._connman.remove_connection(self)
        #TODO: maybe notify all connected rooms and do final actions

    def _on_sign_error(self):
        self.s.error(msg=SIGN_ERROR)

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
        assert uid not in self._uid_to_connection, 'uid already exists'
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

    def send_by_uid(self, uid, msg):
        conn = self._uid_to_connection.get(uid)
        if conn is None:
            return False
        conn.send(msg)
        return True

    @property
    def connections_count(self):
        return len(self._connections)

    def get_uid(self, conn):
        return self._connection_to_uid.get(conn)

