import unittest
import zmq
from mock import Mock, call
from sulaco.outer_server.tcp_server import SimpleProtocol
from sulaco.outer_server.connection_manager import (
    DistributedConnectionManager,
    LocationMixin, ConnectionHandler)


class Protocol(ConnectionHandler, SimpleProtocol):
    pass


class TestDistributedConnectionManager(unittest.TestCase):

    def setUp(self):
        self.connman = DistributedConnectionManager(pub_socket=Mock(),
                                                    sub_socket=Mock())

    def get_connection(self):
        conn = Protocol(Mock())
        conn.setup(self.connman, None)
        return conn

    def test_bind_connection_to_uid(self):
        conn = self.get_connection()
        conn.on_open()
        connman = self.connman
        connman.bind_connection_to_uid(conn, 111)
        self.assertEqual({conn: 111}, connman._connection_to_uid)
        self.assertEqual({111: conn}, connman._uid_to_connection)
        connman._sub_socket.setsockopt.assert_called_with(zmq.SUBSCRIBE,
                                                          b'send_by_uid:111')

    def test_add_connection_to_channel(self):
        conn = self.get_connection()
        conn.on_open()
        connman = self.connman
        connman.add_connection_to_channel(conn, 'chan')
        self.assertEqual({'chan': {conn,}}, connman._channel_to_connections)
        self.assertEqual({conn: {'chan',}}, connman._connection_to_channels)
        connman._sub_socket.setsockopt.assert_called_with(zmq.SUBSCRIBE,
                                                b'publish_to_channel:chan')

    def test_remove_connection_from_channel(self):
        conn = self.get_connection()
        conn.on_open()
        connman = self.connman
        connman.add_connection_to_channel(conn, 'chan')
        connman.remove_connection_from_channel(conn, 'chan')
        self.assertEqual({}, connman._channel_to_connections)
        self.assertEqual({conn: set()}, connman._connection_to_channels)
        connman._sub_socket.setsockopt.assert_called_with(zmq.UNSUBSCRIBE,
                                                b'publish_to_channel:chan')

    def test_remove_connection(self):
        conn = self.get_connection()
        conn.on_open()
        connman = self.connman
        connman.bind_connection_to_uid(conn, 222)
        connman.add_connection_to_channel(conn, 'ccc')
        connman.remove_connection(conn)
        self.assertEqual(set(), connman._connections)
        self.assertEqual({}, connman._connection_to_uid)
        self.assertEqual({}, connman._uid_to_connection)
        self.assertEqual({}, connman._channel_to_connections)
        self.assertEqual({}, connman._connection_to_channels)
        self.assertEqual([
            call(zmq.SUBSCRIBE, b'send_by_uid:222'),
            call(zmq.SUBSCRIBE, b'publish_to_channel:ccc'),
            call(zmq.UNSUBSCRIBE, b'send_by_uid:222'),
            call(zmq.UNSUBSCRIBE, b'publish_to_channel:ccc')],
        connman._sub_socket.setsockopt.call_args_list)


class LocationConnectionManager(LocationMixin, DistributedConnectionManager):
    pass


class TestLocationMixin(unittest.TestCase):

    def setUp(self):
        self.connman = LocationConnectionManager(pub_socket=Mock(),
                                                 sub_socket=Mock(),
                                                 locations_sub_socket=Mock())

    def get_connection(self):
        conn = Protocol(Mock())
        conn.setup(self.connman, None)
        return conn

    def test_add_user_to_location(self):
        conn = self.get_connection()
        conn.on_open()
        connman = self.connman
        connman.bind_connection_to_uid(conn, 111)
        connman.add_user_to_location('fooloc', 111)
        self.assertEqual({111:'fooloc'}, connman._uid_to_location)
        self.assertEqual({'fooloc': {111}}, connman._location_to_uids)
        self.assertEqual([
            call(zmq.SUBSCRIBE, b'private_message_from_location:fooloc:111'),
            call(zmq.SUBSCRIBE, b'public_message_from_location:fooloc')],
        connman._locs_sub_socket.setsockopt.call_args_list)

    def test_remove_connection(self):
        conn = self.get_connection()
        conn.on_open()
        connman = self.connman
        connman.bind_connection_to_uid(conn, 222)
        connman.add_user_to_location('megaloc', 222)
        connman.add_connection_to_channel(conn, 'ccc')
        connman.remove_connection(conn)
        self.assertEqual(set(), connman._connections)
        self.assertEqual({}, connman._connection_to_uid)
        self.assertEqual({}, connman._uid_to_connection)
        self.assertEqual({}, connman._channel_to_connections)
        self.assertEqual({}, connman._connection_to_channels)
        self.assertEqual({}, connman._uid_to_location)
        self.assertEqual({}, connman._location_to_uids)
        self.assertEqual([
            call(zmq.SUBSCRIBE, b'send_by_uid:222'),
            call(zmq.SUBSCRIBE, b'publish_to_channel:ccc'),
            call(zmq.UNSUBSCRIBE, b'send_by_uid:222'),
            call(zmq.UNSUBSCRIBE, b'publish_to_channel:ccc')],
        connman._sub_socket.setsockopt.call_args_list)
        self.assertEqual([
            call(zmq.SUBSCRIBE, b'private_message_from_location:megaloc:222'),
            call(zmq.SUBSCRIBE, b'public_message_from_location:megaloc'),
            call(zmq.UNSUBSCRIBE, b'private_message_from_location:megaloc:222'),
            call(zmq.UNSUBSCRIBE, b'public_message_from_location:megaloc')],
        connman._locs_sub_socket.setsockopt.call_args_list)


if __name__ == '__main__':
    unittest.main()
