import unittest
import zmq
from mock import Mock, call
from sulaco.outer_server.tcp_server import SimpleProtocol
from sulaco.outer_server.connection_manager import (
    DistributedConnectionManager,
    ConnectionHandler)


class Protocol(ConnectionHandler, SimpleProtocol):
    pass


class TestDistributedConnectionManager(unittest.TestCase):

    def setUp(self):
        self.connman = DistributedConnectionManager(Mock(), Mock())

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
                                                          'send_by_uid:111')

    def test_add_connection_to_channel(self):
        conn = self.get_connection()
        conn.on_open()
        connman = self.connman
        connman.add_connection_to_channel(conn, 'chan')
        self.assertEqual({'chan': {conn,}}, connman._channels_to_connections)
        self.assertEqual({conn: {'chan',}}, connman._connections_to_channels)
        connman._sub_socket.setsockopt.assert_called_with(zmq.SUBSCRIBE,
                                                'publish_to_channel:chan')

    def test_remove_connection_from_channel(self):
        conn = self.get_connection()
        conn.on_open()
        connman = self.connman
        connman.add_connection_to_channel(conn, 'chan')
        connman.remove_connection_from_channel(conn, 'chan')
        self.assertEqual({}, connman._channels_to_connections)
        self.assertEqual({conn: set()}, connman._connections_to_channels)
        connman._sub_socket.setsockopt.assert_called_with(zmq.UNSUBSCRIBE,
                                                'publish_to_channel:chan')

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
        self.assertEqual({}, connman._channels_to_connections)
        self.assertEqual({}, connman._connections_to_channels)
        self.assertEqual([
            call(zmq.SUBSCRIBE, 'send_by_uid:222'),
            call(zmq.SUBSCRIBE, 'publish_to_channel:ccc'),
            call(zmq.UNSUBSCRIBE, 'send_by_uid:222'),
            call(zmq.UNSUBSCRIBE, 'publish_to_channel:ccc')],
        connman._sub_socket.setsockopt.call_args_list)


if __name__ == '__main__':
    unittest.main()
