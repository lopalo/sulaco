from collections import defaultdict
from functools import partial

import zmq

from sulaco import (SIGN_ERROR, MAX_CONNECTION_ERROR,
                    SEND_BY_UID_PREFIX, PUBLISH_TO_CHANNEL_PREFIX)
from sulaco.utils.receiver import Sender, dispatch, SignError, USER_SIGN


class ConnectionHandler(object):
    """
    Should be first in list of basic classes, if used as mixin
    """

    def setup(self, connman, root):
        self._connman = connman
        self._root = root
        self.s = Sender(self.send)

    def on_open(self, *args):
        super(ConnectionHandler, self).on_open(*args)
        self._connman.add_connection(self)

    def on_message(self, message):
        super(ConnectionHandler, self).on_message(message)
        path = message['path'].split('.')
        kwargs = message['kwargs']
        kwargs['conn'] = self
        uid = self._connman.get_uid(self)
        if uid is not None:
            kwargs['uid'] = uid
            sign = USER_SIGN
        else:
            sign = None
        try:
            dispatch(self._root, 0, path, sign, kwargs)
        except SignError:
            #TODO: log
            self._on_sign_error()
        #TODO: catch other exceptions and write them to log

    def on_close(self):
        super(ConnectionHandler, self).on_close()
        self._connman.remove_connection(self)
        #TODO: maybe notify all connected channels and do final actions

    def _on_sign_error(self):
        self.s.error(msg=SIGN_ERROR)


class ConnectionManager(object):

    def __init__(self):
        self._connections = set()

        self._uid_to_connection = {}
        self._connection_to_uid = {}

        self._channels_to_connections = defaultdict(set)
        self._connections_to_channels = defaultdict(set)

    def add_connection(self, conn):
        assert conn not in self._connections, 'connection already registered'
        self._connections.add(conn)

    def bind_connection_to_uid(self, conn, uid):
        assert conn in self._connections, 'unknown connection'
        assert uid not in self._uid_to_connection, 'uid already exists'
        self._connection_to_uid[conn] = uid
        self._uid_to_connection[uid] = conn

    def add_connection_to_channel(self, conn, channel):
        assert conn in self._connections, 'unknown connection'
        self._connections_to_channels[conn].add(channel)
        self._channels_to_connections[channel].add(conn)

    def remove_connection_from_channel(self, conn, channel):
        self._connections_to_channels[conn].remove(channel)
        self._channels_to_connections[channel].remove(conn)
        if not self._channels_to_connections[channel]:
            del self._channels_to_connections[channel]

    def remove_connection(self, conn):
        self._connections.remove(conn)
        uid = self._connection_to_uid.pop(conn, None)
        if uid is not None:
            del self._uid_to_connection[uid]
        channels = self._connections_to_channels.pop(conn, [])
        for channel in channels:
            self._channels_to_connections[channel].remove(conn)
            if not self._channels_to_connections[channel]:
                del self._channels_to_connections[channel]

    def send_by_uid(self, uid, msg):
        conn = self._uid_to_connection.get(uid)
        if conn is None:
            return False
        conn.send(msg)
        return True

    def us(self, uid):
        """ Returns user's sender """

        send = partial(self.send_by_uid, uid)
        return Sender(send)

    def publish_to_channel(self, channel, msg):
        if channel not in self._channels_to_connections:
            return
        for conn in self._channels_to_connections[channel]:
            conn.send(msg)

    def cs(self, channel, locally=True):
        """ Returns channel's sender """

        send = partial(self.publish_to_channel, channel, locally=locally)
        return Sender(send)

    @property
    def connections_count(self):
        return len(self._connections)

    def get_uid(self, conn):
        return self._connection_to_uid.get(conn)


class DistributedConnectionManager(ConnectionManager):

    def __init__(self, pub_socket, sub_socket):
        super(DistributedConnectionManager, self).__init__()
        self._pub_socket = pub_socket
        self._sub_socket = sub_socket
        self._local_channels = set()

    def bind_connection_to_uid(self, conn, uid):
        super(DistributedConnectionManager,
                self).bind_connection_to_uid(conn, uid)
        topic = SEND_BY_UID_PREFIX + str(uid)
        self._sub_socket.setsockopt(zmq.SUBSCRIBE, topic)

    def add_connection_to_channel(self, conn, channel, locally=True):
        super(DistributedConnectionManager,
                self).add_connection_to_channel(conn, channel)
        if locally:
            #TODO: write test
            self._local_channels.add(channel)
            return
        topic = PUBLISH_TO_CHANNEL_PREFIX + str(channel)
        self._sub_socket.setsockopt(zmq.SUBSCRIBE, topic)

    def remove_connection_from_channel(self, conn, channel):
        super(DistributedConnectionManager,
                self).remove_connection_from_channel(conn, channel)
        if channel in self._local_channels:
            #TODO: write test
            if channel not in self._channels_to_connections:
                self._local_channels.remove(channel)
            return
        topic = PUBLISH_TO_CHANNEL_PREFIX + str(channel)
        self._sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic)

    def remove_connection(self, conn):
        uid = self._connection_to_uid.get(conn, None)
        if uid is not None:
            topic = SEND_BY_UID_PREFIX + str(uid)
            self._sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic)
        channels = self._connections_to_channels.get(conn, [])
        for channel in channels:
            topic = PUBLISH_TO_CHANNEL_PREFIX + str(channel)
            self._sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic)
        super(DistributedConnectionManager, self).remove_connection(conn)
        #TODO: write test
        self._local_channels.intersection_update(self._channels_to_connections)

    def send_by_uid(self, uid, msg):
        sent = super(DistributedConnectionManager, self).send_by_uid(uid, msg)
        if sent:
            return
        topic = SEND_BY_UID_PREFIX + str(uid)
        self._pub_socket.send(topic, zmq.SNDMORE)
        self._pub_socket.send_json(msg)

    def publish_to_channel(self, channel, msg, locally=True):
        super(DistributedConnectionManager,
                self).publish_to_channel(channel, msg)
        if locally:
            # to prevent of infinite broadcast
            return
        topic = PUBLISH_TO_CHANNEL_PREFIX + str(channel)
        self._pub_socket.send(topic, zmq.SNDMORE)
        self._pub_socket.send_json(msg)


