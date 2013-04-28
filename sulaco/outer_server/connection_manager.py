from collections import defaultdict
from functools import partial

import zmq

from sulaco import (PUBLIC_MESSAGE_FROM_LOCATION_PREFIX,
                    PRIVATE_MESSAGE_FROM_LOCATION_PREFIX)
from sulaco.outer_server import (
    SIGN_ERROR, MAX_CONNECTION_ERROR,
    SEND_BY_UID_PREFIX, PUBLISH_TO_CHANNEL_PREFIX)
from sulaco.utils import Sender
from sulaco.utils.receiver import root_dispatch, SignError, USER_SIGN


class ConnectionHandler(object):
    """
    Should be first in list of basic classes, if used as mixin
    """

    def setup(self, connman, root):
        self._connman = connman
        self._root = root
        self.s = Sender(self.send)

    def on_open(self, *args):
        super().on_open(*args)
        self._connman.add_connection(self)

    def on_message(self, message):
        super().on_message(message)
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
            root_dispatch(self._root, path, kwargs, sign)
        except SignError:
            #TODO: log
            self._on_sign_error()
        #TODO: catch other exceptions and write them to log

    def on_close(self):
        super().on_close()
        self._connman.remove_connection(self)
        #TODO: maybe notify all connected channels and do final actions

    def _on_sign_error(self):
        self.s.error(msg=SIGN_ERROR)


class ConnectionManager(object):

    def __init__(self, **kwargs):
        self._connections = set()

        self._uid_to_connection = {}
        self._connection_to_uid = {}

        self._channel_to_connections = defaultdict(set)
        self._connection_to_channels = defaultdict(set)

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
        error = 'connection already in channel'
        assert channel not in self._connection_to_channels[conn], error
        self._connection_to_channels[conn].add(channel)
        self._channel_to_connections[channel].add(conn)

    def remove_connection_from_channel(self, conn, channel):
        self._connection_to_channels[conn].remove(channel)
        self._channel_to_connections[channel].remove(conn)
        if not self._channel_to_connections[channel]:
            del self._channel_to_connections[channel]

    def remove_connection(self, conn):
        self._connections.remove(conn)
        uid = self._connection_to_uid.pop(conn, None)
        if uid is not None:
            del self._uid_to_connection[uid]
        channels = self._connection_to_channels.pop(conn, [])
        for channel in channels:
            self._channel_to_connections[channel].remove(conn)
            if not self._channel_to_connections[channel]:
                del self._channel_to_connections[channel]

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

    def publish_to_channel(self, channel, msg, **kwargs):
        if channel not in self._channel_to_connections:
            return
        for conn in self._channel_to_connections[channel]:
            conn.send(msg)

    def cs(self, channel, locally=True):
        """ Returns channel's sender """

        send = partial(self.publish_to_channel, channel, locally=locally)
        return Sender(send)

    def publish_to_all(self, msg):
        for conn in self._connections:
            conn.send(msg)

    @property
    def alls(self):
        """ Returns sender that publishes to all connections"""

        return Sender(self.publish_to_all)

    @property
    def connections_count(self):
        return len(self._connections)

    def get_uid(self, conn):
        return self._connection_to_uid.get(conn)


class DistributedConnectionManager(ConnectionManager):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._pub_socket = kwargs['pub_socket']
        self._sub_socket = kwargs['sub_socket']

    def bind_connection_to_uid(self, conn, uid):
        super().bind_connection_to_uid(conn, uid)
        topic = SEND_BY_UID_PREFIX + str(uid)
        self._sub_socket.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))

    def add_connection_to_channel(self, conn, channel):
        super().add_connection_to_channel(conn, channel)
        topic = PUBLISH_TO_CHANNEL_PREFIX + str(channel)
        self._sub_socket.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))

    def remove_connection_from_channel(self, conn, channel):
        super().remove_connection_from_channel(conn, channel)
        topic = PUBLISH_TO_CHANNEL_PREFIX + str(channel)
        self._sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode('utf-8'))

    def remove_connection(self, conn):
        uid = self._connection_to_uid.get(conn, None)
        if uid is not None:
            topic = SEND_BY_UID_PREFIX + str(uid)
            self._sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode('utf-8'))
        channels = self._connection_to_channels.get(conn, [])
        for channel in channels:
            topic = PUBLISH_TO_CHANNEL_PREFIX + str(channel)
            self._sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic.encode('utf-8'))
        super().remove_connection(conn)

    def send_by_uid(self, uid, msg):
        sent = super().send_by_uid(uid, msg)
        if sent:
            return
        topic = SEND_BY_UID_PREFIX + str(uid)
        self._pub_socket.send(topic.encode('utf-8'), zmq.SNDMORE)
        self._pub_socket.send_json(msg)

    def publish_to_channel(self, channel, msg, locally=True):
        super().publish_to_channel(channel, msg)
        if locally:
            # to prevent of infinite broadcast
            return
        topic = PUBLISH_TO_CHANNEL_PREFIX + str(channel)
        self._pub_socket.send(topic.encode('utf-8'), zmq.SNDMORE)
        self._pub_socket.send_json(msg)


class LocationMixin(object):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._locs_sub_socket = kwargs['locations_sub_socket']
        self._uid_to_location = {}
        self._location_to_uids = defaultdict(set)

    def add_user_to_location(self, location, uid):
        assert uid in self._uid_to_connection
        assert uid not in self._uid_to_location
        self._uid_to_location[uid] = location
        self._location_to_uids[location].add(uid)
        topic = '{}{}:{}'.format(PRIVATE_MESSAGE_FROM_LOCATION_PREFIX,
                                                    location, str(uid))
        self._locs_sub_socket.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
        topic = PUBLIC_MESSAGE_FROM_LOCATION_PREFIX + str(location)
        self._locs_sub_socket.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))

    def remove_user_from_location(self, location, uid):
        del self._uid_to_location[uid]
        self._location_to_uids[location].remove(uid)
        if not self._location_to_uids[location]:
            del self._location_to_uids[location]
        topic = '{}{}:{}'.format(PRIVATE_MESSAGE_FROM_LOCATION_PREFIX,
                                                    location, str(uid))
        self._locs_sub_socket.setsockopt(zmq.UNSUBSCRIBE,
                                         topic.encode('utf-8'))
        topic = PUBLIC_MESSAGE_FROM_LOCATION_PREFIX + str(location)
        self._locs_sub_socket.setsockopt(zmq.UNSUBSCRIBE,
                                         topic.encode('utf-8'))

    def remove_connection(self, conn):
        uid = self._connection_to_uid.get(conn, None)
        super().remove_connection(conn)
        if uid is None or uid not in self._uid_to_location:
            return
        location = self._uid_to_location[uid]
        self.remove_user_from_location(location, uid)

    def publish_to_location(self, location, msg):
        for uid in self._location_to_uids[location]:
            self._uid_to_connection[uid].send(msg)

    def ls(self, location):
        """ Returns location's sender """

        send = partial(self.publish_to_location, location)
        return Sender(send)




