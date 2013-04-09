import argparse

from random import choice
from zmq.eventloop import ioloop
ioloop.install()

from tornado.ioloop import IOLoop
from sulaco.outer_server.tcp_server import TCPServer, SimpleProtocol
from sulaco.outer_server.connection_manager import (
    DistributedConnectionManager,
    ConnectionHandler, LocationMixin)
from sulaco.utils.receiver import (
    message_receiver, message_router,
    USER_SIGN, INTERNAL_USER_SIGN, INTERNAL_SIGN)
from sulaco.utils import Config
from sulaco.outer_server.message_manager import MessageManager
from sulaco.outer_server.message_manager import Root as ABCRoot


class Root(ABCRoot):

    def __init__(self, config, connman, msgman):
        self._config = config
        self._connman = connman
        self._msgman = msgman
        self._users = {}

    @message_receiver()
    def echo(self, text, conn, **kwargs):
        text = 'Echo: ' + text
        conn.s.echo(text=text)

    @message_receiver()
    def sign_id(self, username, conn, **kwargs):
        uid = hash(username)
        self._connman.bind_connection_to_uid(conn, uid)
        loc = choice(self._config.user.start_locations)
        self._users[uid] = User(uid, loc)
        conn.s.sign_id(uid=uid)

    @message_receiver(USER_SIGN)
    def method_signed(self):
        pass

    @message_receiver(USER_SIGN)
    def send_to_user(self, text, receiver, uid, **kwargs):
        self._connman.us(receiver).message_from_user(text=text, uid=uid)

    @message_router()
    def channels(self, **kwargs):
        return Channels(self._connman)

    @message_router(INTERNAL_USER_SIGN)
    def location(self, **kwargs):
        uid = kwargs.get('uid')
        user = self._users[uid] if uid is not None else None
        loc_name = kwargs.get('location') or user.location
        socket = self._msgman.loc_input_sockets[loc_name]
        #TODO: implement autosave for user using contextmanager
        return Location(loc_name, user, socket, self._connman)

    def location_added(self, loc_id):
        self._connman.alls.location_added(loc_id=loc_id)

    def location_removed(self, loc_id):
        self._connman.alls.location_removed(loc_id=loc_id)


class Channels(object):

    def __init__(self, connman):
        self._connman = connman

    @message_receiver()
    def subscribe(self, conn, channel, **kwargs):
        self._connman.add_connection_to_channel(conn, channel)

    @message_receiver()
    def publish(self, channel, text, **kwargs):
        self._connman.cs(channel, False).message_from_channel(text=text,
                                                              channel=channel)

    @message_receiver()
    def unsubscribe(self, conn, channel, **kwargs):
        self._connman.remove_connection_from_channel(conn, channel)


class User(object):

    def __init__(self, uid, location):
        self.uid = uid
        self.location = location


class Location(object):

    def __init__(self, name, user, loc_input, connman):
        self.name = loc_name
        self._user = user
        self._loc_input = loc_input
        self._connman = connman

    @message_receiver(USER_SIGN)
    def move_to(self, next_location, **kwargs):
        # TODO: push to location > check in location and exit > push to message_manager >
        #       enter location (meth 'enter') > init in new location > init meth
        pass

    @message_receiver(INTERNAL_SIGN)
    def enter(self, data, **kwargs):
        self._user.location = self.name
        # send to loc

    @message_receiver(INTERNAL_SIGN)
    def init(self, data, **kwargs):
        pass


class Protocol(ConnectionHandler, SimpleProtocol):
    pass


class ConnManager(DistributedConnectionManager, LocationMixin):
    pass


def main(options):
    config = Config.load_yaml(options.config)
    msgman = MessageManager(config)
    msgman.connect()
    connman = ConnManager(pub_socket=msgman.pub_to_broker,
                          sub_socket=msgman.sub_to_broker,
                          locations_sub_socket=msgman.sub_to_locs)
    root = Root(config, connman, msgman)
    msgman.setup(connman, root)
    server = TCPServer()
    server.setup(Protocol, connman, root, options.max_conn)
    server.listen(options.port)
    IOLoop.instance().start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', help='run on the given port',
                        action='store', dest='port', type=int, required=True)
    parser.add_argument('-mc', '--max-conn', help='max connections on server',
                        action='store', dest='max_conn',
                        type=int, required=True)
    parser.add_argument('-c', '--config', action='store', dest='config',
                        help='path to config file', type=str, required=True)
    options = parser.parse_args()
    main(options)
