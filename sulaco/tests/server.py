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
    message_receiver, message_router, Loopback,
    USER_SIGN, INTERNAL_USER_SIGN, INTERNAL_SIGN)
from sulaco.utils import Config, Sender
from sulaco.outer_server.message_manager import MessageManager
from sulaco.outer_server.message_manager import Root as ABCRoot


class Root(ABCRoot, Loopback):

    def __init__(self, config, connman, msgman):
        super(Root, self).__init__()
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
        self._users[uid] = User(username, uid, loc, conn)
        conn.s.sign_id(uid=uid)
        self.lbs.location.enter(uid=uid, location=loc)

    @message_receiver(USER_SIGN)
    def method_signed(self):
        pass

    @message_receiver(USER_SIGN)
    def send_to_user(self, text, receiver, uid, **kwargs):
        self._connman.us(receiver).message_from_user(text=text, uid=uid)

    @message_router()
    def channels(self, next_step, **kwargs):
        next_step(Channels(self._connman))

    @message_router(INTERNAL_USER_SIGN)
    def location(self, next_step, uid, location=None, **kwargs):
        user = self._users[uid]
        loc_name = location or user.location
        socket = self._msgman.loc_input_sockets.get(loc_name)
        next_step(Location(loc_name, user, socket, self._connman))

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

    def __init__(self, username, uid, location, conn):
        self.username = username
        self.uid = uid
        self.location = location
        self.conn = conn

    def to_dict(self):
        return {'username': self.username,
                'uid': self.uid}


class Location(object):

    def __init__(self, name, user, loc_input, connman):
        self._name = name
        self._user = user
        self._loc_input = loc_input
        self._connman = connman
        self.s = Sender(self.send)

    def send(self, msg):
        msg['uid'] = self._user.uid
        self._loc_input.send_json(msg)

    @message_receiver(USER_SIGN)
    def move_to(self, next_location, **kwargs):
        # TODO: push to location > check in location and exit > push to message_manager >
        #       enter location (meth 'enter') > init in new location > init meth
        pass

    @message_receiver(INTERNAL_SIGN)
    def enter(self, **kwargs):
        if self._loc_input is None:
            return
        connman = self._connman
        user = self._user
        if user.location != self._name:
            connman.remove_user_from_location(self._name, user.uid)
        user.location = self._name
        connman.add_user_to_location(self._name, user.uid)
        self.s.enter(user=user.to_dict())

    @message_receiver(INTERNAL_SIGN)
    def init(self, users, **kwargs):
        self._user.conn.s.init_location(users=users)


class Protocol(ConnectionHandler, SimpleProtocol):
    pass


class ConnManager(LocationMixin, DistributedConnectionManager):
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
