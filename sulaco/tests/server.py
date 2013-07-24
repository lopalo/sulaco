import argparse
import logging
import msgpack
from random import choice

from tornado.ioloop import IOLoop

from sulaco.outer_server.tcp_server import TCPServer, SimpleProtocol
from sulaco.outer_server.connection_manager import (
    DistributedConnectionManager,
    ConnectionHandler, LocationConnectionManager)
from sulaco.utils.receiver import (
    message_receiver, message_router, LoopbackMixin,
    ProxyMixin, USER_SIGN, INTERNAL_USER_SIGN, INTERNAL_SIGN)
from sulaco.utils import Config, Sender, ColorUTCFormatter
from zmq.eventloop.ioloop import install
from sulaco.outer_server.message_manager import (
    MessageManager, LocationMessageManager)
from sulaco.outer_server.message_manager import LocationRoot


class Root(LocationRoot, LoopbackMixin):

    def __init__(self, config, connman, msgman):
        super().__init__()
        self._config = config
        self._connman = connman
        self._msgman = msgman
        self._locations = {}
        self._users = {}

    @message_receiver()
    def echo(self, text, conn, **kwargs):
        text = 'Echo: ' + text
        conn.s.echo(text=text)

    @message_receiver()
    def sign_id(self, username, conn, loc=None, **kwargs):
        uid = username[-1]
        assert uid not in self._users
        self._connman.bind_connection_to_uid(conn, uid)
        loc = loc or choice(self._config.user.start_locations)
        self._users[uid] = User(username, uid, None, conn)
        conn.s.sign_id(uid=uid)
        self.lbs.location.enter(uid=uid, location=loc)

    @message_receiver(USER_SIGN)
    def send_to_user(self, text, receiver, uid, **kwargs):
        self._connman.us(receiver).message_from_user(text=text, uid=uid)

    @message_router()
    def channels(self, next_step, **kwargs):
        yield from next_step(Channels(self._connman))

    @message_router(INTERNAL_USER_SIGN, pass_sign=True)
    def location(self, next_step, sign, uid, location=None, **kwargs):
        user = self._users[uid]
        if sign == INTERNAL_SIGN:
            loc_name = location or user.location
        else:
            loc_name = user.location
        socket = self._msgman.loc_input_sockets.get(loc_name)
        yield from next_step(Location(loc_name, user, socket,
                                self._connman, self._config))

    def location_added(self, loc_id, data):
        self._connman.alls.location_added(loc_id=loc_id)
        self._locations[loc_id] = data

    def location_removed(self, loc_id):
        self._connman.alls.location_removed(loc_id=loc_id)
        del self._locations[loc_id]

    @message_receiver()
    def get_locations(self, conn, **kwargs):
        conn.s.locations(data=list(self._locations.values()))


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


class Location(ProxyMixin):

    def __init__(self, name, user, loc_input, connman, config):
        self._name = name
        self._user = user
        self._loc_input = loc_input
        self._connman = connman
        self._config = config
        self.s = Sender(self.send)

    def send(self, msg, sign=INTERNAL_SIGN):
        msg['kwargs']['uid'] = self._user.uid
        msg['sign'] = sign
        self._loc_input.send(msgpack.dumps(msg))

    @message_receiver(INTERNAL_SIGN)
    def enter(self, **kwargs):
        if self._loc_input is None:
            return
        connman = self._connman
        user = self._user
        if user.location is not None and user.location != self._name:
            connman.remove_user_from_location(user.location, user.uid)
        user.location = self._name
        connman.add_user_to_location(self._name, user.uid)
        self.s.enter(user=user.to_dict())

    def proxy_method(self, path, sign, kwargs):
        del kwargs['uid']
        kwargs.pop('location', None)
        kwargs.pop('conn', None)
        if sign == USER_SIGN:
            self.send(dict(path='.'.join(path), kwargs=kwargs), sign=USER_SIGN)
        elif sign == INTERNAL_SIGN:
            path_prefix = list(self._config.outer_server.
                            client_location_handler_path.split('.'))
            path = path_prefix + path
            self._user.conn.send(dict(path='.'.join(path), kwargs=kwargs))


class Protocol(ConnectionHandler, SimpleProtocol):
    pass


class ConnManager(LocationConnectionManager, DistributedConnectionManager):
    pass

class MsgManager(MessageManager, LocationMessageManager):
    pass


def main(options):
    install()

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if options.debug else logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(ColorUTCFormatter())
    logger.addHandler(handler)

    config = Config.load_yaml(options.config)
    msgman = MsgManager(config)
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
    parser.add_argument('-d', '--debug', action='store_true',
                        dest='debug', help='set debug level of logging')
    options = parser.parse_args()
    main(options)
