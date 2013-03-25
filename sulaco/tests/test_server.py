import argparse
from zmq.eventloop import ioloop
ioloop.install()

from tornado.ioloop import IOLoop
from sulaco.tcp_server import TCPServer, SimpleProtocol
from sulaco.connection_manager import (DistributedConnectionManager,
                                       ConnectionHandler)
from sulaco.utils.receiver import message_receiver, message_router, unsigned
from sulaco.utils import Config
from sulaco.message_manager import MessageManager


class Root(object):

    @message_receiver
    @unsigned
    def echo(self, text, conn, **kwargs):
        text = 'Echo: ' + text
        conn.s.echo(text=text)

    @unsigned
    @message_receiver
    def sign_id(self, username, conn, connman, **kwargs):
        uid = hash(username)
        connman.bind_connection_to_uid(conn, uid)
        conn.s.sign_id(uid=uid)

    @message_receiver
    def method_signed(self):
        pass

    @message_receiver
    def send_to_user(self, text, receiver, connman, uid, **kwargs):
        connman.us(receiver).message_from_user(text=text, uid=uid)

    @message_router
    @unsigned
    def channels(self, **kwargs):
        return Channels()


class Channels(object):

    @message_receiver
    @unsigned
    def subscribe(self, conn, connman, channel, **kwargs):
        connman.add_connection_to_channel(conn, channel)

    @message_receiver
    @unsigned
    def publish(self, connman, channel, text, **kwargs):
        connman.cs(channel).message_from_channel(text=text, channel=channel)

    @message_receiver
    @unsigned
    def unsubscribe(self, conn, connman, channel, **kwargs):
        connman.remove_connection_from_channel(conn, channel)


class Protocol(ConnectionHandler, SimpleProtocol):
    pass


def main(options):
    config = Config.load_yaml(options.config)
    msgman = MessageManager(config)
    connman = DistributedConnectionManager(msgman.pub_to_broker,
                                           msgman.sub_to_broker)
    root = Root()
    msgman.setup(connman)
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
