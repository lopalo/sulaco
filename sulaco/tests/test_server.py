import argparse
from zmq.eventloop import ioloop
ioloop.install()

from tornado.ioloop import IOLoop
from sulaco.tcp_server import TCPServer, SimpleProtocol
from sulaco.outer_server import ConnectionManager, ConnectionHandler
from sulaco.utils.receiver import message_receiver, message_router, unsigned
#from sulaco.message_manager import MessageManager

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
        msg = dict(path='message_from_user', kwargs=dict(text=text, uid=uid))
        connman.send_by_uid(receiver, msg)

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
        kwargs = dict(text=text, channel=channel)
        msg = dict(path='message_from_channel', kwargs=kwargs)
        connman.publish_to_channel(channel, msg)

    @message_receiver
    @unsigned
    def unsubscribe(self, conn, connman, channel, **kwargs):
        connman.remove_connection_from_channel(conn, channel)


class Protocol(ConnectionHandler, SimpleProtocol):
    pass


def main(options):
    #msgman = MessageManager()
    connman = ConnectionManager()
    root = Root()
    server = TCPServer()
    server.setup(Protocol, connman, root, options.max_conn)
    server.listen(options.port)
    IOLoop.instance().start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', help='run on the given port',
                        action='store', dest='port', type=int, required=True)
    parser.add_argument('-c', '--max-conn', help='max connections on server',
                        action='store', dest='max_conn',
                        type=int, required=True)
    options = parser.parse_args()
    main(options)
