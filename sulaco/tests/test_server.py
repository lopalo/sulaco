import argparse
from zmq.eventloop import ioloop
ioloop.install()

from tornado.ioloop import IOLoop
from sulaco.tcp_server import TCPServer, SimpleProtocol
from sulaco.outer_server import ConnectionManager, ConnectionHandler
from sulaco.utils.receiver import message_receiver, unsigned
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
    def send_to_user(self, text, receiver, connman, **kwargs):
        msg = dict(path='message_from_user', kwargs=dict(text=text))
        connman.send_by_uid(receiver, msg)


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
