import argparse
from zmq.eventloop import ioloop
ioloop.install()

from tornado.ioloop import IOLoop
from sulaco.simple_protocol import SimpleProtocol
from sulaco.outer_server import (TCPServer,
                                 ConnectionManager,
                                 ConnectionHandler)
from sulaco.utils.receiver import message_receiver
#from sulaco.message_manager import MessageManager

class Root(object):

    @message_receiver
    def echo(self, text, conn, **kwargs):
        text = 'Echo: ' + text
        conn.sender.echo(text=text)


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
