import signal
import json

from functools import partial

import zmq
from zmq.eventloop.zmqstream import ZMQStream
from tornado.ioloop import IOLoop, PeriodicCallback

from sulaco import (PUBLIC_MESSAGE_FROM_LOCATION_PREFIX,
                    PRIVATE_MESSAGE_FROM_LOCATION_PREFIX)
from sulaco.utils import Sender
from sulaco.utils.receiver import root_dispatch, INTERNAL_SIGN
from sulaco.location_server import (
    CONNECT_MESSAGE, DISCONNECT_MESSAGE,
    HEARTBEAT_MESSAGE)


class Gateway(object):

    def __init__(self, config, ident, data={}):
        self._config = config
        self._ident = ident
        self.data = data

    def setup(self, root):
        self._root = root

    def connect(self, pub_address, pull_address):
        context = zmq.Context()
        req_socket = context.socket(zmq.REQ)
        req_socket.connect(self._config.location_manager.rep_address)
        data = self.data.copy()
        data.update(ident=self._ident,
                    pub_address=pub_address,
                    pull_address=pull_address)
        req_socket.send(CONNECT_MESSAGE.encode('utf-8'), zmq.SNDMORE)
        req_socket.send(self._ident.encode('utf-8'), zmq.SNDMORE)
        req_socket.send_json(data)
        connected = req_socket.recv_json()
        if not connected:
            return False

        self._push_to_man = context.socket(zmq.PUSH)
        self._push_to_man.connect(self._config.location_manager.pull_address)

        self._pub_sock = context.socket(zmq.PUB)
        self._pub_sock.bind(pub_address)

        self._pull_sock = context.socket(zmq.PULL)
        self._pull_sock.bind(pull_address)
        ZMQStream(self._pull_sock).on_recv(self._receive)
        return True

    def start(self):
        def heartbeat():
            parts = (HEARTBEAT_MESSAGE.encode('utf-8'),
                     self._ident.encode('utf-8'))
            self._push_to_man.send_multipart(parts)
        period = self._config.location.heartbeat_period * 1000
        PeriodicCallback(heartbeat, period).start()

        def stop(signum, frame):
            IOLoop.instance().stop()
        signal.signal(signal.SIGTERM, stop)
        try:
            IOLoop.instance().start()
        finally:
            parts = (DISCONNECT_MESSAGE.encode('utf-8'),
                     self._ident.encode('utf-8'))
            self._push_to_man.send_multipart(parts, copy=False,
                                                track=True).wait(2)

    def _receive(self, parts):
        assert len(parts) == 1
        message = json.loads(parts[0].decode('utf-8'))
        path = message['path'].split('.')
        kwargs = message['kwargs']
        root_dispatch(self._root, path, kwargs, INTERNAL_SIGN)
        #TODO: catch exceptions and write them to log

    def private_message(self, uid, msg):
        topic = '{}{}:{}'.format(PRIVATE_MESSAGE_FROM_LOCATION_PREFIX,
                                                self._ident, str(uid))
        self._pub_sock.send(topic.encode('utf-8'), zmq.SNDMORE)
        self._pub_sock.send_json(msg)

    def prs(self, uid):
        """ Returns private sender """

        send = partial(self.private_message, uid)
        return Sender(send)

    def public_message(self, msg):
        topic = PUBLIC_MESSAGE_FROM_LOCATION_PREFIX + self._ident
        self._pub_sock.send(topic.encode('utf-8'), zmq.SNDMORE)
        self._pub_sock.send_json(msg)

    @property
    def pubs(self):
        """ Returns private sender """

        return Sender(self.public_message)

