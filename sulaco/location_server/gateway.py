import signal

import zmq
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback

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
                    pull_address=pub_address)
        req_socket.send(CONNECT_MESSAGE, zmq.SNDMORE)
        req_socket.send(self._ident, zmq.SNDMORE)
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
            self._push_to_man.send_multipart([HEARTBEAT_MESSAGE, self._ident])
        period = self._config.location.heartbeat_period * 1000
        PeriodicCallback(heartbeat, period).start()

        def stop(signum, frame):
            IOLoop.instance().stop()
        signal.signal(signal.SIGTERM, stop)
        try:
            IOLoop.instance().start()
        finally:
            parts = DISCONNECT_MESSAGE, self._ident
            self._push_to_man.send_multipart(parts, copy=False,
                                                track=True).wait(2)

    def _receive(self, parts):
        #TODO: dispatch
        self._root

    def private_message(self, uid):
        pass

    def public_message(self, uid):
        pass

