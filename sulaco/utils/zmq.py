from zmq.eventloop import ioloop as zioloop
from tornado import ioloop as tioloop


class ZMQIOLoop(tioloop.PollIOLoop):

    def initialize(self, **kwargs):
        super().initialize(impl=zioloop.ZMQPoller(), **kwargs)


def install():
    assert not tioloop.IOLoop.initialized()
    tioloop.IOLoop.configure(ZMQIOLoop)
    zioloop.IOLoop = tioloop.IOLoop
