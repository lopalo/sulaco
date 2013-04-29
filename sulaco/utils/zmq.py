from zmq.eventloop.ioloop import IOLoop as ZIOLoop, ZMQPoller
from tornado.ioloop import PollIOLoop


class ZMQIOLoop(PollIOLoop):

    def initialize(self, **kwargs):
        super().initialize(impl=ZMQPoller(), **kwargs)


def install():
    assert not ZIOLoop.initialized() and not PollIOLoop.initialized()
    PollIOLoop.configure(ZMQIOLoop)
    ZIOLoop._instance = PollIOLoop.instance()
