import json
import zmq
from zmq.eventloop import zmqstream

from sulaco.outer_server.connection_manager import (
    SEND_BY_UID_PREFIX,
    PUBLISH_TO_CHANNEL_PREFIX)


def message_handler(prefix):
    def wrapper(func):
        func.__handle_message__ = prefix
        return func
    return wrapper


class MessageManager(object):

    def __init__(self, config):
        self._collect_handlers()
        self._context = context = zmq.Context()

        # setup connections with broker
        self.pub_to_broker = context.socket(zmq.PUB)
        self.pub_to_broker.connect(config.message_broker.sub_address)

        self.sub_to_broker = context.socket(zmq.SUB)
        self.sub_to_broker.connect(config.message_broker.pub_address)
        zmqstream.ZMQStream(self.sub_to_broker).on_recv(self._on_message)

    def setup(self, connman):
        self._connman = connman

    def _collect_handlers(self):
        self._handlers = {}
        for name in dir(self):
            item = getattr(self, name)
            if not hasattr(item, '__handle_message__'):
                continue
            prefix = item.__handle_message__
            assert prefix not in self._handlers
            self._handlers[prefix] = item

    def _on_message(self, parts):
        topic, body = parts
        prefix, data = topic.split(':')
        msg = json.loads(body)
        self._handlers[prefix + ':'](data, msg)

    @message_handler(SEND_BY_UID_PREFIX)
    def send_by_uid(self, uid, msg):
        self._connman.send_by_uid(int(uid), msg)

    @message_handler(PUBLISH_TO_CHANNEL_PREFIX)
    def publish_to_channel(self, channel, msg):
        self._connman.publish_to_channel(channel, msg, True)

