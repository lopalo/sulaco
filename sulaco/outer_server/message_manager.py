import json
import zmq
from abc import ABCMeta, abstractmethod
from zmq.eventloop import zmqstream

from sulaco import (
    PUBLIC_MESSAGE_FROM_LOCATION_PREFIX,
    PRIVATE_MESSAGE_FROM_LOCATION_PREFIX,
    LOCATION_CONNECTED_PREFIX, LOCATION_DISCONNECTED_PREFIX)
from sulaco.outer_server import SEND_BY_UID_PREFIX, PUBLISH_TO_CHANNEL_PREFIX
from sulaco.utils.receiver import INTERNAL_SIGN


def message_handler(prefix):
    def wrapper(func):
        func.__handle_message__ = prefix
        return func
    return wrapper


class MessageManager(object):

    def __init__(self, config):
        self._collect_handlers()
        self._config = config
        self.loc_input_sockets = {}
        self._context = None

    def connect(self):
        self._context = context = zmq.Context()
        config = self._config

        # setup connections with broker
        self.pub_to_broker = context.socket(zmq.PUB)
        self.pub_to_broker.connect(config.message_broker.sub_address)

        self.sub_to_broker = context.socket(zmq.SUB)
        self.sub_to_broker.connect(config.message_broker.pub_address)
        zmqstream.ZMQStream(self.sub_to_broker).on_recv(self._on_message)

        # setup connection with location_manager
        self._sub_to_locman = context.socket(zmq.SUB)
        self._sub_to_locman.connect(config.location_manager.pub_address)
        self._sub_to_locman.setsockopt(zmq.SUBSCRIBE, '')
        zmqstream.ZMQStream(self._sub_to_locman).on_recv(self._on_message)

        # create socket for receiving of messages from locations
        self.sub_to_locs = context.socket(zmq.SUB)
        zmqstream.ZMQStream(self.sub_to_locs).on_recv(self._on_message)

    def setup(self, connman, root):
        self._connman = connman
        if not isinstance(root, Root):
            raise Exception('Should be an instance of Root')
        self._root = root

    def _collect_handlers(self):
        self._handlers = {}
        for name in dir(self):
            item = getattr(self, name)
            if not hasattr(item, '__handle_message__'):
                continue
            prefix = item.__handle_message__
            assert prefix not in self._handlers, 'handler already exists'
            self._handlers[prefix] = item

    def _on_message(self, parts):
        topic, body = parts
        prefix, data = topic.split(':', 1)
        msg = json.loads(body)
        #TODO: try-except and log
        self._handlers[prefix + ':'](data, msg)

    @message_handler(SEND_BY_UID_PREFIX)
    def send_by_uid(self, uid, msg):
        self._connman.send_by_uid(int(uid), msg)

    @message_handler(PUBLISH_TO_CHANNEL_PREFIX)
    def publish_to_channel(self, channel, msg):
        self._connman.publish_to_channel(channel, msg, True)

    @message_handler(LOCATION_CONNECTED_PREFIX)
    def add_location(self, loc_id, data):
        assert loc_id not in self.loc_input_sockets, 'location already exists'
        push_sock = self._context.socket(zmq.PUSH)
        push_sock.connect(data['pull_address'])
        self.loc_input_sockets[loc_id] = push_sock
        self.sub_to_locs.connect(data['pub_address'])
        self._root.location_added(loc_id)

    @message_handler(LOCATION_DISCONNECTED_PREFIX)
    def remove_location(self, loc_id, data):
        sock = self.loc_input_sockets.pop(loc_id)
        sock.close()
        self._root.location_removed(loc_id)

    def _location_dispatch(self, location, uid, msg):
        path_prefix = self._config.outer_server.location_handler_path.spit('.')
        path = msg['path'].split('.')
        path = path_prefix + path
        kwargs = msg['kwargs']
        #TODO: try-except with logging
        dispatch(self,_root, 0, path, INTERNAL_SIGN, kwargs)

    @message_handler(PUBLIC_MESSAGE_FROM_LOCATION_PREFIX)
    def location_public(self, location, msg):
        self._location_dispatch(location, None, msg)

    @message_handler(PRIVATE_MESSAGE_FROM_LOCATION_PREFIX)
    def location_private(self, location_uid, msg):
        location, uid = location_uid.split(':')
        self._location_dispatch(location, uid, msg)


class Root(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def location_added(self):
        pass

    @abstractmethod
    def location_removed(self):
        pass
