import msgpack
import zmq
import logging

from abc import ABCMeta, abstractmethod
from zmq.eventloop import zmqstream
from tornado.stack_context import ExceptionStackContext

from sulaco import (
    PUBLIC_MESSAGE_FROM_LOCATION_PREFIX,
    PRIVATE_MESSAGE_FROM_LOCATION_PREFIX, GET_LOCATIONS_INFO,
    LOCATION_CONNECTED_PREFIX, LOCATION_DISCONNECTED_PREFIX)
from sulaco.outer_server import SEND_BY_UID_PREFIX, PUBLISH_TO_CHANNEL_PREFIX
from sulaco.utils import InstanceError
from sulaco.utils.receiver import INTERNAL_SIGN, root_dispatch
from sulaco.outer_server.connection_manager import (
    DistributedConnectionManager,
    LocationConnectionManager)


logger = logging.getLogger(__name__)


def message_handler(prefix):
    def wrapper(func):
        func.__handle_message__ = prefix
        return func
    return wrapper


class BasicMessageManager(object):

    def __init__(self, config):
        self._collect_handlers()
        self._config = config
        self._context = None

    def connect(self):
        self._context = zmq.Context()

    def setup(self, connman, root):
        self._connman = connman
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
        topic = topic.decode('utf-8')
        msg = msgpack.loads(body, encoding='utf-8')
        logger.debug("Received message - topic: %s, body: %s", topic, msg)
        prefix, data = topic.split(':', 1)
        with ExceptionStackContext(self.exception_handler):
            self._handlers[prefix + ':'](data, msg)

    def exception_handler(self, type, value, traceback):
        logger.exception('Exception in message handler')
        return True


class MessageManager(BasicMessageManager):

    def connect(self):
        super().connect()

        # setup connections with broker
        self.pub_to_broker = self._context.socket(zmq.PUB)
        self.pub_to_broker.connect(self._config.message_broker.sub_address)

        self.sub_to_broker = self._context.socket(zmq.SUB)
        self.sub_to_broker.connect(self._config.message_broker.pub_address)
        zmqstream.ZMQStream(self.sub_to_broker).on_recv(self._on_message)

    @message_handler(SEND_BY_UID_PREFIX)
    def send_by_uid(self, uid, msg):
        self._connman.send_by_uid(uid, msg)

    @message_handler(PUBLISH_TO_CHANNEL_PREFIX)
    def publish_to_channel(self, channel, msg):
        self._connman.publish_to_channel(channel, msg, True)

    def setup(self, connman, root):
        if not isinstance(connman, DistributedConnectionManager):
            raise InstanceError('connman', DistributedConnectionManager)
        super().setup(connman, root)


class LocationMessageManager(BasicMessageManager):

    def __init__(self, config):
        super().__init__(config)
        self.loc_input_sockets = {}
        self._loc_pub_addresses = {}

    def connect(self):
        super().connect()

        # setup connection with location_manager
        self._sub_to_locman = self._context.socket(zmq.SUB)
        self._sub_to_locman.connect(self._config.location_manager.pub_address)
        self._sub_to_locman.setsockopt(zmq.SUBSCRIBE, b'')
        zmqstream.ZMQStream(self._sub_to_locman).on_recv(self._on_message)

        self._req_to_locman = self._context.socket(zmq.REQ)
        self._req_to_locman.connect(self._config.location_manager.rep_address)

        # create socket for receiving of messages from locations
        self.sub_to_locs = self._context.socket(zmq.SUB)
        zmqstream.ZMQStream(self.sub_to_locs).on_recv(self._on_message)

    @message_handler(LOCATION_CONNECTED_PREFIX)
    def add_location(self, loc_id, data):
        assert loc_id not in self.loc_input_sockets, 'location already exists'
        push_sock = self._context.socket(zmq.PUSH)
        push_sock.connect(data.pop('pull_address'))
        self.loc_input_sockets[loc_id] = push_sock
        self.sub_to_locs.connect(data['pub_address'])
        self._loc_pub_addresses[loc_id] = data.pop('pub_address')
        self._root.location_added(loc_id, data)

    @message_handler(LOCATION_DISCONNECTED_PREFIX)
    def remove_location(self, loc_id, data):
        sock = self.loc_input_sockets.pop(loc_id)
        sock.close()
        self.sub_to_locs.disconnect(self._loc_pub_addresses.pop(loc_id))
        self._root.location_removed(loc_id)

    @message_handler(PUBLIC_MESSAGE_FROM_LOCATION_PREFIX)
    def location_public(self, location, msg):
        path_prefix = self._config.outer_server.client_location_handler_path
        msg['path'] = '.'.join((path_prefix, msg['path']))
        self._connman.publish_to_location(location, msg)

    @message_handler(PRIVATE_MESSAGE_FROM_LOCATION_PREFIX)
    def location_private(self, location_uid, msg):
        location, uid = location_uid.split(':', 1)
        path_prefix = (self._config.outer_server.
                        location_handler_path.split('.'))
        path = msg['path'].split('.')
        path = path_prefix + path
        kwargs = msg['kwargs']
        if not 'location' in kwargs:
            kwargs['location'] = location
        kwargs['uid'] = uid
        root_dispatch(self._root, path, kwargs, INTERNAL_SIGN)

    def setup(self, connman, root):
        if not isinstance(connman, LocationConnectionManager):
            raise InstanceError('connman', LocationConnectionManager)
        if not isinstance(root, LocationRoot):
            raise InstanceError('root', LocationRoot)
        super().setup(connman, root)

        # connect to existing locations
        req_sock = self._req_to_locman
        del self._req_to_locman
        req_sock.send(GET_LOCATIONS_INFO.encode('utf-8'))
        locations = msgpack.loads(req_sock.recv(), encoding='utf-8')
        req_sock.close()
        for loc_id, data in locations.items():
            self.add_location(loc_id, data)


class LocationRoot(object, metaclass=ABCMeta):

    @abstractmethod
    def location_added(self, loc_id, data):
        pass

    @abstractmethod
    def location_removed(self, loc_id):
        pass

