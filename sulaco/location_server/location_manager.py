import argparse
import msgpack
import zmq
import logging

from time import time
from zmq.eventloop import zmqstream
from tornado.ioloop import IOLoop, PeriodicCallback

from sulaco.utils import Config, UTCFormatter, ColorUTCFormatter
from zmq.eventloop.ioloop import install

from sulaco import (
    GET_LOCATIONS_INFO,
    LOCATION_DISCONNECTED_PREFIX,
    LOCATION_CONNECTED_PREFIX)
from sulaco.location_server import (
    DISCONNECT_MESSAGE, HEARTBEAT_MESSAGE,
    CONNECT_MESSAGE)


logger = logging.getLogger('location_manager')


def start_location_manager(config):
    conf = config.location_manager
    locations = {}
    last_heartbeats = {}
    ioloop = IOLoop.instance()

    def disconnect(loc_id):
        del locations[loc_id]
        del last_heartbeats[loc_id]
        msg = LOCATION_DISCONNECTED_PREFIX + loc_id
        pub_sock.send_multipart([msg.encode('utf-8'), msgpack.dumps(None)])
        logger.info("Location '%s' disconnected", loc_id)

    ### handlers ###

    def request(stream, parts):
        logger.debug("Parts of request message: %s", parts)
        msg = parts[0].decode('utf-8')
        if msg == CONNECT_MESSAGE:
            loc_id, data = parts[1:]
            loc_id = loc_id.decode('utf-8')
            if loc_id in locations:
                stream.send(msgpack.dumps(False))
                return
            stream.send(msgpack.dumps(True))
            topic = (LOCATION_CONNECTED_PREFIX + loc_id).encode('utf-8')
            pub_sock.send(topic, zmq.SNDMORE)
            pub_sock.send(data)
            locations[loc_id] = msgpack.loads(data, encoding='utf-8')
            last_heartbeats[loc_id] = ioloop.time()
            logger.info("Location '%s' connected", loc_id)
        elif msg == GET_LOCATIONS_INFO:
            stream.send(msgpack.dumps(locations))
        else:
            logger.warning('Unknown request message: %s', msg)

    def input(parts):
        logger.debug("Parts of input message: %s", parts)
        msg, loc_id = parts
        msg = msg.decode('utf-8')
        loc_id = loc_id.decode('utf-8')
        if msg == HEARTBEAT_MESSAGE:
            if loc_id not in locations:
                logger.warning('Unknown location: %s', loc_id)
                return
            last_heartbeats[loc_id] = ioloop.time()
        elif msg == DISCONNECT_MESSAGE:
            if loc_id not in locations:
                logger.warning('Unknown location: %s', loc_id)
                return
            disconnect(loc_id)
        else:
            logger.warning('Unknown request message: %s', msg)


    def heartbeats_checker():
        logger.debug('Check heartbeats')
        for loc_id, t in last_heartbeats.copy().items():
            if ioloop.time() - t < conf.max_heartbeat_silence:
                continue
            disconnect(loc_id)


    context = zmq.Context()

    rep_sock = context.socket(zmq.REP)
    rep_sock.bind(conf.rep_address)
    zmqstream.ZMQStream(rep_sock).on_recv_stream(request)

    pull_sock = context.socket(zmq.PULL)
    pull_sock.bind(conf.pull_address)
    zmqstream.ZMQStream(pull_sock).on_recv(input)

    pub_sock = context.socket(zmq.PUB)
    pub_sock.bind(conf.pub_address)

    period = conf.heartbeats_checker_period * 1000
    PeriodicCallback(heartbeats_checker, period).start()

    ioloop.start()


if __name__ == "__main__":
    install()

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', action='store', dest='config',
                        help='path to config file', type=str, required=True)
    parser.add_argument('-d', '--debug', action='store_true',
                        dest='debug', help='set debug level of logging')
    parser.add_argument('-lf', '--log-file', action='store', dest='log_file',
                        help='path to log file', type=str, default=None)
    options = parser.parse_args()

    logger.setLevel(logging.DEBUG if options.debug else logging.INFO)
    logger.propagate = False
    if options.log_file is None:
        handler = logging.StreamHandler()
        formatter = ColorUTCFormatter()
    else:
        handler = logging.FileHandler(options.log_file)
        formatter = UTCFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    start_location_manager(Config.load_yaml(options.config))

