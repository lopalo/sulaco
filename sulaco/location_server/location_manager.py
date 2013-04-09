from zmq.eventloop import zmqstream

import argparse
import json
import zmq
from time import time
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback

from sulaco.utils import Config

from sulaco import (
    GET_LOCATIONS_INFO, LOCATIONS_INFO,
    LOCATION_DISCONNECTED_PREFIX,
    LOCATION_CONNECTED_PREFIX)
from sulaco.location_server import (
    DISCONNECT_MESSAGE, HEARTBEAT_MESSAGE,
    CONNECT_MESSAGE)


def start_location_manager(config):
    conf = config.location_manager
    locations = {}
    last_heartbeats = {}

    def disconnect(loc_id):
        del locations[loc_id]
        del last_heartbeats[loc_id]
        msg = LOCATION_DISCONNECTED_PREFIX + loc_id
        pub_sock.send_multipart([msg, 'null'])

    ### handlers ###

    def request(stream, parts):
        msg = parts[0]
        if msg == CONNECT_MESSAGE:
            loc_id, data = parts[1:]
            if loc_id in locations:
                stream.send_json(False)
                return
            stream.send_json(True)
            pub_sock.send(LOCATION_CONNECTED_PREFIX + loc_id, zmq.SNDMORE)
            pub_sock.send(data)
            locations[loc_id] = json.loads(data)
            last_heartbeats[loc_id] = time()
        elif msg == GET_LOCATIONS_INFO:
            stream.send(LOCATIONS_INFO, zmq.SNDMORE)
            stream.send_json(locations)
        else:
            #TODO: log unknown message
            raise Exception()

    def input(parts):
        msg, loc_id = parts
        if msg == HEARTBEAT_MESSAGE:
            if loc_id not in locations:
                #TODO: log unknown location
                raise Exception()
                return
            last_heartbeats[loc_id] = time()
        elif msg == DISCONNECT_MESSAGE:
            if loc_id not in locations:
                #TODO: log unknown location
                raise Exception()
                return
            disconnect(loc_id)
        else:
            #TODO: log unknown message
            raise Exception()

    def heartbeats_checker():
        for loc_id, t in last_heartbeats.copy().items():
            if time() - t < conf.max_heartbeat_silence:
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

    IOLoop.instance().start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', action='store', dest='config',
                        help='path to config file', type=str, required=True)
    options = parser.parse_args()
    start_location_manager(Config.load_yaml(options.config))

