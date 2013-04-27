import argparse
import zmq
from sulaco.utils import Config


def forward(config):
    context = zmq.Context()

    sub = context.socket(zmq.SUB)
    sub.bind(config.message_broker.sub_address)
    sub.setsockopt(zmq.SUBSCRIBE, b'')

    pub = context.socket(zmq.PUB)
    pub.bind(config.message_broker.pub_address)

    zmq.device(zmq.FORWARDER, sub, pub)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', action='store', dest='config',
                        help='path to config file', type=str, required=True)
    options = parser.parse_args()
    forward(Config.load_yaml(options.config))
