import argparse
import logging

from sulaco.utils import Config, ColorUTCFormatter
from zmq.eventloop.ioloop import install
from sulaco.utils.receiver import message_receiver, INTERNAL_SIGN, USER_SIGN
from sulaco.location_server.gateway import Gateway


class Root(object):

    def __init__(self, gateway, ident):
        self._gateway = gateway
        self._users = {}
        self._ident = ident

    @message_receiver(INTERNAL_SIGN)
    def enter(self, user, uid):
        self._users[uid] = user
        users = sorted(self._users.values(), key=lambda u: u['uid'])
        self._gateway.prs(uid).init(users=users, ident=self._ident)
        self._gateway.pubs.user_connected(user=user)

    @message_receiver(USER_SIGN)
    def move_to(self, uid, target_location):
        del self._users[uid]
        self._gateway.prs(uid).enter(location=target_location)
        self._gateway.pubs.user_disconnected(uid=uid)


def main(options):
    install()

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if options.debug else logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(ColorUTCFormatter())
    logger.addHandler(handler)

    config = Config.load_yaml(options.config)
    gateway = Gateway(config, options.ident)
    root = Root(gateway, options.ident)
    gateway.setup(root)
    connected = gateway.connect(options.pub_address, options.pull_address)
    if not connected:
        return
    gateway.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-pub', '--pub-address',
                        help='address of zmq pub socket', action='store',
                        dest='pub_address', type=str, required=True)
    parser.add_argument('-pull', '--pull-address',
                        help='address of zmq pull socket', action='store',
                        dest='pull_address', type=str, required=True)
    parser.add_argument('-ident', '--ident',
                        help='ident of location that will be processing',
                        action='store', dest='ident', type=str, required=True)
    parser.add_argument('-c', '--config', action='store', dest='config',
                        help='path to config file', type=str, required=True)
    parser.add_argument('-d', '--debug', action='store_true',
                        dest='debug', help='set debug level of logging')
    options = parser.parse_args()
    main(options)
