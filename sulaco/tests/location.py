import argparse

from sulaco.utils import Config
from sulaco.utils.zmq import install
from sulaco.utils.receiver import message_receiver, INTERNAL_SIGN
from sulaco.location_server.gateway import Gateway

class Root(object):

    def __init__(self, gateway, ident, config):
        self._gateway = gateway
        self._config = config
        self._users = {}
        self._ident = ident

    @message_receiver(INTERNAL_SIGN)
    def enter(self, user):
        uid = user['uid']
        self._users[uid] = user
        self._gateway.prs(uid).init(users=list(self._users.values()),
                                    ident=self._ident)
        self._gateway.pubs.user_connected(user=user)

    @message_receiver(INTERNAL_SIGN)
    def move_to(self, uid, location):
        del self._users[uid]
        self._gateway.prs(uid).enter(location=location)
        self._gateway.pubs.user_disconnected(uid=uid)


def main(options):
    install()
    config = Config.load_yaml(options.config)
    gateway = Gateway(config, options.ident)
    root = Root(gateway, options.ident, config)
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
    options = parser.parse_args()
    main(options)
