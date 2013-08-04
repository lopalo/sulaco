#TODO: move all utils files to a higher directory
import time
import logging
import functools
import yaml

from tornado.ioloop import IOLoop
from tornado.gen import Task
from tornado.concurrent import return_future


class Config(object):

    def __init__(self, dct, is_root):
        self._dct = dct
        self.__dict__.update(dct)
        self._root = is_root

    @classmethod
    def load_yaml(cls, filename):
        with open(filename, 'rb') as f:
            dct = yaml.safe_load(f)
            return cls(dct, True)

    def __getattribute__(self, name):
        val = super().__getattribute__(name)
        if not name.startswith('_') and isinstance(val, dict):
            return self.__class__(val, False)
        return val

    def __getitem__(self, name):
        return self._dct[name]


class Sender(object):

    def __init__(self, send, path=tuple()):
        assert callable(send), send
        self._send = send
        self._path = path

    def __getattr__(self, name):
        return Sender(self._send, self._path + (name,))

    def __call__(self, **kwargs):
        message = dict(kwargs=kwargs, path='.'.join(self._path))
        return self._send(message)


class InstanceError(Exception):

    def __init__(self, name, cls):
        text = "'{}' should be an instance of {}".format(name, cls)
        super().__init__(text)


class SubclassError(Exception):

    def __init__(self, name, cls):
        text = "'{}' should be a subclass of {}".format(name, cls)
        super().__init__(text)


class UTCFormatter(logging.Formatter):
    converter = time.gmtime
    fmt = '[%(asctime)s] %(levelname)s %(name)s: %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'

    def __init__(self):
        super().__init__(self.fmt, self.datefmt)


class ColorUTCFormatter(UTCFormatter):

    RED = '\x1b[31m'
    YELLOW = '\x1b[33m'
    GREEN = '\x1b[32m'
    CYAN = '\x1b[36m'

    colors = {logging.DEBUG: CYAN,
              logging.INFO: GREEN,
              logging.ERROR: RED,
              logging.WARNING: YELLOW}

    def format(self, record):
        msg = super().format(record)
        no = record.levelno
        if no not in self.colors:
            return msg
        color = self.colors[no]
        return color + msg + '\x1b[0m'


def sleep(seconds, ioloop=None):
    ioloop = ioloop or IOLoop.instance()
    time = ioloop.time() + seconds
    yield Task(ioloop.add_timeout, time)


class Proxy(object):

    def __init__(self, obj):
        self._obj = obj

    def __getattribute__(self, name):
        if name.startswith('__') and name.endswith('__'):
            return getattr(self._obj, name)
        try:
            return super().__getattribute__(name)
        except AttributeError:
            return getattr(self._obj, name)

    def __repr__(self):
        return repr(self._obj)

    def __str__(self):
        return str(self._obj)


class FutureIterProxy(Proxy):

   def __iter__(self):
        return (yield self)


def return_iter_future(f):
    wrapped = return_future(f)
    @functools.wraps(wrapped)
    def wrap(*args, **kwargs):
        fut =  wrapped(*args, **kwargs)
        return FutureIterProxy(fut)
    return wrap


def get_pairs(items):
    i = iter(items)
    while True:
        yield next(i), next(i)

