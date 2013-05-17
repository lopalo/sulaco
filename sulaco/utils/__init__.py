import time
import logging
import yaml


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

