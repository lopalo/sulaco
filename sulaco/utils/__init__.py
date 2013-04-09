import yaml


class Config(object):


    def __init__(self, dct, is_root):
        self._dct = dct
        self.__dict__.update(dct)
        self._root = is_root

    @classmethod
    def load_yaml(cls, filename):
        with open(filename, 'rb') as f:
            dct = yaml.load(f)
            return cls(dct, True)

    def __getattribute__(self, name):
        val = super(Config, self).__getattribute__(name)
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
        self._send(message)

