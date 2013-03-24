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

