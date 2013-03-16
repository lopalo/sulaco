import json
from functools import wraps

class Sender(object):

    def __init__(self, conn, path=tuple()):
        self._conn = conn
        self._path = path

    def __getattr__(self, name):
        return Sender(self._conn, self._path + (name,))

    def __call__(self, **kwargs):
        assert 'path' not in kwargs, "Keyword argument 'path' is not available"
        kwargs['path'] = '.'.join(self._path)
        self._conn.send(kwargs)


MESSAGE_ROUTER = '__message_router__'
MESSAGE_RECEIVER = '__message_receiver__'


class ReceiverError(Exception):
    pass


def dispatch(obj, index, path, kwargs):
    """ Additional arguments need add to kwargs dict """

    name = path[index]
    max_index = len(path) - 1

    path_info = list(path)
    path_info[index] = '|' + name + '|'
    pinfo = 'Path: ' + '.'.join(path_info)

    meth = getattr(obj, name, None)
    if meth is None:
        etxt = '{} has no method {}. {}'
        raise ReceiverError(etxt.format(obj, name,  pinfo))
    meth_type = getattr(meth, '__receiver__method__', '')
    if meth_type not in (MESSAGE_RECEIVER, MESSAGE_ROUTER):
        etxt = "Method '{}' of {} is forbidden. {}"
        raise ReceiverError(etxt.format(name, obj, pinfo))
    if meth_type == MESSAGE_RECEIVER and index != max_index:
        etxt = "Got receiver method '{}' of {}, expected router. {}"
        raise ReceiverError(etxt.format(name, obj, pinfo))
    if meth_type == MESSAGE_ROUTER and index == max_index:
        etxt = "Got router method '{}' of {}, expected receiver. {}"
        raise ReceiverError(etxt.format(name, obj, pinfo))
    if meth_type == MESSAGE_ROUTER:
        meth(index, path, kwargs)
    if meth_type == MESSAGE_RECEIVER:
        meth(**kwargs)


def message_router(func):
    func.__receiver__method__ = MESSAGE_ROUTER
    @wraps(func)
    def new_func(self, index, path, kwargs):
        obj = func(self, **kwargs)
        assert obj is not None
        dispatch(obj, index+1, path, kwargs)
    return new_func


def message_receiver(func):
    func.__receiver__method__ = MESSAGE_RECEIVER
    return func




