import json
from functools import wraps

class Sender(object):

    def __init__(self, conn, path=tuple()):
        self._conn = conn
        self._path = path

    def __getattr__(self, name):
        return Sender(self._conn, self._path + (name,))

    def __call__(self, **kwargs):
        message = dict(kwargs=kwargs, path='.'.join(self._path))
        self._conn.send(message)


MESSAGE_ROUTER = '__message_router__'
MESSAGE_RECEIVER = '__message_receiver__'


class ReceiverError(Exception):
    pass


class SignError(Exception):
    pass


def dispatch(obj, index, path, signed, kwargs):
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
    if not signed and getattr(meth, '__must_be_signed__', True):
        raise SignError()
    if meth_type == MESSAGE_ROUTER:
        meth(index, path, signed, kwargs)
    if meth_type == MESSAGE_RECEIVER:
        meth(**kwargs)


def message_router(func):
    func.__receiver__method__ = MESSAGE_ROUTER
    @wraps(func)
    def new_func(self, index, path, signed, kwargs):
        obj = func(self, **kwargs)
        assert obj is not None
        dispatch(obj, index+1, path, signed, kwargs)
    return new_func


def message_receiver(func):
    func.__receiver__method__ = MESSAGE_RECEIVER
    return func


def unsigned(func):
    func.__must_be_signed__ = False
    return func
