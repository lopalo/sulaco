import json
from functools import wraps


MESSAGE_ROUTER = '__message_router__'
MESSAGE_RECEIVER = '__message_receiver__'

USER_SIGN = '__user_sign__'
INTERNAL_SIGN = '__internal_sign__'
INTERNAL_USER_SIGN = '__internal_user_sign__'
SIGNS = (None, USER_SIGN, INTERNAL_USER_SIGN, INTERNAL_SIGN,)


class ReceiverError(Exception):
    pass


class SignError(Exception):
    pass


def dispatch(obj, index, path, sign, kwargs):
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

    if (meth.__sign__ == INTERNAL_USER_SIGN and
            sign not in (INTERNAL_SIGN, USER_SIGN)):
        raise SignError("Need internal or user's sign")
    elif meth.__sign__ == INTERNAL_SIGN and sign != INTERNAL_SIGN:
        raise SignError("Need internal sign")
    elif meth.__sign__ == USER_SIGN and sign != USER_SIGN:
        raise SignError("Need user's sign")

    if meth_type == MESSAGE_ROUTER:
        meth(index, path, sign, kwargs)
    if meth_type == MESSAGE_RECEIVER:
        meth(**kwargs)


def message_router(sign=None):
    assert sign in SIGNS, "unknown sign '{}'".format(sign)
    def _message_router(func):
        func.__sign__ = sign
        func.__receiver__method__ = MESSAGE_ROUTER
        @wraps(func)
        def new_func(self, index, path, sign, kwargs):
            obj = func(self, **kwargs)
            assert obj is not None
            dispatch(obj, index+1, path, sign, kwargs)
        return new_func
    return _message_router


def message_receiver(sign=None):
    assert sign in SIGNS, "unknown sign '{}'".format(sign)
    def _message_receiver(func):
        func.__sign__ = sign
        func.__receiver__method__ = MESSAGE_RECEIVER
        return func
    return _message_receiver


