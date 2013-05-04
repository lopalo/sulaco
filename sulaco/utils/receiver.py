import sys
import json
from abc import ABCMeta, abstractmethod
from types import GeneratorType
from functools import wraps, partial

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

from sulaco.utils import Sender


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


def _dispatch(obj, path, kwargs, sign, index, root=False):
    """ Additional arguments necessary to add to kwargs dict """

    name = path[index]
    max_index = len(path) - 1

    path_info = list(path)
    path_info[index] = '|' + name + '|'
    pinfo = 'Path: ' + '.'.join(path_info)

    try:
        meth = getattr(obj, name, None)
        if meth is None:
            if isinstance(obj, ProxyMixin):
                rest_path = path[index:]
                if not root:
                    obj.proxy_method(rest_path, sign, kwargs)
                    return dummy_generator()
                return partial(meth, rest_path, sign)
            else:
                etxt = '{} has no method {}.'
                raise ReceiverError(etxt.format(obj, name))

        meth_type = getattr(meth, '__receiver__method__', '')
        if meth_type not in (MESSAGE_RECEIVER, MESSAGE_ROUTER):
            etxt = "Method '{}' of {} is forbidden."
            raise ReceiverError(etxt.format(name, obj, pinfo))
        if meth_type == MESSAGE_RECEIVER and index != max_index:
            etxt = "Got receiver method '{}' of {}, expected router."
            raise ReceiverError(etxt.format(name, obj))
        if meth_type == MESSAGE_ROUTER and index == max_index:
            etxt = "Got router method '{}' of {}, expected receiver."
            raise ReceiverError(etxt.format(name, obj))

        if (meth.__sign__ == INTERNAL_USER_SIGN and
                sign not in (INTERNAL_SIGN, USER_SIGN)):
            raise SignError("Necessary internal or user's sign.")
        elif meth.__sign__ == INTERNAL_SIGN and sign != INTERNAL_SIGN:
            raise SignError("Necessary internal sign.")
        elif meth.__sign__ == USER_SIGN and sign != USER_SIGN:
            raise SignError("Necessary user's sign.")

        if meth_type == MESSAGE_ROUTER:
            if not root:
                return meth(path, kwargs, sign, index)
            return partial(meth, path, kwargs, sign, index)
        if meth_type == MESSAGE_RECEIVER:
            if not root:
                return meth(kwargs)
            return partial(meth, kwargs)
    except Exception:
        type, value, traceback = sys.exc_info()
        args = list(value.args)
        args[0] = '{} {}'.format(args[0], pinfo)
        raise type(*args)


def message_router(sign=None, pass_sign=False):
    assert sign in SIGNS, "unknown sign '{}'".format(sign)

    def _message_router(func):
        func.__sign__ = sign
        func.__receiver__method__ = MESSAGE_ROUTER
        @wraps(func)
        def new_func(self, path, kwargs, sign, index):
            def next_step(obj):
                return _dispatch(obj, path, kwargs, sign, index + 1)
            if pass_sign:
                result = func(self, next_step, sign, **kwargs)
            else:
                result = func(self, next_step, **kwargs)
            error = '{} must return generator'.format(func)
            assert isinstance(result, GeneratorType), error
            return result
        return new_func
    return _message_router


def message_receiver(sign=None):
    assert sign in SIGNS, "unknown sign '{}'".format(sign)

    def _message_receiver(func):
        func.__sign__ = sign
        func.__receiver__method__ = MESSAGE_RECEIVER
        @wraps(func)
        def new_func(self, kwargs):
            result = func(self, **kwargs)
            if isinstance(result, GeneratorType):
                pass
            elif result is None:
                result = dummy_generator()
            else:
                error = '{} must return generator or None'.format(func)
                raise AssertionError(error)
            return result
        return new_func
    return _message_receiver


def dummy_generator():
    return
    yield


def root_dispatch(root, path, kwargs, sign):
    func = _dispatch(root, path, kwargs, sign, 0, True)
    future = coroutine(func)()
    if isinstance(root, LoopbackMixin):
        future.add_done_callback(root.process_loopback_callbacks)
    else:
        def check_error(future):
            future.result()
        future.add_done_callback(check_error)
    return future


class LoopbackMixin(object):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._callbacks = []

    def process_loopback_callbacks(self, future):
        try:
            future.result()
            ioloop = IOLoop.instance()
            for cb in self._callbacks:
                # run on next iteraion of IOLoop to prevent recursion
                ioloop.add_callback(cb)
        finally:
            self._callbacks = []

    def send_loopback(self, message):
        path = message['path'].split('.')
        kwargs = message['kwargs']
        cb = partial(root_dispatch, self, path, kwargs, INTERNAL_SIGN)
        self._callbacks.append(cb)

    @property
    def lbs(self):
        """ Returns sender that will schedule a dispatch of message"""

        return Sender(self.send_loopback)


class ProxyMixin(object, metaclass=ABCMeta):

    @abstractmethod
    def proxy_method(self, rest_path, sign, kwargs):
        pass

