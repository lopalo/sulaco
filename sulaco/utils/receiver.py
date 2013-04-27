import json
import types
from functools import wraps, partial

from tornado.ioloop import IOLoop
from tornado.gen import Runner, Task

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


def _dispatch(obj, path, kwargs, sign, index, stack):
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
        meth(path, kwargs, sign, index, stack)
    if meth_type == MESSAGE_RECEIVER:
        meth(kwargs, stack)

#TODO: rewrite using yield from
def message_router(sign=None):
    assert sign in SIGNS, "unknown sign '{}'".format(sign)

    def _message_router(func):
        func.__sign__ = sign
        func.__receiver__method__ = MESSAGE_ROUTER
        @wraps(func)
        def new_func(self, path, kwargs, sign, index, stack):
            def next_step(obj, callback=None):
                if callback is not None:
                    stack.append(callback)
                _dispatch(obj, path, kwargs, sign, index + 1, stack)

            def finish_cb(ok=True):
                if stack:
                    stack.pop()(ok)

            try:
                gen = func(self, next_step, **kwargs)
            except Exception:
                finish_cb(False)
                raise
            if isinstance(gen, types.GeneratorType):
                runner = SyncRunner(gen, finish_cb)
                runner.run()
        return new_func
    return _message_router


def message_receiver(sign=None):
    assert sign in SIGNS, "unknown sign '{}'".format(sign)

    def _message_receiver(func):
        func.__sign__ = sign
        func.__receiver__method__ = MESSAGE_RECEIVER
        @wraps(func)
        def new_func(self, kwargs, stack):
            def finish_cb(ok=True):
                if stack:
                    stack.pop()(ok)

            try:
                gen = func(self, **kwargs)
            except Exception:
                finish_cb(False)
                raise
            if isinstance(gen, types.GeneratorType):
                runner = SyncRunner(gen, finish_cb)
                runner.run()
            else:
               finish_cb()
        return new_func
    return _message_receiver


class SyncRunner(Runner):

    def __init__(self, gen, finish_callback):
        super(SyncRunner, self).__init__(gen, lambda: None)
        self.finish_callback = finish_callback

    def run(self):
        if self.running or self.finished:
            return
        try:
            super(SyncRunner, self).run()
        except Exception:
            if self.finished:
                self.finish_callback(False)
            raise
        if self.finished:
            self.finish_callback()


def root_dispatch(root, path, kwargs, sign):
    stack = []
    if isinstance(root, Loopback):
        stack.append(root.process_loopback_callbacks)
    _dispatch(root, path, kwargs, sign, 0, stack)


class Loopback(object):

    def __init__(self, *args, **kwargs):
        super(Loopback, self).__init__(*args, **kwargs)
        self._callbacks = []

    def process_loopback_callbacks(self, ok):
        ioloop = IOLoop.instance()
        for cb in self._callbacks:
            ioloop.add_callback(cb)
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

#TODO: implement ProxyReceiver with default_receiver that takes rest of the path
