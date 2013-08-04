from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from tornado.ioloop import IOLoop
from sulaco.utils import sleep


class LockError(Exception):
    pass


class BasicLock(metaclass=ABCMeta):

    def __init__(self, ioloop=None):
        self._ioloop = ioloop or IOLoop.instance()

    @abstractmethod
    def acquire(self, key, blocking=True, timeout=10, check_period=0.005):
        pass

    @abstractmethod
    def release(self, key):
        pass

    @contextmanager
    def _ctxman(self, key):
        try:
            yield True
        finally:
            self.release(key)

    def atomic(self, key):
        yield from self.acquire(key)
        return self._ctxman(key)


class Lock(BasicLock):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._keys = set()

    def acquire(self, key, blocking=True, timeout=10, check_period=0.005):
        if key in self._keys:
            if blocking:
                start = self._ioloop.time()
                while key in self._keys:
                    yield from sleep(check_period, self._ioloop)
                    if self._ioloop.time() - start >= timeout:
                        raise LockError('Timeout expired')
            else:
                return False
        self._keys.add(key)
        return True

    def release(self, key):
        if not key in self._keys:
            raise LockError('Try to release unlocked lock')
        self._keys.remove(key)


class RedisLock(BasicLock):
    key_prefix = 'redis_lock:'
    key_ttl = 60

    def __init__(self, *args, **kwargs):
        self._client = kwargs.pop('client')
        super().__init__(*args, **kwargs)

    def acquire(self, key, blocking=True, timeout=10, check_period=0.005):
        key = self.key_prefix + str(key)
        ttl = timeout * self.key_ttl
        ok = yield from self._client.setnx(key, 1)
        if ok:
            self._client.expire(key, self.key_ttl) # set without waiting
            return True
        if not blocking:
            return False
        start = self._ioloop.time()
        while not ok:
            yield from sleep(check_period, self._ioloop)
            if self._ioloop.time() - start >= timeout:
                raise LockError('Timeout expired')
            ok = yield from self._client.setnx(key, 1)
        self._client.expire(key, self.key_ttl) # set without waiting
        return True

    def release(self, key):
        key = self.key_prefix + str(key)
        self._client.delete(key, callback=self._check_release) # check without waiting

    def _check_release(self, ok):
        if not ok:
            raise LockError('Try to release unlocked lock')


