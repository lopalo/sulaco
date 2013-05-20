from abc import ABCMeta, abstractmethod
from tornado.ioloop import IOLoop
from tornado.gen import Task


class LockError(Exception):
    pass


class BasicLock(metaclass=ABCMeta):

    def __init__(self, check_period=0.005, ioloop=None):
        self._check_period = check_period # seconds
        self._ioloop = ioloop or IOLoop.instance()

    @abstractmethod
    def acquire(self, key, blocking=True):
        pass

    @abstractmethod
    def release(self, key):
        """ Should return iterable """
        pass


class Lock(BasicLock):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._keys = set()

    def acquire(self, key, blocking=True):
        if key in self._keys:
            if blocking:
                while key in self._keys:
                    time = self._ioloop.time() + self._check_period
                    yield Task(self._ioloop.add_timeout, time) #TODO: implement async sleep
            else:
                return False
        self._keys.add(key)
        return True

    def release(self, key):
        if not key in self._keys:
            raise LockError('Try to release unlocked lock')
        self._keys.remove(key)
        return []


class RedisLock(BasicLock):
    #TODO: add timeout

    def __init__(self, *args, **kwargs):
        self._client = kwargs.pop('client')
        super().__init__(*args, **kwargs)

    def acquire(self, key, blocking=True):
        ok = yield self._client.setnx(key, 1)
        if ok:
            return True
        if not blocking:
            return False
        while not ok:
            time = self._ioloop.time() + self._check_period
            yield Task(self._ioloop.add_timeout, time)
            ok = yield self._client.setnx(key, 1)
        return True

    def release(self, key):
        ok = yield self._client.delete(key)
        if not ok:
            raise LockError('Try to release unlocked lock')


