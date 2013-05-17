from abc import ABCMeta, abstractmethod
from tornado.ioloop import IOLoop
from tornado.gen import Task


class LockError(Exception):
    pass


class BasicLock(metaclass=ABCMeta):

    def __init__(self, check_period=0.005):
        self._check_period = check_period # seconds
        self._ioloop = IOLoop.instance()

    @abstractmethod
    def acquire(self, key, blocking=True):
        pass

    @abstractmethod
    def release(self, key):
        pass


class Lock(BasicLock):
    #TODO: write test checking of ioloop._timeouts

    def __init__(self, check_period=0.005):
        super().__init__(check_period)
        self._keys = set()

    def acquire(self, key, blocking=True):
        if key in self._keys:
            if blocking:
                while key in self._keys:
                    time = self._ioloop.time() + self._check_period
                    yield Task(self._ioloop.add_timeout, time)
            else:
                return False
        self._keys.add(key)
        return True

    def release(self, key):
        if not key in self._keys:
            raise LockError('Try to release unlocked lock')
        self._keys.remove(key)

