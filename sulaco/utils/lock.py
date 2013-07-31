from abc import ABCMeta, abstractmethod
from tornado.ioloop import IOLoop
from sulaco.utils import sleep


class LockError(Exception):
    pass


class BasicLock(metaclass=ABCMeta):

    def __init__(self, key, ioloop=None):
        self._key = key
        self._ioloop = ioloop or IOLoop.instance()

    @abstractmethod
    def acquire(self, key, blocking=True, timeout=10, check_period=0.005):
        pass

    @abstractmethod
    def release(self, key):
        pass

    def __iter__(self):
        yield from self.acquire()
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()


def lock_factory(keys=None, ioloop=None):
    if keys is None:
        keys = set()
    return lambda key: Lock(key=key, keys=keys, ioloop=ioloop)


class Lock(BasicLock):

    def __init__(self, *args, **kwargs):
        self._keys = kwargs.pop('keys')
        super().__init__(*args, **kwargs)

    def acquire(self, blocking=True, timeout=10, check_period=0.005):
        if self._key in self._keys:
            if blocking:
                start = self._ioloop.time()
                while self._key in self._keys:
                    yield from sleep(check_period, self._ioloop)
                    if self._ioloop.time() - start >= timeout:
                        raise LockError('Timeout expired')
            else:
                return False
        self._keys.add(self._key)
        return True

    def release(self):
        if not self._key in self._keys:
            raise LockError('Try to release unlocked lock')
        self._keys.remove(self._key)


def redis_lock_factory(client, ioloop=None):
    return lambda key: RedisLock(key=key, client=client, ioloop=ioloop)


class RedisLock(BasicLock):
    key_prefix = 'redis_lock:'
    key_ttl = 60

    def __init__(self, *args, **kwargs):
        self._client = kwargs.pop('client')
        super().__init__(*args, **kwargs)

    def acquire(self, blocking=True, timeout=10, check_period=0.005):
        key = self.key_prefix + str(self._key)
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

    def release(self):
        key = self.key_prefix + str(self._key)
        self._client.delete(key, callback=self._check_release) # check without waiting

    def _check_release(self, ok):
        if not ok:
            raise LockError('Try to release unlocked lock')


