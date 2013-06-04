from time import time
from tornado import testing
from tornado import gen
from toredis.client import Client as BasicClient
from toredis.commands_future import RedisCommandsFutureMixin
from sulaco.utils import async_sleep
from sulaco.utils.lock import Lock, RedisLock, LockError


class BasicTestLock(object):
    """ Use for testing of subclasses of sulaco.lock.BasicLock """

    key = 'test_key'

    @testing.gen_test
    def test_not_blocking(self):
        ok = yield from self.lock.acquire(self.key, blocking=False)
        self.assertTrue(ok)
        ok = yield from self.lock.acquire(self.key, blocking=False)
        self.assertFalse(ok)
        self.lock.release(self.key)
        yield async_sleep(0.01, self.io_loop)
        ok = yield from self.lock.acquire(self.key, blocking=False)
        self.assertTrue(ok)

    @testing.gen_test
    def test_blocking(self):
        self._blocking_coroutine(1) # run in parallel
        start = time()
        yield from self.lock.acquire(self.key)
        self.assertLessEqual(1, time() - start)

    @gen.coroutine
    def _blocking_coroutine(self, dtime):
        yield from self.lock.acquire(self.key)
        yield async_sleep(dtime, self.io_loop)
        self.lock.release(self.key)

    def test_release_unlocked(self):
        with self.assertRaisesRegexp(LockError,
                    'Try to release unlocked lock'):
            self.lock.release(self.key)
            self.wait(timeout=0.1)

    @testing.gen_test
    def test_timeout_error(self):
        yield from self.lock.acquire(self.key)
        with self.assertRaisesRegexp(LockError, 'Timeout expired'):
            yield from self.lock.acquire(self.key, timeout=0.5)

class TestLock(BasicTestLock, testing.AsyncTestCase):

    def setUp(self):
        super().setUp()
        self.lock = Lock(ioloop=self.io_loop)


class Client(RedisCommandsFutureMixin, BasicClient):
    pass


class TestRedisLock(BasicTestLock, testing.AsyncTestCase):
    db = 0
    key = 'test_redis_lock'

    def setUp(self):
        super().setUp()
        self.client = Client(io_loop=self.io_loop)
        self.client.connect()
        self.client.select(self.db)
        self.lock = RedisLock(ioloop=self.io_loop, client=self.client)
        self.client.delete(self.lock.key_prefix + self.key,
                                callback=lambda r: self.stop())
        self.wait()

    @testing.gen_test
    def test_ttl(self):
        yield from self.lock.acquire(self.key)
        yield async_sleep(0.01, self.io_loop)
        ret = yield self.client.ttl(self.lock.key_prefix + self.key)
        self.assertLessEqual(50, ret)


if __name__ == '__main__':
    testing.main()
