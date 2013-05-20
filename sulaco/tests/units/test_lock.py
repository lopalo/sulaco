from time import time
from tornado import testing
from tornado import gen
from sulaco.redis import Client
from sulaco.utils.lock import Lock, RedisLock, LockError


class BasicTestLock(object):
    """ Use for testing of subclasses of sulaco.lock.BasicLock """

    key = 'test_key'

    def test_not_blocking(self):
        ok = yield from self.lock.acquire(self.key, blocking=False)
        self.assertTrue(ok)
        ok = yield from self.lock.acquire(self.key, blocking=False)
        self.assertFalse(ok)
        yield from self.lock.release(self.key)
        ok = yield from self.lock.acquire(self.key, blocking=False)
        self.assertTrue(ok)

    def test_blocking(self):
        self._blocking_coroutine(1)
        start = time()
        yield from self.lock.acquire(self.key)
        self.assertLessEqual(1, time() - start)

    @gen.coroutine
    def _blocking_coroutine(self, dtime):
        yield from self.lock.acquire(self.key)
        yield gen.Task(self.io_loop.add_timeout, time() + dtime)
        yield from self.lock.release(self.key)

    def test_release_unlocked(self):
        with self.assertRaisesRegexp(LockError,
                    'Try to release unlocked lock'):
            yield from self.lock.release(self.key)


class TestLock(BasicTestLock, testing.AsyncTestCase):

    def setUp(self):
        super().setUp()
        self.lock = Lock(ioloop=self.io_loop)

    @testing.gen_test
    def test_not_blocking(self):
        yield from super().test_not_blocking()

    @testing.gen_test
    def test_blocking(self):
        yield from super().test_blocking()

    @testing.gen_test
    def test_release_unlocked(self):
        yield from super().test_release_unlocked()


class TestRedisLock(BasicTestLock, testing.AsyncTestCase):
    db = 0
    key = 'test_redis_lock'

    def setUp(self):
        super().setUp()
        self.client = Client(selected_db=self.db, io_loop=self.io_loop)
        self.lock = RedisLock(ioloop=self.io_loop, client=self.client)

    @testing.gen_test
    def test_not_blocking(self):
        yield self.client.delete(self.key)
        yield from super().test_not_blocking()

    @testing.gen_test
    def test_blocking(self):
        yield self.client.delete(self.key)
        yield from super().test_blocking()

    @testing.gen_test
    def test_release_unlocked(self):
        yield self.client.delete(self.key)
        yield from super().test_release_unlocked()


if __name__ == '__main__':
    testing.main()
