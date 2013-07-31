from time import time
from tornado import testing
from tornado import gen
from sulaco.utils import sleep
from sulaco.utils.db import RedisClient
from sulaco.utils.lock import lock_factory, redis_lock_factory, LockError


class BasicTestLock(object):
    """ Use for testing of subclasses of sulaco.lock.BasicLock """

    key = 'test_key'

    @testing.gen_test
    def test_not_blocking(self):
        ok = yield from self.lock.acquire(blocking=False)
        self.assertTrue(ok)
        ok = yield from self.lock.acquire(blocking=False)
        self.assertFalse(ok)
        self.lock.release()
        yield from sleep(0.01, self.io_loop)
        ok = yield from self.lock.acquire(blocking=False)
        self.assertTrue(ok)

    @testing.gen_test
    def test_blocking(self):
        self._blocking_coroutine(1) # run in parallel
        start = self.io_loop.time()
        yield from self.lock.acquire()
        self.assertLessEqual(1, self.io_loop.time() - start)

    @testing.gen_test
    def test_context_manager(self):
        self._blocking_coroutine(1) # run in parallel
        start = self.io_loop.time()
        with (yield from self.lock):
            self.assertLessEqual(1, self.io_loop.time() - start)
            ok = yield from self.lock.acquire(blocking=False)
            self.assertFalse(ok)
        ok = yield from self.lock.acquire(blocking=False)
        self.assertTrue(ok)


    @gen.coroutine
    def _blocking_coroutine(self, dtime):
        yield from self.lock.acquire()
        yield from sleep(dtime, self.io_loop)
        self.lock.release()

    def test_release_unlocked(self):
        with self.assertRaisesRegexp(LockError,
                    'Try to release unlocked lock'):
            self.lock.release()
            self.wait(timeout=0.1)

    @testing.gen_test
    def test_timeout_error(self):
        yield from self.lock.acquire()
        with self.assertRaisesRegexp(LockError, 'Timeout expired'):
            yield from self.lock.acquire(timeout=0.5)


class TestLock(BasicTestLock, testing.AsyncTestCase):

    def setUp(self):
        super().setUp()
        self.lock = lock_factory(ioloop=self.io_loop)(self.key)


class TestRedisLock(BasicTestLock, testing.AsyncTestCase):
    db = 0
    key = 'test_redis_lock'

    def setUp(self):
        super().setUp()
        self.client = RedisClient(io_loop=self.io_loop)
        self.client.connect()
        self.client.select(self.db)
        self.lock = redis_lock_factory(self.client, self.io_loop)(self.key)
        self.client.delete(self.lock.key_prefix + self.key,
                                callback=lambda r: self.stop())
        self.wait()

    @testing.gen_test
    def test_ttl(self):
        yield from self.lock.acquire()
        yield from sleep(0.01, self.io_loop)
        ret = yield from self.client.ttl(self.lock.key_prefix + self.key)
        self.assertLessEqual(50, ret)


if __name__ == '__main__':
    testing.main()
