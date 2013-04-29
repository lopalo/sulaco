import unittest
from tornado.ioloop import IOLoop
from tornado.concurrent import return_future
from sulaco.utils import Sender
from sulaco.utils.receiver import (
    root_dispatch, message_router, message_receiver,
    ReceiverError, SignError, USER_SIGN, INTERNAL_SIGN,
    INTERNAL_USER_SIGN)


class Conn(object):

    def __init__(self, root):
        self._root = root
        self.sign = USER_SIGN

    def send(self, msg):
        return self.on_message(msg)

    def on_message(self, msg):
        path = msg['path'].split('.')
        return root_dispatch(self._root, path, msg['kwargs'], self.sign)


class Obj(object):

    def __init__(self):
        self.accumulator = []

    @message_router(USER_SIGN)
    def meth_a(self, next_step, **kwargs):
        yield from next_step(self)

    @message_receiver(USER_SIGN)
    def meth_b(self, a, b):
        self.received_args = (a, b)

    def meth_c(self):
        pass

    @message_router()
    def meth_z(self, next_step):
        yield from next_step(self)

    @message_receiver()
    def meth_x(self):
        pass

    @message_receiver(INTERNAL_SIGN)
    def meth_g(self):
        pass

    @message_receiver(INTERNAL_USER_SIGN)
    def meth_y(self):
        pass

    @message_router()
    def meth_u(self, next_step):
        self.accumulator.append('111')
        yield from next_step(self)
        self.accumulator.append('222')
        ret = yield self.async_operation()
        self.accumulator.append('router_' + ret)

    @message_receiver()
    def meth_async(self):
        ret = yield self.async_operation()
        self.accumulator.append('receiver_' + ret)

    @return_future
    def async_operation(self, callback):
        callback('async_result')


class TestReceiver(unittest.TestCase):

    def setUp(self):
        self.obj = Obj()
        self.conn = Conn(self.obj)
        self.sender = Sender(self.conn.send)

    def test_route(self):
        self.sender.meth_a.meth_a.meth_a.meth_b(b='gg', a=44)
        self.assertEqual(self.obj.received_args, (44, 'gg'))

    def test_wrong_path1(self):
        with self.assertRaisesRegexp(ReceiverError, 'expected router'):
            self.sender.meth_b.meth_a()

    def test_wrong_path2(self):
        with self.assertRaisesRegexp(ReceiverError, 'expected receiver'):
            self.sender.meth_a.meth_a()

    def test_wrong_path3(self):
        with self.assertRaisesRegexp(ReceiverError, 'has no method'):
            self.sender.meth_d()

    def test_forbidden(self):
        with self.assertRaisesRegexp(ReceiverError, 'forbidden'):
            self.sender.meth_a.meth_a.meth_c()

    def test_path_info(self):
        txt = "Path\: meth_a\.meth_a\.\|meth_b\|\.meth_b\.meth_b\.meth_a"
        with self.assertRaisesRegexp(ReceiverError, txt):
            self.sender.meth_a.meth_a.meth_b.meth_b.meth_b.meth_a()

    def test_sign1(self):
        self.conn.sign = None
        with self.assertRaisesRegexp(SignError, "Need user's sign"):
            self.sender.meth_a.meth_a()
        with self.assertRaisesRegexp(SignError, "Need user's sign"):
            self.sender.meth_b()
        self.conn.sign = USER_SIGN
        self.sender.meth_a.meth_b(a=1, b=1)

    def test_sign2(self):
        self.conn.sign = None
        self.sender.meth_z.meth_x()

    def test_sign3(self):
        with self.assertRaisesRegexp(SignError, "Need internal sign"):
            self.sender.meth_g()

    def test_sign4(self):
        self.sender.meth_y()
        self.conn.sign = None
        with self.assertRaisesRegexp(SignError,
                "Need internal or user's sign"):
            self.sender.meth_y()

    def test_async(self):
        f = self.sender.meth_u.meth_a.meth_u.meth_async
        IOLoop.instance().run_sync(f)
        self.assertEqual(['111', '111', 'receiver_async_result',
                          '222', 'router_async_result', '222',
                          'router_async_result'], self.obj.accumulator)


if __name__ == '__main__':
    unittest.main()



