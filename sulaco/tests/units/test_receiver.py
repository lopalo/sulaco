import unittest
from sulaco.utils import Sender
from sulaco.utils.receiver import (
    root_dispatch, message_router, message_receiver, Task,
    ReceiverError, SignError, USER_SIGN, INTERNAL_SIGN,
    INTERNAL_USER_SIGN)


class Conn(object):

    def __init__(self, root):
        self._root = root
        self.sign = USER_SIGN

    def send(self, msg):
        self.on_message(msg)

    def on_message(self, msg):
        path = msg['path'].split('.')
        root_dispatch(self._root, path, msg['kwargs'], self.sign)


class Obj(object):

    @message_router(USER_SIGN)
    def meth_a(self, next_step, **kwargs):
        next_step(self)

    @message_receiver(USER_SIGN)
    def meth_b(self, a, b):
        self.received_args = (a, b)

    def meth_c(self):
        pass

    @message_router()
    def meth_z(self, next_step):
        next_step(self)

    @message_receiver()
    def meth_x(self):
        pass

    @message_receiver(INTERNAL_SIGN)
    def meth_g(self):
        pass

    @message_receiver(INTERNAL_USER_SIGN)
    def meth_y(self):
        pass


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


class AsyncObj(object):

    def __init__(self, testcase):
        self.marks = []
        self._counter = 0
        self._obj_marks = {}
        self.testcase = testcase

    def add_mark(self, mark, obj):
        if not obj in self._obj_marks:
            c = self._counter
            self._obj_marks[obj] = c
            self._counter += 1
        mark = '{}_{}'.format(mark, self._obj_marks[obj])
        self.marks.append(mark)

    def async_operation(self, callback):
        self.add_mark('async_operation', object())
        callback()

    @message_router(USER_SIGN)
    def meth_a(self, next_step, **kwargs):
        o = object()
        self.add_mark('meth_a_enter', o)
        ok = yield Task(next_step, self)
        self.testcase.assertTrue(ok)
        self.add_mark('meth_a_exit', o)

    @message_receiver(USER_SIGN)
    def meth_b(self, a, b):
        o = object()
        self.add_mark('meth_b_enter', o)
        yield Task(self.async_operation)
        self.received_args = (a, b)
        self.add_mark('meth_b_exit', o)

    @message_router(USER_SIGN)
    def meth_c(self, next_step, **kwargs):
        self.add_mark('meth_c', object())
        next_step(self)

    @message_receiver(USER_SIGN)
    def meth_d(self, **kwargs):
        self.add_mark('meth_d', object())

    @message_router(USER_SIGN)
    def meth_e(self, next_step, **kwargs):
        ok = yield Task(next_step, self)
        self.testcase.assertFalse(ok)

    @message_receiver(USER_SIGN)
    def meth_f(self, **kwargs):
        yield Task(self.async_operation)
        raise Exception()

class TestAsyncReceiver(unittest.TestCase):

    def setUp(self):
        self.obj = AsyncObj(self)
        self.conn = Conn(self.obj)
        self.sender = Sender(self.conn.send)

    def test_route(self):
        self.sender.meth_a.meth_a.meth_a.meth_b(b='gg', a=44)
        self.assertEqual(self.obj.received_args, (44, 'gg'))
        self.assertEqual(['meth_a_enter_0',
                          'meth_a_enter_1',
                          'meth_a_enter_2',
                          'meth_b_enter_3',
                          'async_operation_4',
                          'meth_b_exit_3',
                          'meth_a_exit_2',
                          'meth_a_exit_1',
                          'meth_a_exit_0'], self.obj.marks)

    def test_with_sync_router(self):
        self.sender.meth_a.meth_c.meth_b(b='gg', a=22)
        self.assertEqual(['meth_a_enter_0',
                          'meth_c_1',
                          'meth_b_enter_2',
                          'async_operation_3',
                          'meth_b_exit_2',
                          'meth_a_exit_0'], self.obj.marks)

    def test_with_sync_receiver(self):
        self.sender.meth_a.meth_d(b='gg', a=22)
        self.assertEqual(['meth_a_enter_0',
                          'meth_d_1',
                          'meth_a_exit_0'], self.obj.marks)

    def test_router_exception(self):
        with self.assertRaises(Exception):
            self.sender.meth_e.meth_f()



if __name__ == '__main__':
    unittest.main()



