import unittest
from sulaco.utils.receiver import (
    Sender, dispatch, message_router, message_receiver,
    ReceiverError)

class Conn(object):

    def __init__(self, root):
        self._root = root

    def send(self, msg):
        self.on_message(msg)

    def on_message(self, msg):
        path = msg.pop('path').split('.')
        dispatch(self._root, 0, path, msg)


class Obj(object):

    @message_router
    def meth_a(self, **kwargs):
        return self

    @message_receiver
    def meth_b(self, a, b):
        self.received_args = (a, b)

    def meth_c(self):
        pass


class TestReceiver(unittest.TestCase):

    def setUp(self):
        self.obj = Obj()
        self.sender = Sender(Conn(self.obj))

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



if __name__ == '__main__':
    unittest.main()
