from sulaco.tests.test_tools import BasicFuncTest


class TestDistributedSendToUser(BasicFuncTest):

    def runTest(self):
        self.run_server(7770, 5)
        c1 = self.client()
        c1.connect(7770)
        c1.s.sign_id(username='user1')
        uid1 = c1.recv(path_prefix='sign_id')['kwargs']['uid']
        self.assertEqual(-2878283150406289529, uid1)


        self.run_server(7773, 5)
        c2 = self.client()
        c2.connect(7773)
        c2.s.sign_id(username='user2')
        uid2 = c2.recv(path_prefix='sign_id')['kwargs']['uid']
        self.assertEqual(-2878283150406289532, uid2)

        c1.s.send_to_user(receiver=uid2, text='Foo')
        self.assertEqual({u'path': u'message_from_user',
                          u'kwargs': {u'text': u'Foo', u'uid': uid1}},
                          c2.recv(path_prefix='message_from_user'))
        c2.s.send_to_user(receiver=uid1, text='Bar')
        self.assertEqual({u'path': u'message_from_user',
                          u'kwargs': {u'text': u'Bar', u'uid': uid2}},
                          c1.recv(path_prefix='message_from_user'))

