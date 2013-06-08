from sulaco.tests.tools import BasicFuncTest


class TestLocationConnect(BasicFuncTest):

    def runTest(self):

        self.run_servers((7770, 5), (7773, 5))

        c1 = self.client()
        c1.connect(7770)
        c2 = self.client()
        c2.connect(7773)
        self.run_location('loc_X', 'tcp://127.0.0.1:8770',
                                   'tcp://127.0.0.1:8771')
        self.assertEqual({'kwargs': {'loc_id': 'loc_X'},
                          'path': 'location_added'},
                          c1.recv())

        c1.s.sign_id(username='user1')
        self.assertEqual({'users': [{'uid': '1',
                                     'username': 'user1'}],
                          'ident': 'loc_X'},
                          c1.recv(path_prefix='location.init')['kwargs'])


        c2.s.sign_id(username='user2')
        self.assertEqual({'users': [{'uid': '1',
                                     'username': 'user1'},
                                    {'uid': '2',
                                     'username': 'user2'}],
                          'ident': 'loc_X'},
                          c2.recv(path_prefix='location.init')['kwargs'])
        self.assertEqual(
            {'user': {'username': 'user1', 'uid': '1'}},
            c1.recv(path_prefix='location.user_connected')['kwargs'])

