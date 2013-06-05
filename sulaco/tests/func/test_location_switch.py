from sulaco.tests.tools import BasicFuncTest


class TestLocationSwitch(BasicFuncTest):

    def runTest(self):

        self.run_servers((7770, 5), (7773, 5))

        c1 = self.client()
        c1.connect(7770)
        c2 = self.client()
        c2.connect(7773)
        c3 = self.client()
        c3.connect(7773)
        self.run_locations(
            ['loc_X', 'tcp://127.0.0.1:8770', 'tcp://127.0.0.1:8771'],
            ['loc_Y', 'tcp://127.0.0.1:8772', 'tcp://127.0.0.1:8773'])
        self.assertEqual({'kwargs': {'loc_id': 'loc_X'},
                          'path': 'location_added'},
                          c1.recv(kwargs_contain={'loc_id': 'loc_X'}))
        self.assertEqual({'kwargs': {'loc_id': 'loc_Y'},
                          'path': 'location_added'},
                          c1.recv(kwargs_contain={'loc_id': 'loc_Y'}))

        c1.s.sign_id(username='user1', loc='loc_X')
        c1.recv(path_prefix='location.init')
        c1.recv(path_prefix='location.user_connected')
        c2.s.sign_id(username='user2', loc='loc_Y')
        c2.recv(path_prefix='location.init')
        c2.recv(path_prefix='location.user_connected')

        c3.s.sign_id(username='user3', loc='loc_X')
        self.assertEqual('loc_X', c3.recv(path_prefix='location.init')
                                                    ['kwargs']['ident'])

        self.assertEqual(
            {'user': {'username': 'user3', 'uid': 3}},
            c1.recv(path_prefix='location.user_connected')['kwargs'])

        c3.s.location.move_to(target_location='loc_Y')

        self.assertEqual(
            {'uid': 3},
            c1.recv(path_prefix='location.user_disconnected')['kwargs'])
        self.assertEqual(
            {'user': {'uid': 3, 'username': 'user3'}},
            c2.recv(path_prefix='location.user_connected')['kwargs'])
        self.assertEqual({'ident': 'loc_Y',
                          'users': [{'username': 'user2',
                                     'uid': 2},
                                    {'username': 'user3',
                                     'uid': 3}]},
                           c3.recv(path_prefix='location.init')['kwargs'])

