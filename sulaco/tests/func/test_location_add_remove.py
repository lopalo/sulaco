from sulaco.tests.tools import BasicFuncTest


class TestLocationAddRemove(BasicFuncTest):

    def runTest(self):
        self.run_servers((7770, 5), (7773, 5))

        c1 = self.client()
        c1.connect(7770)
        c1.s.get_locations()
        self.assertEqual({'data':[]},
                c1.recv(path_prefix='locations')['kwargs'])

        c2 = self.client()
        c2.connect(7773)
        self.run_location('loc_1', 'tcp://127.0.0.1:8770',
                                   'tcp://127.0.0.1:8771')

        self.assertEqual({'kwargs': {'loc_id': 'loc_1'},
                          'path': 'location_added'},
                          c1.recv())
        self.assertEqual({'kwargs': {'loc_id': 'loc_1'},
                          'path': 'location_added'},
                          c2.recv())
        c1.s.get_locations()
        self.assertEqual({'data': [{'ident': 'loc_1'}]},
                c1.recv(path_prefix='locations')['kwargs'])

        self.shutdown_location('loc_1')

        self.assertEqual({'kwargs': {'loc_id': 'loc_1'},
                          'path': 'location_removed'},
                          c1.recv())
        self.assertEqual({'kwargs': {'loc_id': 'loc_1'},
                          'path': 'location_removed'},
                          c2.recv())
        c1.s.get_locations()
        self.assertEqual({'data':[]},
                c1.recv(path_prefix='locations')['kwargs'])
