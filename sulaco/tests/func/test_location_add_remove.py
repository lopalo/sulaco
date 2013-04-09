from sulaco.tests.test_tools import BasicFuncTest


class TestLocationAddRemove(BasicFuncTest):

    def runTest(self):
        self.run_servers((7770, 5), (7773, 5))

        c1 = self.client()
        c1.connect(7770)

        c2 = self.client()
        c2.connect(7773)
        self.run_location('loc_1', 'tcp://127.0.0.1:8770',
                                   'tcp://127.0.0.1:8771')

        self.assertEqual({u'kwargs': {u'loc_id': u'loc_1'},
                          u'path': u'location_added'},
                          c1.recv())
        self.assertEqual({u'kwargs': {u'loc_id': u'loc_1'},
                          u'path': u'location_added'},
                          c2.recv())

        self.shutdown_location('loc_1')

        self.assertEqual({u'kwargs': {u'loc_id': u'loc_1'},
                          u'path': u'location_removed'},
                          c1.recv())
        self.assertEqual({u'kwargs': {u'loc_id': u'loc_1'},
                          u'path': u'location_removed'},
                          c2.recv())
