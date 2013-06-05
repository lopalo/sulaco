from sulaco.tests.tools import BasicFuncTest


class TestLocationAddExisting(BasicFuncTest):

    def runTest(self):
        self.run_locations(
            ['loc_X', 'tcp://127.0.0.1:8770', 'tcp://127.0.0.1:8771'],
            ['loc_Y', 'tcp://127.0.0.1:8772', 'tcp://127.0.0.1:8773'])

        self.run_server(7770, 5)

        c = self.client()
        c.connect(7770)
        c.s.get_locations()
        ret = sorted(c.recv(path_prefix='locations')['kwargs']['data'],
                                                key=lambda i: i['ident'])
        self.assertEqual([{'ident': 'loc_X'}, {'ident': 'loc_Y'}], ret)
