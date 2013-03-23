from sulaco.tests.test_tools import BasicFuncTest


class TestEcho(BasicFuncTest):

    def runTest(self):
        self.run_server(7770, 1)
        c = self.client()
        c.connect(7770)
        c.s.echo(text='test message 1')
        self.assertEqual({u'kwargs': {u'text': u'Echo: test message 1'},
                          u'path': u'echo'}, c.recv())
        c.s.echo(text='test message 2')
        self.assertEqual({u'kwargs': {u'text': u'Echo: test message 2'},
                          u'path': u'echo'}, c.recv())


