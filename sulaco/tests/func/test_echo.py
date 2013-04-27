from sulaco.tests.tools import BasicFuncTest


class TestEcho(BasicFuncTest):

    def runTest(self):
        self.run_server(7770, 1)
        c = self.client()
        c.connect(7770)
        c.s.echo(text='test message 1')
        self.assertEqual({'kwargs': {'text': 'Echo: test message 1'},
                          'path': 'echo'}, c.recv())
        c.s.echo(text='test message 2')
        self.assertEqual({'kwargs': {'text': 'Echo: test message 2'},
                          'path': 'echo'}, c.recv())


