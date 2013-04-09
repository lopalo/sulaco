from sulaco.tests.tools import BasicFuncTest


class TestErrorMessages(BasicFuncTest):

    def runTest(self):
        self.run_server(7770, 1)
        c1 = self.client()
        c1.connect(7770)
        c1.s.method_signed()
        self.assertEqual({u'kwargs': {u'msg': u'sign_error'},
                          u'path': u'error'}, c1.recv())
        c2 = self.client()
        c2.connect(7770)
        self.assertEqual({u'kwargs': {u'msg': u'max_connections_error'},
                          u'path': u'error'}, c2.recv())
        with self.assertRaisesRegexp(IOError, 'Stream is closed'):
            c2.s.echo()



