from sulaco.tests.tools import BasicFuncTest


class TestChannel(BasicFuncTest):

    def runTest(self):
        self.run_server(7770, 5)

        c1 = self.client()
        c1.connect(7770)
        c1.s.channels.subscribe(channel='foo_channel')

        c2 = self.client()
        c2.connect(7770)
        c2.s.channels.subscribe(channel='foo_channel')

        c3 = self.client()
        c3.connect(7770)
        c3.s.channels.subscribe(channel='foo_channel')


        c1.s.channels.publish(channel='foo_channel', text='hello')

        self.assertEqual({'path': 'message_from_channel',
                          'kwargs': {'text': 'hello',
                                     'channel': 'foo_channel'}},
                          c2.recv())
        self.assertEqual({'path': 'message_from_channel',
                          'kwargs': {'text': 'hello',
                                     'channel': 'foo_channel'}},
                          c3.recv())
        self.assertEqual({'path': 'message_from_channel',
                          'kwargs': {'text': 'hello',
                                     'channel': 'foo_channel'}},
                          c1.recv())


