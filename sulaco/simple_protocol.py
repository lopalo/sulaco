import json


class SimpleProtocol(object):
    _header_bytes = 10

    def __init__(self, stream):
        self._stream = stream
        stream.set_close_callback(self.on_close)

    def _on_header(self, data):
        content_size = int(data)
        self._stream.read_bytes(content_size, self._on_body)

    def _on_body(self, data):
        self._stream.read_bytes(self._header_bytes, self._on_header)
        self.on_message(json.loads(data))

    def send(self, message):
        data = json.dumps(message)
        dlen = str(len(data))
        data = (self._header_bytes - len(dlen)) * '0' + dlen + data
        self._stream.write(data, self.on_sent)

    def connect(self, address):
        self._stream.connect(address, self.on_open)

    def write_and_close(self):
        #TODO: implement
        pass

    def close(self):
        self._stream.close()

    def on_open(self, *args):
        self._stream.read_bytes(self._header_bytes, self._on_header)

    def on_sent(self):
        pass

    def on_message(self, mesage):
        pass

    def on_close(self):
        pass


