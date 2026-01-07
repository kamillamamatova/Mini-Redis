# This skeleton code is meant to
# accept many client connections at once,
# read structured commands from clients,
# excecute some command logic,
# send structured responses back,
# handle errors cleanly,
# and dicsonnect clients gracefully

# Just a framework so far

from gevent import socket
from gevent.pool import Pool # Limits how many clients can be handled at once
from gevent.server import StreamServer # TCP server that works with gevent

from collections import namedtuple # Lightweight data structure
from io import BytesIO # Treats bytes like a file
from socket import error as socket_error # Catches low level socket errors

# To notify the connection handling loop of problems
class CommandError(Exception):
    pass
class Disconnect(Exception):
    pass

Error = namedtuple('Error', ('message',))

# Defines how data moves across the wire
class ProtocolHandler(object):
    def handle_request(self, socket_file):
        # Analyzes a request from the client into its component parts
        pass

    def write_response(self, socket_file, data):
        # Serealizes the response data and sends it to the client
        pass

class Server(object):
    # Localhost only
    def __init__(self, host = '127.0.0.1', port = 31337, max_clients = 64):
        self._pool = Pool(max_clients) # Prevents memory exhaustion
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn = self._pool
        )

        self._protocal = ProtocalHandler()
        self._kv = {}

    # Runs once per client connection
    def connection_handler(self, conn, address):
        # Converts "conn", which is a socket object, into a file like object
        socket_file = conn.makefile('rwb')

        # Processes client requests until the client disconnects
        while True:
            try:
                data = self._protocal.handle_request(socket_file)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

            self._protocal.write_response(socket_file, resp)

    def get_response(self, data):
        # Unpacks the data sent by the client,
        # excecutes the command they specified, 
        # and passes back the return value
        pass

    def run(self):
        self._server.serve_forever()