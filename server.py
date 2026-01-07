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
    def __init__(self):
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict
        }

    # Analyzes a request from the client into its component parts
    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()
        
        try:
            # Delegates to the appropriate handler based on the first byte
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('Bad Request')
        
    # For each handler

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip('\r\n')
    
    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip('\r\n'))
    
    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip('\r\n'))
    
    def handle_string(self, socket_file):
        # First reads the length
        length = int(socket_file.readline().rstrip('\r\n'))
        # NULL
        if length == -1:
            return None
        length += 2 # Includes the '\r\n' in count
        return socket_file.read(length).rstrip[:-2]
    
    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    # Serealizes the response data and sends it to the client
    def write_response(self, socket_file, data):
        buf = BytesIO()
        self.write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def write(self, buf, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buf.write('$%s\n%s\r\n' % (len(data), data))
        elif isinstance(data, int):
            buf.write(':%s\r\n' % data)
        elif isinstance(data, Error):
            buf.write('-%s\r\n' % error.message)
        elif isinstance(data, (list, tuple)):
            buf.write('*%s\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write('%%%s\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])

        elif data is None:
            buf.write('$-1\r\n')
        else:
            raise CommandError('Unrecognized type: %s' % type(data))

class Server(object):
    # Localhost only
    def __init__(self, host = '127.0.0.1', port = 31337, max_clients = 64):
        self._pool = Pool(max_clients) # Prevents memory exhaustion
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn = self._pool
        )

        self._protocal = ProtocolHandler()
        self._kv = {}

        self._commands = self.get_commands()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset
        }

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

    # Unpacks the data sent by the client,
    # excecutes the command they specified, 
    # and passes back the return value
    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be a list or simple string')
        
        if not data:
            raise CommandError('Missing command')
        
        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
        
        return self._commands[command](*data[1:])

    def run(self):
        self._server.serve_forever()