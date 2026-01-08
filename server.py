# This skeleton code is meant to
# accept many client connections at once,
# read structured commands from clients,
# excecute some command logic,
# send structured responses back,
# handle errors cleanly,
# and dicsonnect clients gracefully

from gevent import socket # gevent is a 3rd party python library
from gevent.pool import Pool # Limits how many clients can be handled at once
from gevent.server import StreamServer # TCP server that works with gevent

from collections import namedtuple # Lightweight data structure
from io import BytesIO # Treats bytes like a file
import logging

logger = logging.getLogger(__name__)

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
        
        #first_byte is bytes in Python 3
        first_byte = first_byte.decode("utf-8", errors = "strict")
        
        try:
            # Delegates to the appropriate handler based on the first byte
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('Bad Request')
        
    # For each handler

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b"\r\n").decode("utf-8", errors = "strict")
    
    def handle_error(self, socket_file):
        msg = socket_file.readline().rstrip(b"\r\n").decode("utf-8", errors = "strict")
        return Error(msg)
    
    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip(b"\r\n"))
    
    def handle_string(self, socket_file):
        # First reads the length
        length = int(socket_file.readline().rstrip(b"\r\n"))

        # NULL
        if length == -1:
            return None
        
        length += 2 # Includes the '\r\n' in count
        data = socket_file.read(length)[:-2]
        return data.decode("utf-8", errors = "strict")
    
    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b"\r\n"))
        return [self.handle_request(socket_file) for _ in range(num_elements)]
        #return [self.handle_request(socket_file) for _ in range(num_items)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b"\r\n"))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
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
            data = data.encode("utf-8")

        if isinstance(data, bytes):
            buf.write(b"$%d\r\n" % len(data))
            buf.write(data + b"\r\n")

        elif isinstance(data, int):
            buf.write(b":%d\r\n" % data)

        elif isinstance(data, Error):
            msg = data.message
            if isinstance(msg, str):
                msg = msg.encode("utf-8")
            buf.write(b"-" + msg + b"\r\n")

        elif isinstance(data, (list, tuple)):
            buf.write(b"*%d\r\n" % len(data))
            for item in data:
                self.write(buf, item)

        elif isinstance(data, dict):
            buf.write(b"%%%d\r\n" % len(data))
            for key, value in data.items():
                self.write(buf, key)
                self.write(buf, value)

        elif data is None:
            buf.write(b"$-1\r\n")

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

        self._protocol = ProtocolHandler()
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
        logger.info("Connection received: %s:%s", address[0], address[1])
        # Converts "conn", which is a socket object, into a file like object
        socket_file = conn.makefile('rwb')

        # Processes client requests until the client disconnects
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                logger.info("Client disconnected: %s:%s", address[0], address[1])
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                logger.exception('Command error')
                resp = Error(exc.args[0])

            self._protocol.write_response(socket_file, resp)

    # Unpacks the data sent by the client,
    # excecutes the command they specified, 
    # and passes back the return value
    def get_response(self, data):
        # If client sent a simple string, split it into tokens
        # If client sent an array, it should already be a list
        if not isinstance(data, list):
            try:
                if isinstance(data, bytes):
                    data = data.decode("utf-8", errors = "strict")
                data = data.split()
            except:
                raise CommandError("Request must be a list or simple string")
        
        if not data:
            raise CommandError("Missing command")
        
        cmd = data[0]

        if isinstance(cmd, bytes):
            cmd = cmd.decode("utf-8", errors = "strict")
        command = cmd.upper()

        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
        
        logger.debug('Received %s', command)

        args = []
        for x in data[1:]:
            if isinstance(x, bytes):
                args.append(x.decode("utf-8", errors = "strict"))
            else:
                args.append(x)
        
        return self._commands[command](*args)

    def run(self):
        self._server.serve_forever()

    def get(self, key):
        return self._kv.get(key)
    
    def set(self, key, value):
        self._kv[key] = value
        return 1
    
    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0
    
    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen
    
    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]
    
    def mset(self, *items):
        if len(items) % 2 != 0:
            raise CommandError("MSET requires an even number of arguments")

        pairs = list(zip(items[::2], items[1::2]))
        for key, value in pairs:
            self._kv[key] = value
        return len(pairs)

class Client(object):
    def __init__(self, host = '127.0.0.1', port = 31337):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        self._protocol.write_response(self._fh, list(args))
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp
    
    def get(self, key):
        return self.execute('GET', key)
    
    def set(self, key, value):
        return self.execute('SET', key, value)
    
    def delete(self, key):
        return self.execute('DELETE', key)
    
    def flush(self):
        return self.execute('FLUSH')
    
    def mget(self, *keys):
        return self.execute('MGET', *keys)
    
    def mset(self, *items):
        return self.execute('MSET', *items)
    
if __name__ == '__main__':
    from gevent import monkey
    
    monkey.patch_all()

    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    Server().run()