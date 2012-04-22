import os
import re
import stat
import time
import sys
import socket
import errno
import pwd
import urllib
import pika


try:
    from hashlib import sha1
except ImportError:
    from sha import new as sha1

from supervisor.medusa import asyncore_25 as asyncore
from supervisor.medusa import asynchat_25 as asynchat
from supervisor.medusa import http_date
from supervisor.medusa import http_server
from supervisor.medusa import producers
from supervisor.medusa import filesys
from supervisor.medusa import default_handler
from supervisor.medusa.counter import counter
from supervisor.medusa import logger
from pika.adapters import AsyncoreConnection #, AsyncoreDispatcher
from pika.connection import ConnectionParameters
from pika.credentials import PlainCredentials

from urllib import unquote, splitquery
from supervisor.medusa.auth_handler import auth_handler
class NOT_DONE_YET:
    pass

class deferring_chunked_producer:
    """A producer that implements the 'chunked' transfer coding for HTTP/1.1.
    Here is a sample usage:
            request['Transfer-Encoding'] = 'chunked'
            request.push (
                    producers.chunked_producer (your_producer)
                    )
            request.done()
    """

    def __init__ (self, producer, footers=None):
        self.producer = producer
        self.footers = footers
        self.delay = 0.1

    def more (self):
        if self.producer:
            data = self.producer.more()
            if data is NOT_DONE_YET:
                return NOT_DONE_YET
            elif data:
                return '%x\r\n%s\r\n' % (len(data), data)
            else:
                self.producer = None
                if self.footers:
                    return '\r\n'.join(['0'] + self.footers) + '\r\n\r\n'
                else:
                    return '0\r\n\r\n'
        else:
            return ''

class deferring_composite_producer:
    "combine a fifo of producers into one"
    def __init__ (self, producers):
        self.producers = producers
        self.delay = 0.1

    def more (self):
        while len(self.producers):
            p = self.producers[0]
            d = p.more()
            if d is NOT_DONE_YET:
                return NOT_DONE_YET
            if d:
                return d
            else:
                self.producers.pop(0)
        else:
            return ''


class deferring_globbing_producer:
    """
    'glob' the output from a producer into a particular buffer size.
    helps reduce the number of calls to send().  [this appears to
    gain about 30% performance on requests to a single channel]
    """

    def __init__ (self, producer, buffer_size=1<<16):
        self.producer = producer
        self.buffer = ''
        self.buffer_size = buffer_size
        self.delay = 0.1

    def more (self):
        while len(self.buffer) < self.buffer_size:
            data = self.producer.more()
            if data is NOT_DONE_YET:
                return NOT_DONE_YET
            if data:
                self.buffer = self.buffer + data
            else:
                break
        r = self.buffer
        self.buffer = ''
        return r


class deferring_hooked_producer:
    """
    A producer that will call <function> when it empties,.
    with an argument of the number of bytes produced.  Useful
    for logging/instrumentation purposes.
    """

    def __init__ (self, producer, function):
        self.producer = producer
        self.function = function
        self.bytes = 0
        self.delay = 0.1

    def more (self):
        if self.producer:
            result = self.producer.more()
            if result is NOT_DONE_YET:
                return NOT_DONE_YET
            if not result:
                self.producer = None
                self.function (self.bytes)
            else:
                self.bytes = self.bytes + len(result)
            return result
        else:
            return ''


class deferring_amqp_request(http_server.http_request):
    """ The medusa http_request class uses the default set of producers in
    medusa.prodcers.  We can't use these because they don't know anything about
    deferred responses, so we override various methods here.  This was added
    to support tail -f like behavior on the logtail handler """

    def __init__(self, channel, body, command, uri, version, header, props):
        http_server.http_request.__init__(self, channel, body, command, uri, version, header)
        self.props = props

    def get_header(self, header):
        # this is overridden purely for speed (the base class doesn't
        # use string methods
        header = header.lower()
        hc = self._header_cache
        if not hc.has_key(header):
            h = header + ': '
            for line in self.header:
                if line.lower().startswith(h):
                    hl = len(h)
                    r = line[hl:]
                    hc[header] = r
                    return r
            hc[header] = None
            return None
        else:
            return hc[header]

    def done(self, *arg, **kw):

        """ I didn't want to override this, but there's no way around
        it in order to support deferreds - CM

        finalize this transaction - send output to the http channel"""

        # ----------------------------------------
        # persistent connection management
        # ----------------------------------------

        #  --- BUCKLE UP! ----

        connection = http_server.get_header(http_server.CONNECTION,self.header)
        connection = connection.lower()

        close_it = 0
        wrap_in_chunking = 0
        globbing = 1

        if self.version == '1.0':
            if connection == 'keep-alive':
                if not self.has_key ('Content-Length'):
                    close_it = 1
                else:
                    self['Connection'] = 'Keep-Alive'
            else:
                close_it = 1
        elif self.version == '1.1':
            if connection == 'close':
                close_it = 1
            elif not self.has_key('Content-Length'):
                if self.has_key('Transfer-Encoding'):
                    if not self['Transfer-Encoding'] == 'chunked':
                        close_it = 1
                elif self.use_chunked:
                    self['Transfer-Encoding'] = 'chunked'
                    wrap_in_chunking = 1
                    # globbing slows down tail -f output, so only use it if
                    # we're not in chunked mode
                    globbing = 0
                else:
                    close_it = 1
        elif self.version is None:
            # Although we don't *really* support http/0.9 (because
            # we'd have to use \r\n as a terminator, and it would just
            # yuck up a lot of stuff) it's very common for developers
            # to not want to type a version number when using telnet
            # to debug a server.
            close_it = 1

        outgoing_header = producers.simple_producer(self.build_reply_header())

        if close_it:
            self['Connection'] = 'close'

        if wrap_in_chunking:
            outgoing_producer = deferring_chunked_producer(
                    deferring_composite_producer(self.outgoing)
                    )
            # prepend the header
            outgoing_producer = deferring_composite_producer(
                [outgoing_header, outgoing_producer]
                )
        else:
            # prepend the header
            self.outgoing.insert(0, outgoing_header)
            outgoing_producer = deferring_composite_producer(self.outgoing)

        # hook logging into the output
        outgoing_producer = deferring_hooked_producer(outgoing_producer,
                                                      self.log)

        if globbing:
            outgoing_producer = deferring_globbing_producer(outgoing_producer)

        self.channel.push_with_producer(outgoing_producer, props=self.props)

        self.channel.current_request = None

        if close_it:
            self.channel.close_when_done()

    def log (self, bytes):
        """ We need to override this because UNIX domain sockets return
        an empty string for the addr rather than a (host, port) combination """
        if self.channel.addr:
            host = self.channel.addr[0]
            port = self.channel.addr[1]
        else:
            host = 'localhost'
            port = 0
        self.channel.server.logger.log (
                host,
                '%d - - [%s] "%s" %d %d\n' % (
                        port,
                        self.log_date_string (time.time()),
                        self.request,
                        self.reply_code,
                        bytes
                        )
                )

    def cgi_environment(self):
        env = {}

        # maps request some headers to environment variables.
        # (those that don't start with 'HTTP_')
        header2env= {'content-length'    : 'CONTENT_LENGTH',
                     'content-type'      : 'CONTENT_TYPE',
                     'connection'        : 'CONNECTION_TYPE'} 

        workdir = os.getcwd()
        (path, params, query, fragment) = self.split_uri()

        if params:
            path = path + params # undo medusa bug!

        while path and path[0] == '/':
            path = path[1:]
        if '%' in path:
            path = http_server.unquote(path)
        if query:
            query = query[1:]

        server = self.channel.server
        env['REQUEST_METHOD'] = self.command.upper()
        env['SERVER_PORT'] = str(server.port)
        env['SERVER_NAME'] = server.server_name
        env['SERVER_SOFTWARE'] = server.SERVER_IDENT
        env['SERVER_PROTOCOL'] = "HTTP/" + self.version
        env['channel.creation_time'] = self.channel.creation_time
        env['SCRIPT_NAME'] = ''
        env['PATH_INFO'] = '/' + path
        env['PATH_TRANSLATED'] = os.path.normpath(os.path.join(
                workdir, env['PATH_INFO']))
        if query:
            env['QUERY_STRING'] = query
        env['GATEWAY_INTERFACE'] = 'CGI/1.1'
        if self.channel.addr:
            env['REMOTE_ADDR'] = self.channel.addr[0]
        else:
            env['REMOTE_ADDR'] = '127.0.0.1'

        for header in self.header:
            key,value=header.split(":",1)
            key=key.lower()
            value=value.strip()
            if header2env.has_key(key) and value:
                env[header2env.get(key)]=value
            else:
                key='HTTP_%s' % ("_".join(key.split( "-"))).upper()
                if value and not env.has_key(key):
                    env[key]=value
        return env

    def get_server_url(self):
        """ Functionality that medusa's http request doesn't have; set an
        attribute named 'server_url' on the request based on the Host: header
        """
        default_port={'http': '80', 'https': '443'}
        environ = self.cgi_environment()
        if (environ.get('HTTPS') in ('on', 'ON') or
            environ.get('SERVER_PORT_SECURE') == "1"):
            # XXX this will currently never be true
            protocol = 'https'
        else:
            protocol = 'http'

        if environ.has_key('HTTP_HOST'):
            host = environ['HTTP_HOST'].strip()
            hostname, port = urllib.splitport(host)
        else:
            hostname = environ['SERVER_NAME'].strip()
            port = environ['SERVER_PORT']

        if (port is None or default_port[protocol] == port):
            host = hostname
        else:
            host = hostname + ':' + port
        server_url = '%s://%s' % (protocol, host)
        if server_url[-1:]=='/':
            server_url=server_url[:-1]
        return server_url

# ===========================================================================
#                                                AMQP Server Object
# ===========================================================================

class supervisor_amqp_server(AsyncoreConnection):
    ip = None

    def __init__(self, parameters, queue, logger_object):        
        
        AsyncoreConnection.__init__(self, 
            parameters, 
            on_open_callback=self._on_connect)
        
        self.handlers = []
        self.server = self
        self.parameters = parameters
        self.logger = logger.unresolving_logger (logger_object)
        self.queue = queue
        current_request = None
        self.addr = parameters.host, parameters.port

        from supervisor.medusa.counter import counter
        self.channel_counter = counter()
        self.request_counter = counter()
        self.total_clients = counter()
        self.total_requests = counter()
        self.exceptions = counter()
        self.bytes_out = counter()
        self.bytes_in  = counter()

        self.log_info (
                'Medusa AMQP started at %s'
                '\n\tHostname: %s'
                '\n\tPort:%s'
                '\n' % (
                        time.ctime(time.time()),
                        self.parameters.host,
                        self.parameters.port,
                        )
                )

    def log_info(self, message, type='info'):
        ip = ''
        if getattr(self, 'ip', None) is not None:
            ip = self.ip
        self.logger.log(ip, message)

    def _on_connect(self, connection):
        self.log_info (
                'Medusa AMQP connected at %s' %
                        time.ctime(time.time()))

        self.connection = connection.channel(self._on_channel_open)
    
    def _on_channel_open(self, channel):
        self.channel = channel
        self.log_info (
                'Medusa AMQP channel opened at %s' % 
                        time.ctime(time.time()))

        self.channel.queue_declare(queue=self.queue,
                          durable=True,
                          exclusive=False,
                          auto_delete=False,
                          callback=self._on_queue_declared)

 
    def _on_queue_declared(self, frame):
        self.log_info (
                'Medusa AMQP queue %s opened at %s' % (self.queue, time.ctime(time.time())))
        self.channel.basic_consume(self.handle_delivery, queue=self.queue)


    def handle_delivery(self, channel, method_frame, header_frame, body):
        self.total_clients.increment()
        self.channel = channel
        self.log_info (
                'Medusa AMQP handling_delivery of %s' % header_frame)
        self.props = header_frame

        self.request_counter.increment()
        self.server.total_requests.increment()
        self.server.channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        http_head, http_headers = body.splitlines()[0], body.splitlines()[1:] 
        
        command, uri, version = crack_request(http_head)
        self.log_info ('HTTP %s request: %s %s' % (version, command, uri))          

        if command is None:
            self.log_info ('Bad HTTP request: %s' % repr(request), 'error')
            r.error (400)
            return
        
        header = join_headers (http_headers)
        # unquote path if necessary (thanks to Skip Montanaro for pointing
        # out that we must unquote in piecemeal fashion).
        rpath, rquery = splitquery(uri)
        if '%' in rpath:
            if rquery:
                uri = unquote (rpath) + '?' + rquery
            else:
                uri = unquote (rpath)


        # --------------------------------------------------
        # handler selection and dispatch
        # --------------------------------------------------
        r = deferring_amqp_request (self, body, command, uri, version, header, header_frame)

        for h in self.handlers:
            if h.match (r):
                try:
                    self.current_request = r
                    # This isn't used anywhere.
                    # r.handler = h # CYCLE
                    h.handle_request (r)
                    h.continue_request(body.split('\t')[-1], r)
                    
                except:
                    self.server.exceptions.increment()
                    (file, fun, line), t, v, tbinfo = asyncore.compact_traceback()
                    try:
                        r.error (500)
                    except:
                        pass
                return
        

    def install_handler (self, handler, back=0):
        if back:
            self.handlers.append (handler)
        else:
            self.handlers.insert (0, handler)

    def remove_handler (self, handler):
        self.handlers.remove (handler)

    def push_with_producer(self, producer, props = None):
        if not props:
            props = self.props
        msg = producer.more()

        if not type(msg) == type(''):
            return NOT_DONE_YET
        
        self.channel.basic_publish(exchange='',
                    routing_key=props.reply_to,
                    properties=pika.BasicProperties(correlation_id = \
                    props.correlation_id),
                    body=msg)
        
    def log (self, bytes):
        pass

    def set_terminator(self, thing):
        pass
  
    def handle_connect(self):
        self.log_info (
                'handle_connect' % (time.ctime(time.time())))





def join_headers (headers):
    r = []
    for i in range(len(headers)):
        
        try:
            if headers[i][0] in ' \t':
                r[-1] = r[-1] + headers[i][1:]
            else:
                r.append (headers[i])
        except:
            pass
    return r

def get_header (head_reg, lines, group=1):
    for line in lines:
        m = head_reg.match (line)
        if m and m.end() == len(line):
            return m.group (group)
    return ''

def get_header_match (head_reg, lines):
    for line in lines:
        m = head_reg.match (line)
        if m and m.end() == len(line):
            return m
    return ''


REQUEST = re.compile ('([^ ]+) ([^ ]+)(( HTTP/([0-9.]+))$|$)')

def crack_request (r):
    m = REQUEST.match (r)
    if m and m.end() == len(r):
        if m.group(3):
            version = m.group(5)
        else:
            version = None
        return m.group(1), m.group(2), version
    else:
        return None, None, None  




def make_amqp_servers(options, supervisord):
    brokers = []
    class LogWrapper:
        def log(self, msg):
            if msg.endswith('\n'):
                msg = msg[:-1]
            options.logger.trace(msg)
    wrapper = LogWrapper()
    for config in options.broker_configs:
        host, port, user, password = config['host'], config['port'], config['username'], config['password']


        user_credentials = PlainCredentials(user, password)
        connection_params = ConnectionParameters(host=host, port=port, credentials=user_credentials)
        ampq_connection = supervisor_amqp_server(parameters = connection_params, queue='test', logger_object=wrapper)
        asyncore.socket_map.update({ampq_connection.socket.fileno(): ampq_connection.ioloop})
    

        from xmlrpc import supervisor_xmlrpc_handler
        from xmlrpc import SystemNamespaceRPCInterface
        # from web import supervisor_ui_handler

        subinterfaces = []
        for name, factory, d in options.rpcinterface_factories:
             try:
                 inst = factory(supervisord, **d)
             except:
                 import traceback; traceback.print_exc()
                 raise ValueError('Could not make %s rpc interface' % name)
             subinterfaces.append((name, inst))
             options.logger.info('RPC interface %r initialized' % name)


        subinterfaces.append(('system',
                               SystemNamespaceRPCInterface(subinterfaces)))
        xmlrpchandler = supervisor_xmlrpc_handler(supervisord, subinterfaces)
        # tailhandler = logtail_handler(supervisord)
        # maintailhandler = mainlogtail_handler(supervisord)
        # uihandler = supervisor_ui_handler(supervisord)
        # here = os.path.abspath(os.path.dirname(__file__))
        # templatedir = os.path.join(here, 'ui')
        # filesystem = filesys.os_filesystem(templatedir)
        # defaulthandler = default_handler.default_handler(filesystem)

        # username = config['username']
        # password = config['password']

        # if username:
        #     # wrap the xmlrpc handler and tailhandler in an authentication
        #     # handler
        #     users = {username:password}
        #     xmlrpchandler = supervisor_auth_handler(users, xmlrpchandler)
        #     tailhandler = supervisor_auth_handler(users, tailhandler)
        #     maintailhandler = supervisor_auth_handler(users, maintailhandler)
        #     uihandler = supervisor_auth_handler(users, uihandler)
        #     defaulthandler = supervisor_auth_handler(users, defaulthandler)
        # else:
        #     options.logger.critical(
        #         'Server %r running without any HTTP '
        #         'authentication checking' % config['section'])
        # # defaulthandler must be consulted last as its match method matches
        # # everything, so it's first here (indicating last checked)
        # hs.install_handler(defaulthandler)
        # hs.install_handler(uihandler)
        # hs.install_handler(maintailhandler)
        # hs.install_handler(tailhandler)
        ampq_connection.install_handler(xmlrpchandler) # last for speed (first checked)
        brokers.append((config, ampq_connection.ioloop))

    return brokers
