from datetime import timedelta
import itertools

from cocaine.exceptions import (
    ServiceConnectionError,
    DisconnectionError,
    ServiceError,
)
from cocaine.logger import Logger
from cocaine.services import Service
import msgpack
from tornado import ioloop
from tornado.concurrent import Future
from tornado.gen import coroutine, Return


class ReconnectableService(object):

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 10053
    DEFAULT_ADDRESS = '{host}:{port}'.format(host=DEFAULT_HOST,
                                             port=DEFAULT_PORT)

    def __init__(self,
                 app_name,
                 addresses=None,
                 attempts=3,
                 delay=0.1, max_delay=60.0, delay_exp=2.0,
                 connect_timeout=None,
                 timeout=None,
                 logger=None):
        self.delay = delay
        self.max_delay = max_delay
        self.delay_exp = delay_exp
        self.connect_timeout = connect_timeout
        self.timeout = timeout
        self.attempts = attempts
        self.logger = logger or Logger()
        self._reset()

        addresses = addresses or ReconnectableService.DEFAULT_ADDRESS
        pairs = []
        for address in addresses.split(','):
            address_parts = address.split(':')
            host = address_parts[0]
            port = (len(address_parts) > 1 and int(address_parts[1]) or
                    ReconnectableService.DEFAULT_PORT)
            pairs.append((host, port))
        self.addresses = itertools.cycle(pairs)

        self.app_name = app_name
        self.upstream = None

    def _reset(self):
        self._cur_delay = self.delay

    def run_sync(self, *args, **kwargs):
        return ioloop.IOLoop.current().run_sync(lambda: self.enqueue(*args, **kwargs))

    @coroutine
    def enqueue(self, handler, data, attempts=None, timeout=None):
        attempt = 1
        request_attempts = attempts or self.attempts
        while True:
            try:
                yield self._reconnect_if_needed()
                channel = yield self.upstream.enqueue(handler)
                yield channel.tx.write(data)
                yield channel.tx.close()
                response = yield channel.rx.get(timeout=timeout or self.timeout)
                self._reset()
                raise Return(msgpack.unpackb(response, use_list=False))
            except Return:
                raise
            except Exception as e:
                error_str = 'Upstream service request failed (attempt {}/{}): {} ({})'.format(
                    attempt, request_attempts, e, type(e))
                self.logger.error(error_str)
                if isinstance(e, ServiceConnectionError):
                    if isinstance(e, DisconnectionError):
                        self.logger.debug('Disconnection from upstream service, '
                                          'will reconnect on next attempt')
                        self.upstream = None
                elif isinstance(e, ServiceError):
                    self.upstream = None
                if attempt >= request_attempts:
                    self._reset()
                    raise
                attempt += 1
                yield self._delay()

    @coroutine
    def _delay(self):
        f = Future()
        ioloop.IOLoop.current().add_timeout(timedelta(seconds=self._cur_delay),
                                            lambda: f.set_result(None))
        self.logger.debug('Delaying for {:.2f} s'.format(self._cur_delay))
        yield f
        self.logger.debug('Resuming from delay...')
        self._cur_delay = min(self._cur_delay * self.delay_exp, self.max_delay)

    @coroutine
    def _reconnect_if_needed(self):
        if not self.upstream:
            host, port = self.addresses.next()
            self.upstream = Service(
                name=self.app_name,
                endpoints=((host, port),),
                timeout=self.connect_timeout,
            )
            self.logger.debug('Connecting to upstream service "{}", host={}, '
                              'port={}'.format(self.app_name, host, port))
            yield self.upstream.connect()

        if not self.upstream._connected:
            self.logger.debug(
                'Reconnecting to upstream service "{}"'.format(
                    self.app_name))
            yield self.upstream.connect()
