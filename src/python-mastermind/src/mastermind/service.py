from datetime import timedelta
import itertools

from cocaine.asio.exceptions import CommunicationError, DisconnectionError, IllegalStateError
from cocaine.futures import chain, Deferred
from cocaine.logging import Logger
from cocaine.services import Service
from tornado import ioloop


class ReconnectableService(object):

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 10053
    DEFAULT_ADDRESS = '{host}:{port}'.format(host=DEFAULT_HOST,
                                             port=DEFAULT_PORT)

    def __init__(self, app_name, attempts=3,
                 delay=0.1, max_delay=60.0, delay_exp=2.0,
                 connect_timeout=None,
                 timeout=None,
                 addresses=None,
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

    @chain.source
    def enqueue(self, handler, data):
        attempt = 1
        while True:
            try:
                yield self._reconnect_if_needed()
                yield self.upstream.enqueue(handler, data, timeout=self.timeout)
                self._reset()
                break
            except Exception as e:
                error_str = 'Upstream service request failed (attempt {}/{}): {}'.format(
                    attempt, self.attempts, e)
                if isinstance(e, CommunicationError):
                    self.logger.error(error_str)
                    if isinstance(e, DisconnectionError):
                        self.logger.debug('Disconnection from upstream service, '
                                          'will reconnect on next attempt')
                        self.upstream = None
                else:
                    self.logger.error(error_str)
                if attempt >= self.attempts:
                    self._reset()
                    raise
                attempt += 1
                yield self._delay()

    @chain.source
    def _delay(self):
        d = Deferred()
        ioloop.IOLoop.current().add_timeout(timedelta(seconds=self._cur_delay),
                                            lambda: d.trigger(None))
        self.logger.debug('Delaying for {:.2f} s'.format(self._cur_delay))
        yield d
        self.logger.debug('Resuming from delay...')
        self._cur_delay = min(self._cur_delay * self.delay_exp, self.max_delay)

    @chain.source
    def _reconnect_if_needed(self):
        if not self.upstream:
            host, port = self.addresses.next()
            self.upstream = Service(self.app_name, blockingConnect=False)
            self.logger.debug('Connecting to upstream service "{}", host={}, '
                              'port={}'.format(self.app_name, host, port))
            yield self.upstream.connect(host=host, port=port,
                                        timeout=self.connect_timeout,
                                        blocking=False)

        if not self.upstream.isConnected():
            try:
                self.logger.debug(
                    'Reconnecting to upstream service "{}"'.format(
                        self.app_name))
                yield self.upstream.reconnect(timeout=self.connect_timeout,
                                              blocking=False)
            except IllegalStateError:
                # seems to be in connecting state
                pass
