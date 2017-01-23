import json
import threading
import time

from mastermind_core.config import config
from mastermind_core import helpers


class CachedResponse(object):
    """Mastermind cached response

    Use CachedResponse to store mastermind handle's response data that requires
    significant calculation.

    Instance's reentrant ``lock`` object is designed to provide atomicity when deriving
    from CachedResponse class for implementation of 'set_result',
    'set_exception' and 'get_result' methods.

    NB: this object is not able to cache responses for parametrized requests.

    TODO: add unit tests
    """
    def __init__(self):
        self.lock = threading.RLock()
        self._result = None
        self._exception = None
        self._ts = time.time()

    def get_result(self, *args, **kwargs):
        """Get cached result or raise stored exception

        Parameters: None
        """
        with self.lock:
            if self.exception:
                raise self.exception
            return self._result

    def _is_fresh_result(self, ts):
        if ts is None:
            ts = time.time()
        return ts > self._ts

    def set_result(self, result, ts=None):
        """Set result for cached response

        Parameters:
            result - result of calculation;
        """
        with self.lock:
            if not self._is_fresh_result(ts):
                return
            self._result = result
            self._exception = None
            self._ts = ts

    @property
    def exception(self):
        return self._exception

    def set_exception(self, exception):
        """Set exception that will be thrown on 'get_result' call

        Parameters:
            exception - exception to throw;
        """
        with self.lock:
            self._result = None
            self._exception = exception


GZIP_CONFIG = config.get('gzip', {})
DEFAULT_GZIP_COMPRESSION_LEVEL = GZIP_CONFIG.get('compression_level', 1)


class CachedGzipResponse(CachedResponse):
    """Mastermind cached response with additionally stored compressed version

    Compression is performed using gzip on serialized json string, so be sure
    that result can be dumped to json.
    """

    def __init__(self, compression_level=DEFAULT_GZIP_COMPRESSION_LEVEL):
        super(CachedGzipResponse, self).__init__()
        self._compressed_result = None
        self._compression_level = compression_level

    def set_result(self, result, ts=None):
        """Set result for cached response and also store its compressed version
        """
        if not self._is_fresh_result(ts):
            # check to prevent expensive compression step
            return
        compressed_result = self._compress(result)
        with self.lock:
            if not self._is_fresh_result(ts):
                return
            super(CachedGzipResponse, self).set_result(result)
            self._compressed_result = compressed_result

    def set_exception(self, exception):
        """Set exception that will be thrown on 'get_result' call

        Parameters:
            exception - exception to throw;
        """
        with self.lock:
            super(CachedGzipResponse, self).set_exception(exception)
            self._compressed_result = None

    def get_result(self, compressed=True):
        """Get cached result or raise stored exception

        Parameters:
            compressed - boolean flag which determines if compressed version
            of cached response should be returned;
        """
        with self.lock:
            if not compressed:
                return super(CachedGzipResponse, self).get_result()
            if self.exception:
                raise self.exception
            return self._compressed_result

    def _compress(self, result):
        try:
            data = json.dumps(result)
        except (TypeError, ValueError):
            raise TypeError(
                'Cached gzip response does not support objects '
                'that cannot be dumped to json'
            )
        return helpers.gzip_compress(data, compression_level=self._compression_level)
