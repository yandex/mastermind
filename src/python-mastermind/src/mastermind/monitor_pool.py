try:
    import simplejson as json
except ImportError:
    import json
import zlib

import msgpack
from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from mastermind.pool import PoolWorker


class MonitorStatParseWorker(PoolWorker):
    """Fetch and parse monitor stat from elliptics monitor port

    Performs heavy task of json parsing and packs it back using fast msgpack.

    Arguments:
        ioloop: tornado IOLoop instance;
        max_http_clients: number of concurrent requests that can be in progress;
        monitor_stat_categories: elliptics statistics categories that should be fetched;
        monitor_port: elliptics monitor port;
        connect_timeout: timeout for initial connection in seconds;
        request_timeout: timeout for entire request in seconds;
        **kwds: passed through to base PoolWorker class;
    """

    HTTPClient = AsyncHTTPClient

    def __init__(self,
                 ioloop=None,
                 max_http_clients=1,
                 monitor_stat_categories=0,
                 monitor_port=10025,
                 connect_timeout=5.0,
                 request_timeout=5.0,
                 **kwds):
        super(MonitorStatParseWorker, self).__init__(ioloop=ioloop, **kwds)
        self._monitor_stat_categories = monitor_stat_categories
        self._monitor_port = monitor_port
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self.http_client = MonitorStatParseWorker.HTTPClient(
            self._ioloop,
            max_clients=max_http_clients,
        )

    def url(self, host):
        return 'http://{host}:{port}/?categories={categories}'.format(
            host=host,
            port=self._monitor_port,
            categories=self._monitor_stat_categories,
        )

    @gen.coroutine
    def process(self, (host, port, family)):
        http_request = HTTPRequest(
            self.url(host=host),
            connect_timeout=self._connect_timeout,
            request_timeout=self._request_timeout,
        )
        response = yield self.http_client.fetch(http_request, raise_error=False)
        result = self._parse_response(host, port, family, response)

        raise gen.Return(msgpack.packb(result))

    def _parse_response(self, host, port, family, response):
        error = None
        content = ''
        if response.error:
            error = str(response.error)
        else:
            try:
                content_type = response.headers.get('Content-Type')
                if content_type != 'application/json':
                    raise ValueError(
                        'unsupported content-type "{}"'.format(content_type)
                    )
                content = response.body
                if response.headers.get('Content-Encoding') == 'deflate':
                    content = zlib.decompress(content)
                content = json.loads(content)
            except Exception as e:
                error = 'Failed to parse json: {}'.format(e)
        return {
            'host': host,
            'port': port,
            'family': family,
            'code': response.code,
            'request_time': response.request_time,
            'url': response.effective_url,
            'error': error,
            'content': content,
        }
