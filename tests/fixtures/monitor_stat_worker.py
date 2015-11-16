import json
import threading
import zlib

import pytest
from tornado import gen
import tornado.ioloop
import tornado.httpserver
import tornado.netutil
import tornado.web

from mastermind.pool import Pool
from mastermind.monitor_pool import MonitorStatParseWorker


@pytest.fixture
def monitor_pool(monitor_port, request_timeout):
    """Pool of monitor stat workers"""
    pool = Pool(
        processes=1,
        worker=MonitorStatParseWorker,
        w_initkwds={
            'monitor_port': monitor_port,
            'request_timeout': request_timeout,
        }
    )
    return pool


@pytest.yield_fixture
def monitor_server(family,
                   ascii_data,
                   response_code,
                   response_processing_time,
                   valid_json,
                   encode_content):
    """Run monitor server emulating elliptics in a separate thread"""

    class DataHandler(tornado.web.RequestHandler):
        @gen.coroutine
        def get(self):
            """Emulate elliptics monitor stat compression"""
            if response_code != 200:
                self.set_status(response_code)
                self.finish()
                return
            data = ascii_data
            if valid_json:
                self.set_header('Content-Type', 'application/json')
                data = json.dumps({'data': data})
            if encode_content:
                self.set_header('Content-Encoding', 'deflate')
                data = zlib.compress(data)
            if response_processing_time:
                yield gen.sleep(response_processing_time)
            self.write(data)

    sockets = tornado.netutil.bind_sockets(port=0, family=family)

    def run_server(c):
        thread_io_loop = tornado.ioloop.IOLoop()
        thread_io_loop.make_current()
        app = tornado.web.Application([
            (r'/', DataHandler)
        ])
        server = tornado.httpserver.HTTPServer(app, io_loop=thread_io_loop)
        server.add_sockets(sockets)

        t = threading.current_thread()
        t.server = server
        t.io_loop = thread_io_loop
        with c:
            c.notify()
        thread_io_loop.start()
        thread_io_loop.close()

    c = threading.Condition()
    with c:
        t = threading.Thread(target=run_server, args=(c,))
        t.start()
        # wait until server is initialized
        c.wait()

    yield t.server

    def stop_server():
        t = threading.current_thread()
        t.server.stop()
        t.io_loop.stop()

    # ioloop should be stopped from the thread itself, therefore callback
    t.io_loop.add_callback(stop_server)
    t.join()


@pytest.fixture
def monitor_port(monitor_server):
    """Port of the running monitor server"""
    _, monitor_port = monitor_server._sockets.values()[0].getsockname()[:2]
    return monitor_port
