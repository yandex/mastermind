import socket

import msgpack
import pytest


@pytest.mark.parametrize('family', (socket.AF_INET, socket.AF_INET6))
@pytest.mark.parametrize('data_size', (1024,))
@pytest.mark.parametrize('valid_json', (True, False,))
@pytest.mark.parametrize('encode_content', (True, False,))
@pytest.mark.parametrize('request_timeout', (0.2,))
class TestMonitorStatParseWorker(object):
    """Test MonitorStatParse worker

    This suit validates:
        - acting upon various http response statuses;
        - support of 'deflate' encoding;
        - timeout tolerance.
    """
    @pytest.mark.parametrize('response_code', (200,))
    @pytest.mark.parametrize('response_processing_time', (0.0,))
    def test_200_response(self,
                          monitor_pool,
                          ascii_data,
                          family,
                          response_code,
                          valid_json):
        """Valid response fetching check with enabled or disable encoding"""
        task = ('localhost', 1025, family)
        result = msgpack.unpackb(monitor_pool.apply(None, (task,)))
        assert response_code == result['code']
        if not valid_json:
            assert result['error'].startswith('Failed to parse json')
        if valid_json:
            assert result['content']['data'] == ascii_data

    @pytest.mark.parametrize('response_code', (404, 502,))
    @pytest.mark.parametrize('response_processing_time', (0.0,))
    def test_bad_response(self,
                          monitor_pool,
                          ascii_data,
                          family,
                          response_code,
                          valid_json):
        """Tolerance for any http status other than 200"""
        task = ('localhost', 1025, family)
        result = msgpack.unpackb(monitor_pool.apply(None, (task,)))
        assert response_code == result['code']

    @pytest.mark.parametrize('response_code', (200,))
    @pytest.mark.parametrize('response_processing_time', (0.5,))
    def test_timeout(self,
                     monitor_pool,
                     ascii_data,
                     family,
                     response_code,
                     valid_json):
        """Request timeout tolerance"""
        task = ('localhost', 1025, family)
        result = msgpack.unpackb(monitor_pool.apply(None, (task,)))
        # From tornado docs:
        # "Error code 599 is used when no HTTP response was received, e.g. for a timeout."
        #
        # http://www.tornadoweb.org/en/stable/httpclient.html#tornado.httpclient.HTTPError
        assert result['code'] == 599
