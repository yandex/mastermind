from collections import defaultdict
from functools import wraps
import logging
import msgpack
import socket
from time import time
import traceback
import uuid

import elliptics
from errors import CacheUpstreamError
from cocaine.futures import chain
from config import config


logger = logging.getLogger('mm.balancer')


def handler(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error('Error: ' + str(e) + '\n' + traceback.format_exc())
            return {'Error': str(e)}

    return wrapper


def source(func):
    """Marks function or method as source of engine context.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return chain.Chain([lambda: func(*args, **kwargs)])
    return wrapper


def concurrent_handler(f):

    def sync_wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error('Error: ' + str(e) + '\n' + traceback.format_exc())
            return {'Error': str(e)}

    @wraps(f)
    @source
    def wrapper(*args, **kwargs):
        yield chain.concurrent(sync_wrapper)(*args, **kwargs)

    return wrapper


def session_op_retry(op, acc_codes):

    def wrapped_retry(session, *args, **kwargs):
        """
        Returns tuple of (successful groups, failed_groups)
        """
        s = session.clone()

        groups = set(s.groups)
        dest_groups = set(s.groups)

        s.set_checker(elliptics.checkers.no_check)
        s.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
        s.set_filter(elliptics.filters.all_with_ack)

        retries = kwargs.pop('retries', 3)

        for i in xrange(retries):
            s.set_groups(dest_groups)
            res = getattr(s, op)(*args).get()
            success_groups = set(
                [r.group_id for r in res if r.error.code in acc_codes])
            dest_groups -= success_groups
            if not dest_groups:
                break

        return groups - dest_groups, dest_groups

    return wrapped_retry


write_retry = session_op_retry('write_data', (0,))
remove_retry = session_op_retry('remove', (0, -2))


def defaultdict_to_dict(d):
    res = {}

    def convert_value(v):
        if isinstance(v, (dict, defaultdict)):
            return defaultdict_to_dict(v)
        return v

    for k, v in d.iteritems():
        res[k] = convert_value(v)
    return res


def ips_set(hostname):
    ips = set()
    addrinfo = socket.getaddrinfo(hostname,
        config.get('elliptics_base_port', 1024))
    for res in addrinfo:
        ips.add(res[4][0])

    return ips


def register_handle(W, h):
    logger = logging.getLogger('mm.init')
    @wraps(h)
    def wrapper(request, response):
        start_ts = time()
        req_uid = uuid.uuid4().hex
        try:
            data = yield request.read()
            data = msgpack.unpackb(data)
            logger.info(":{req_uid}: Running handler for event {0}, "
                "data={1}".format(h.__name__, str(data), req_uid=req_uid))
            #msgpack.pack(h(data), response)
            res = h(data)
            if isinstance(res, chain.Chain):
                res = yield res
            else:
                logger.error('Synchronous handler for {0} handle'.format(h.__name__))
            response.write(res)
        except Exception as e:
            logger.error(":{req_uid}: handler for event {0}, "
                "data={1}: Balancer error: {2}\n{3}".format(
                    h.__name__, str(data), e,
                    traceback.format_exc(),
                    req_uid=req_uid))
            response.write({"Balancer error": str(e)})
        finally:
            logger.info(':{req_uid}: Finished handler for event {0}, '
                'time: {1:.3f}'.format(h.__name__, time() - start_ts, req_uid=req_uid))
        response.close()

    W.on(h.__name__, wrapper)
    logger.info("Registering handler for event %s" % h.__name__)
    return wrapper


def process_elliptics_async_result(result, processor, *args, **kwargs):
    """Universal processing of elliptics concurrent session requests.

    Additional keyword parameters:
        raise_on_error: raise exception in case of error returned with
            elliptics async result (default is True)
    """
    result.wait()
    if not len(result.get()):
        raise ValueError('empty response')
    entry = result.get()[0]
    raise_on_error = kwargs.pop('raise_on_error', True)
    if entry.error.code and raise_on_error:
        raise Exception(entry.error.message)

    return processor(entry, elapsed_time=result.elapsed_time(),
                     end_time=result.end_time(),
                     *args, **kwargs)

def hosts_dcs(hosts):
    dcs = []
    for host in hosts:
        try:
            dcs.append(host.dc)
        except CacheUpstreamError:
            raise RuntimeError('Failed to get dc for host {}'.format(host))
    return dcs
