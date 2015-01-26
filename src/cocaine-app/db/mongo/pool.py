# -*- coding: utf-8 -*-
import logging
import pymongo
import sys
import time
import re
import threading
import traceback

from copy import deepcopy
import pymongo
from pymongo.collection import Collection as OriginalCollection
from pymongo.errors import ConnectionFailure, OperationFailure, AutoReconnect
from pymongo.mongo_replica_set_client import MongoReplicaSetClient as MRSC


logger = logging.getLogger('mm.mongo')

RE_SPLIT_PATH = re.compile('/')

RE_SOCKET_TIME = re.compile("^(.*socket_time: )(\d+\.\d+)(.*)$")
RE_REQUEST_NUMBER = re.compile("^(.*\.)(\d+)(.*)$")


class Request(object):
    def __init__(self):
        self.local = threading.local()

    @property
    def request_message(self):
        if getattr(self.local, 'request_message', None) is None:
            self.local.request_message = ''
        return self.local.request_message

    @request_message.setter
    def request_message(self, value):
        self.local.request_message = value

    @property
    def processing_time(self):
        if getattr(self.local, 'processing_time', None) is None:
            self.local.processing_time = None
        return self.local.processing_time

    @processing_time.setter
    def processing_time(self, value):
        self.local.processing_time = value


request = Request()

original_unpack_response = deepcopy(pymongo.helpers._unpack_response)


def __set_request_message__(msg):
    request.request_message = msg + '.0'


def __get_request_message__():
    try:
        nose, number, tail = RE_REQUEST_NUMBER.match(request.request_message).groups()
    except (AttributeError, ValueError):
        return request.request_message
    else:
        request.request_message = nose + str(int(number) + 1) + tail
        return request.request_message


def __set_processing_time__(t):
    request.processing_time = t


def __get_processing_time__():
    result = request.processing_time
    request.processing_time = None
    return result


def log_request(f):

    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = f(*args, **kwargs)
        except Exception, e:
            delta = time.time() - start
            msg = '%s %.3f' % (__get_request_message__(), delta)
            error = '%s: %s, %s' % (e.__class__.__name__, e, traceback.format_exc())
            logger.debug(msg)
            logger.error(error)
            raise
        else:
            delta = time.time() - start
            if f.__name__ == '_send_message_with_response':
                __set_processing_time__(delta)
            else:
                msg = '%s %.3f' % (__get_request_message__(), delta)
                logger.debug(msg)
            return result
    return wrapper


def __unpack_response(*args, **kwargs):
    """Unpack a response from the database and log it.
    """
    result = original_unpack_response(*args, **kwargs)
    delta = __get_processing_time__()
    if delta is not None:
        request_message = __get_request_message__()
        if request_message:
            request_message += ' '
        msg = request_message + '%s %s %.3f' % (result.get('number_returned'), sys.getsizeof(result.get('data')), delta)
        logger.debug(msg)
    return result
pymongo.helpers._unpack_response = __unpack_response


class CustomPool(pymongo.pool.Pool):
    def get_socket(self, *args, **kwargs):
        start = time.time()
        result = pymongo.pool.Pool.get_socket(self, *args, **kwargs)
        delta = time.time() - start
        try:
            nose, sock_time, tail = RE_SOCKET_TIME.match(request.request_message).groups()
        except (AttributeError, ValueError):
            request.request_message += " socket_time: %.3f" % delta
        else:
            request.request_message = nose + '%.3f' % (float(sock_time) + delta) + tail
        return result


class MongoReplicaSetClient(MRSC):
    def __init__(self, *args, **kwargs):
        kwargs['_pool_class'] = CustomPool
        kwargs['read_preference'] = pymongo.ReadPreference.PRIMARY_PREFERRED
        super(MongoReplicaSetClient, self).__init__(*args, **kwargs)

    @log_request
    def _send_message(self, *args, **kwargs):
        return super(MongoReplicaSetClient, self)._send_message(*args, **kwargs)

    @log_request
    def _send_message_with_response(self, *args, **kwargs):
        return super(MongoReplicaSetClient, self)._send_message_with_response(*args, **kwargs)


def slave_read_status(kwargs, read_pref=None):
    if kwargs.get('slave_okay'):
        return 'SLAVE'
    elif read_pref == 1:
        return 'PRIMARY_PREFERRED'
    else:
        return 'PRIMARY'


class Collection(OriginalCollection):
    def update(self, *args, **kwargs):
        request_message = '%s.%s.%s(%s, %s)' % (self.database.name, self.name, 'update', str(args), str(kwargs))
        __set_request_message__(request_message)
        return super(Collection, self).update(*args, **kwargs)

    def insert(self, *args, **kwargs):
        request_message = '%s.%s.%s(%s, %s)' % (self.database.name, self.name, 'insert', str(args), str(kwargs))
        __set_request_message__(request_message)
        return super(Collection, self).insert(*args, **kwargs)

    def find_and_modify(self, *args, **kwargs):
        request_message = '%s.%s.%s(%s, %s)' % (self.database.name, self.name, 'find_and_modify', str(args), str(kwargs))
        __set_request_message__(request_message)
        return super(Collection, self).find_and_modify(*args, **kwargs)

    def remove(self, *args, **kwargs):
        request_message = '%s.%s.%s(%s, %s)' % (self.database.name, self.name, 'remove', str(args), str(kwargs))
        __set_request_message__(request_message)
        return super(Collection, self).remove(*args, **kwargs)

    def find(self, *args, **kwargs):
        request_message = '%s.%s.%s(%s, %s, read=%s)' % \
            (self.database.name, self.name, 'find', str(args), str(kwargs), slave_read_status(kwargs))
        __set_request_message__(request_message)
        return super(Collection, self).find(*args, **kwargs)

    def find_one(self, *args, **kwargs):
        request_message = '%s.%s.%s.%s(%s, %s, read=%s)' % \
            (self.database.connection.name, self.database.name, self.name, 'find_one', str(args), str(kwargs), slave_read_status(kwargs))
        __set_request_message__(request_message)
        return super(Collection, self).find_one(*args, **kwargs)
