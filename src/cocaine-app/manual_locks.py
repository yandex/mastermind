import logging
import socket

import helpers as h
import storage
from sync import sync_manager
from sync.error import LockError


logger = logging.getLogger('mm.locker')


class ManualLocker(object):

    HOST_LOCK_PREFIX = 'manual/host/'
    HOST_LOCK = HOST_LOCK_PREFIX + '{0}'

    @h.concurrent_handler
    def host_acquire_lock(self, request):

        try:
            host = request[0]
        except IndexError:
            raise ValueError('Host is required')

        check_host(host)

        lock_id = self.HOST_LOCK.format(host)
        sync_manager.persistent_locks_acquire([lock_id])

        return lock_id

    @h.concurrent_handler
    def host_release_lock(self, request):

        try:
            host = request[0]
        except IndexError:
            raise ValueError('Host is required')

        check_host(host)

        lock_id = self.HOST_LOCK.format(host)
        sync_manager.persistent_locks_release([lock_id])

        return lock_id

    def get_locked_hosts(self):
        locks = sync_manager.get_children_locks(self.HOST_LOCK_PREFIX)
        hostnames = set([lock[len(self.HOST_LOCK_PREFIX):] for lock in locks])
        hosts = set()
        logger.debug('hostnames: {0}'.format(hostnames))
        for host in storage.hosts:
            if host.hostname in hostnames:
                hosts.add(host)
        return hosts


def check_host(host):
    try:
        resolved_host = socket.gethostbyaddr(host)[0]
    except Exception as e:
        error_msg = 'Failed to resolve host {0}'.format(host)
        logger.error(error_msg)
        raise ValueError(error_msg)

    if host != resolved_host:
        error_msg = ('Hostname is required '
                     '(host {0} is resolved to hostname {1})'.format(
                         host, resolved_host))
        logger.error(error_msg)
        raise ValueError(error_msg)


manual_locker = ManualLocker()
