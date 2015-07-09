#!/usr/bin/env python
import json
import logging
import sys

import msgpack
from kazoo.client import KazooClient
from mastermind.utils.queue import LockingQueue


logger = logging.getLogger('mm.convert')


CONFIG_PATH = '/etc/elliptics/mastermind.conf'

try:

    with open(CONFIG_PATH, 'r') as config_file:
        config = json.load(config_file)

except Exception as e:
    raise ValueError('Failed to load config file %s: %s' % (CONFIG_PATH, e))


def make_cache_kazoo_client():
    kz = KazooClient(config['cache']['manager']['host'])
    kz.start()
    return kz


def move_tasks(kz):
    base_path = config['cache']['manager']['lock_path_prefix']
    for path in kz.get_children(base_path + '/entries'):
        item = kz.get('{}/entries/{}'.format(
            base_path, path))[0]
        if not item:
            continue
        task = msgpack.unpackb(item)
        q = LockingQueue(kz, base_path, task['group'])
        q.put(item)


def clean_old_tasks(kz):
    base_path = config['cache']['manager']['lock_path_prefix']
    kz.delete(base_path + '/entries', recursive=True)
    kz.delete(base_path + '/taken', recursive=True)


if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('move', 'clean'):
        print "Usage: {0} move|clean".format(sys.argv[0])
        sys.exit(1)

    if sys.argv[1] == 'move':

        kz = make_cache_kazoo_client()
        move_tasks(kz)

        print
    elif sys.argv[1] == 'clean':
        kz = make_cache_kazoo_client()
        clean_old_tasks(kz)

        print

