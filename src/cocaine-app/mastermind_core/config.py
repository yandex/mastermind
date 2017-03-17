# coding=utf-8
from __future__ import unicode_literals

import json

CONFIG_PATH = '/etc/elliptics/mastermind.conf'

try:

    with open(CONFIG_PATH, 'r') as config_file:
        config = json.load(config_file)

except Exception as e:
    raise ValueError('Failed to load config file %s: %s' % (CONFIG_PATH, e))

ELLIPTICS = config.get('elliptics', {})
ELLIPTICS_NODES = ELLIPTICS.get('nodes') or config['elliptics_nodes']
ELLIPTICS_WAIT_TIMEOUT = ELLIPTICS.get('wait_timeout', 5)

METADATA = config['metadata']
METADATA_OPTIONS = METADATA.get('options', {})
METADATA_URL = METADATA.get('url')
METADATA_JOBS_DB = METADATA.get('jobs', {}).get('db')

# only elliptics.Logger use this setting (json decode to utf8, but need byte arg)
DNET_LOG = config['dnet_log'].encode('utf-8')
DNET_LOG_MASK = config['dnet_log_mask']
IO_THREAD_NUM = config.get('io_thread_num', 1)
NON_BLOCKING_IO_THREAD_NUM = config.get('nonblocking_io_thread_num', 1)
NET_THREAD_NUM = config.get('net_thread_num', 1)
