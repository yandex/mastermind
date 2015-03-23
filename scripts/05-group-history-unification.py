#!/usr/bin/env python
from copy import deepcopy
import datetime
import json
import logging
import msgpack
import os.path
import re
import sys
import time

import elliptics


logger = logging.getLogger('mm.convert')



CONFIG_PATH = '/etc/elliptics/mastermind.conf'

try:

    with open(CONFIG_PATH, 'r') as config_file:
        config = json.load(config_file)

except Exception as e:
    raise ValueError('Failed to load config file %s: %s' % (CONFIG_PATH, e))


log = elliptics.Logger('/tmp/group-history-convert.log', config["dnet_log_mask"])
node_config = elliptics.Config()
meta_node = elliptics.Node(log, node_config)

addresses = [elliptics.Address(host=str(node[0]), port=node[1], family=node[2])
             for node in config["metadata"]["nodes"]]
logger.info('Connecting to meta nodes: {0}'.format(config["metadata"]["nodes"]))
meta_wait_timeout = config['metadata'].get('wait_timeout', 5)

try:
    meta_node.add_remotes(addresses)
except Exception as e:
    logger.error('Failed to connect to any elliptics meta storage node: {0}'.format(
        e))
    raise ValueError('Failed to connect to any elliptics storage META node')

meta_session = elliptics.Session(meta_node)
meta_session.set_timeout(meta_wait_timeout)
meta_session.add_groups(list(config["metadata"]["groups"]))

time.sleep(5)

# def get_max_group_id():
#     max_group = int(meta_session.read_data(
#                     'mastermind:max_group').get()[0].data)
#     return max_group

BASE_PORT = config.get('elliptics_base_port', 1024)

def port_to_path(port):
    if port == 9999:
        return '/srv/cache/'
    return os.path.join(config.get('elliptics_base_storage_path', '/srv/storage/'),
                        port_to_dir(port) + '/')

def port_to_dir(port):
    return str(port - BASE_PORT)

IPV4_RE = re.compile('\d{0,3}\.\d{0,3}\.\d{0,3}\.\d{0,3}')

def ip_to_family(ip):
    if IPV4_RE.match(ip) is not None:
        return 2
    else:
        return 10

def try_convert_group_history(index):
    try:
        data = msgpack.unpackb(index.data)
        for node_set in data['nodes']:
            updated_node_set = []
            for node in node_set['set']:
                record = []
                if len(node) == 2:
                    # old-style record
                    record.extend(node)
                    record.append(ip_to_family(node[0]))
                    record.append(None)  # backend_id didn't exist
                    record.append(port_to_path(node[1]))
                elif len(node) == 4:
                    record.extend(node[0:2])
                    record.append(ip_to_family(node[0]))
                    record.extend(node[2:4])
                elif len(node) == 5:
                    record.extend(node)
                else:
                    raise ValueError('Group {0} history record is strange: {1}'.format(data['id'], data))
                updated_node_set.append(tuple(record))
            node_set['set'] = tuple(updated_node_set)
            if not 'type' in node_set:
                node_set['type'] = 'automatic'

        # print data
        eid = meta_session.transform('mastermind:group_%d' % data['id'])
        meta_session.update_indexes(eid, ['mastermind:groups_idx'],
                                         [msgpack.packb(data)]).get()
        print "Converted group {0}".format(data['id'])
    except Exception as e:
        print "Failed to convert index record: {0}, data: {1}".format(e, index.data)

def convert_groups_history():
    for res in meta_session.find_all_indexes(['mastermind:groups_idx']).get():
        for idx in res.indexes:
            try_convert_group_history(idx)

if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('convert',):
        print "Usage: {0} convert".format(sys.argv[0])
        sys.exit(1)

    convert_groups_history()



