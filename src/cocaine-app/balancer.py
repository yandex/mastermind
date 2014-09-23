# encoding: utf-8
import copy
from datetime import datetime
import itertools
import logging
import random
import re
import sys
import time
import traceback

from cocaine.worker import Worker
import elliptics
import msgpack

import balancelogicadapter as bla
import balancelogic
from config import config
import helpers as h

import infrastructure
import keys
import statistics
import storage


logger = logging.getLogger('mm.balancer')

logger.info('balancer.py')


class Balancer(object):

    DT_FORMAT = '%Y-%m-%d %H:%M:%S'
    MIN_NS_UNITS = config.get('balancer_config', {}).get('min_units', 1)

    def __init__(self, n):
        self.node = n
        self.infrastructure = None
        self.statistics = statistics.Statistics(self)

    def set_infrastructure(self, infrastructure):
        self.infrastructure = infrastructure

    def get_groups(self, request):
        return tuple(group.group_id for group in storage.groups)

    @h.handler
    def get_symmetric_groups(self, request):
        result = [couple.as_tuple() for couple in storage.couples if couple.status == storage.Status.OK]
        logger.debug('good_symm_groups: ' + str(result))
        return result

    @h.handler
    def get_bad_groups(self, request):
        result = [couple.as_tuple() for couple in storage.couples if couple.status not in storage.NOT_BAD_STATUSES]
        logger.debug('bad_symm_groups: ' + str(result))
        return result

    @h.handler
    def get_frozen_groups(self, request):
        result = [couple.as_tuple() for couple in storage.couples if couple.status == storage.Status.FROZEN]
        logger.debug('frozen_couples: ' + str(result))
        return result

    @h.handler
    def get_closed_groups(self, request):
        result = []
        min_free_space = config['balancer_config'].get('min_free_space', 256) * 1024 * 1024
        min_rel_space = config['balancer_config'].get('min_free_space_relative', 0.15)

        logger.debug('configured min_free_space: %s bytes' % min_free_space)
        logger.debug('configured min_rel_space: %s' % min_rel_space)

        result = [couple.as_tuple() for couple in storage.couples
                  if couple.status == storage.Status.FULL]

        logger.debug('closed couples: ' + str(result))
        return result

    @h.handler
    def get_empty_groups(self, request):
        logger.info('len(storage.groups) = %d' % (len(storage.groups.elements)))
        logger.info('groups: %s' % str([(group.group_id, group.couple) for group in storage.groups if group.couple is None]))
        result = [group.group_id for group in storage.groups if group.couple is None]
        logger.debug('uncoupled groups: ' + str(result))
        return result


    STATES = {
        'good': [storage.Status.OK],
        'full': [storage.Status.FULL],
        'frozen': [storage.Status.FROZEN],
        'bad': [storage.Status.INIT, storage.Status.BAD],
    }

    @h.handler
    def get_couples_list(self, request):
        options = request[0]

        couples = storage.couples.keys()

        if options.get('namespace', None):
            couples = filter(lambda c: c.namespace == options['namespace'], couples)

        if options.get('state', None):
            if options['state'] not in self.STATES:
                raise ValueError('Invalid state: {0}'.format(options['state']))
            couples = filter(lambda c: c.status in self.STATES[options['state']], couples)

        data = []
        for c in couples:
            info = c.info()
            info['groups'] = [g.info() for g in c]
            data.append(info)
        return data

    @h.handler
    def get_group_meta(self, request):
        gid = request[0]
        key = request[1] or keys.SYMMETRIC_GROUPS_KEY
        unpack = request[2]

        if not gid in storage.groups:
            raise ValueError('Group %d is not found' % group)

        group = storage.groups[gid]

        logger.info('Creating elliptics session')

        s = elliptics.Session(self.node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
        s.set_timeout(wait_timeout)
        s.add_groups([group.group_id])

        data = s.read_data(key).get()[0]

        logger.info('Read key {0} from group {1}: {2}'.format(key.replace('\0', r'\0'), group, data.data))

        return {'id': repr(data.id),
                'full_id': str(data.id),
                'data': msgpack.unpackb(data.data) if unpack else data.data}

    @h.handler
    def groups_by_dc(self, request):
        groups = request[0]
        logger.info('Groups: %s' % (groups,))
        groups_by_dcs = {}
        for g in groups:

            if not g in storage.groups:
                logger.info('Group %s not found' % (g,))
                continue

            group = storage.groups[g]
            group_data = {
                'group': group.group_id,
                'node_backends': [nb.info() for nb in group.node_backends],
            }
            if group.couple:
                group_data.update({
                    'couple': str(group.couple),
                    'couple_status': group.couple.status})

            if not group.node_backends:
                dc_groups = groups_by_dcs.setdefault('unknown', {})
                dc_groups[group.group_id] = group_data
                continue

            for node_backend in group.node_backends:
                dc = node_backend.node.host.dc
                dc_groups = groups_by_dcs.setdefault(dc, {})
                dc_groups[group.group_id] = group_data

        return groups_by_dcs

    @h.handler
    def couples_by_namespace(self, request):
        couples = request[0]
        logger.info('Couples: %s' % (couples,))

        couples_by_nss = {}

        for c in couples:
            couple_str = ':'.join([str(i) for i in sorted(c)])
            if not couple_str in storage.couples:
                logger.info('Couple %s not found' % couple_str)
            couple = storage.couples[couple_str]

            couple_data = {
                'couple': str(couple),
                'couple_status': couple.status,
                'node_backends': [nb.info() for g in couple for nb in g.node_backends]
            }
            try:
                couples_by_nss.setdefault(couple.namespace, []).append(couple_data)
            except ValueError as e:
                continue

        return couples_by_nss

    @h.handler
    def get_group_weights(self, request):
        namespaces = {}
        all_symm_group_objects = []

        try:
            ns = request[0]
        except IndexError:
            ns = None

        for couple in storage.couples:

            try:
                namespaces.setdefault(couple.namespace, set())
            except ValueError:
                continue

            namespaces[couple.namespace].add(len(couple))

            if couple.status not in storage.GOOD_STATUSES:
                continue

            symm_group = bla.SymmGroup(couple)
            all_symm_group_objects.append(symm_group)

        if ns and not ns in namespaces and not ns in self.infrastructure.ns_settings:
            raise ValueError('Namespace "{0}" does not exist'.format(ns))

        result = {}

        namespaces = ([(ns, namespaces.get(ns, set()))]
                      if ns is not None else
                      namespaces.iteritems())

        for namespace, sizes in namespaces:
            try:
                self._namespaces_weights(all_symm_group_objects, namespace, sizes, result)
            except ValueError:
                if ns is not None:
                    raise

        if len(result) == 0:
            raise ValueError('Failed to satisfy {0} availability settings'.format(
                'namespace ' + ns if ns else 'all namespaces'))

        logger.info(str(result))
        return result

    def _namespaces_weights(self, symm_groups, namespace, sizes, result):

        found_couples = 0

        ns_weights = {}

        for size in sizes:
            try:
                logger.info('Namespace {0}, size {1}: calculating '
                    'cluster info'.format(namespace, size))
                (group_weights, info) = balancelogic.rawBalance(
                    symm_groups, bla.getConfig(),
                    bla._and(bla.GroupSizeEquals(size),
                             bla.GroupNamespaceEquals(namespace)))
                ns_weights[size] = \
                    [([g.group_id for g in item[0].groups],) +
                         item[1:] +
                         (int(item[0].get_stat().free_space),)
                     for item in group_weights.items()]
                found_couples += len([item for item in ns_weights[size] if item[1] > 0])
                logger.info('Namespace {0}, size {1}: '
                    'cluster info: {2}'.format(namespace, size, info))
            except Exception as e:
                logger.error('Namespace {0}, size {1}: error {2}'.format(namespace, size, e))
                ns_weights[size] = []
                continue

        ns_min_units = self.infrastructure.ns_settings.get(namespace, {}).get(
            'min-units', self.MIN_NS_UNITS)
        if found_couples < ns_min_units:

            # TODO: remove logging, uncomment raise statement
            logger.warn('Namespace {0} has {1} available couples, '
                '{2} required'.format(namespace, found_couples, ns_min_units))

            # raise ValueError('Namespace {0} has {1} available couples, '
            #     '{2} required'.format(namespace, found_couples, ns_min_units))

        result[namespace] = ns_weights

    @h.handler
    def repair_groups(self, request):
        logger.info('----------------------------------------')
        logger.info('New repair groups request: ' + str(request))

        group_id = int(request[0])
        try:
            force_namespace = request[1]
        except IndexError:
            force_namespace = None

        if not group_id in storage.groups:
            return {'Balancer error': 'Group %d is not found' % group_id}

        group = storage.groups[group_id]

        bad_couples = []
        for couple in storage.couples:
            if group in couple:
                if couple.status in storage.NOT_BAD_STATUSES:
                    logger.error('Balancer error: cannot repair, group %d is in couple %s' % (group_id, str(couple)))
                    return {'Balancer error' : 'cannot repair, group %d is in couple %s' % (group_id, str(couple))}
                bad_couples.append(couple)

        if not bad_couples:
            logger.error('Balancer error: cannot repair, group %d is not a member of any couple' % group_id)
            return {'Balancer error' : 'cannot repair, group %d is not a member of any couple' % group_id}

        if len(bad_couples) > 1:
            logger.error('Balancer error: cannot repair, group %d is a member of several couples: %s' % (group_id, str(bad_couples)))
            return {'Balancer error' : 'cannot repair, group %d is a member of several couples: %s' % (group_id, str(bad_couples))}

        couple = bad_couples[0]

        # checking namespaces in all couple groups
        for g in couple:
            if g.group_id == group_id:
                continue
            if g.meta is None:
                logger.error('Balancer error: group %d (coupled with group %d) has no metadata' % (g.group_id, group_id))
                return {'Balancer error': 'group %d (coupled with group %d) has no metadata' % (g.group_id, group_id)}

        namespaces = [g.meta['namespace'] for g in couple if g.group_id != group_id]
        if not all(ns == namespaces[0] for ns in namespaces):
            logger.error('Balancer error: namespaces of groups coupled with group %d are not the same: %s' % (group_id, namespaces))
            return {'Balancer error': 'namespaces of groups coupled with group %d are not the same: %s' % (group_id, namespaces)}

        namespace_to_use = namespaces and namespaces[0] or force_namespace
        if not namespace_to_use:
            logger.error('Balancer error: cannot identify a namespace to use for group %d' % (group_id,))
            return {'Balancer error': 'cannot identify a namespace to use for group %d' % (group_id,)}

        (good, bad) = make_symm_group(self.node, couple, namespace_to_use)
        if bad:
            raise Exception(bad[1])

        return {'message': 'Successfully repaired couple', 'couple': str(couple)}

    @h.handler
    def get_group_info(self, request):
        group = int(request)
        logger.info('get_group_info: request: %s' % (str(request),))

        if not group in storage.groups:
            raise ValueError('Group %d is not found' % group)

        logger.info('Group %d: %s' % (group, repr(storage.groups[group])))

        return storage.groups[group].info()

    @h.handler
    def get_group_history(self, request):
        group = int(request[0])
        group_history = {}

        if self.infrastructure:
            for key, data in self.infrastructure.get_group_history(group).iteritems():
                for nodes_data in data:
                    dt = datetime.fromtimestamp(nodes_data['timestamp'])
                    nodes_data['timestamp'] = dt.strftime(self.DT_FORMAT)
                group_history[key] = data

        return group_history

    NODE_BACKEND_RE = re.compile('(.+):(\d+)/(\d+)')

    @h.handler
    def group_detach_node(self, request):
        group_id = int(request[0])
        node_backend_str = request[1]

        if not group_id in storage.groups:
            raise ValueError('Group %d is not found' % group_id)

        group = storage.groups[group_id]
        node_backend = (node_backend_str in storage.node_backends and
                        storage.node_backends[node_backend_str] or
                        None)

        logger.info('Node backend: {0}'.format(node_backend))
        try:
            host, port, backend_id = self.NODE_BACKEND_RE.match(node_backend_str).groups()
            port, backend_id = int(port), int(backend_id)
            logger.info('host, port, backend_id: {0}'.format((host, port, backend_id)))
        except (IndexError, ValueError):
            raise ValueError('Node backend should be of form <host>:<port>:<backend_id>')

        if node_backend and node_backend in group.node_backends:
            logger.info('Removing node backend {0} from group {1} nodes'.format(node_backend, group))
            group.remove_node_backend(node_backend)
            group.update_status_recursive()
            logger.info('Removed node backend {0} from group {1} nodes'.format(node_backend, group))

        logger.info('Removing node backend {0} from group {1} history'.format(node_backend, group))
        try:
            self.infrastructure.detach_node(group, host, port, backend_id)
            logger.info('Removed node backend {0} from group {1} history'.format(node_backend, group))
        except Exception as e:
            logger.error('Failed to remove {0} from group {1} history: {2}'.format(node_backend, group, str(e)))
            raise

        return True

    @h.handler
    def get_couple_info(self, request):
        group_id = int(request)
        logger.info('get_couple_info: request: %s' % (str(request),))

        if not group_id in storage.groups:
            raise ValueError('Group %d is not found' % group_id)

        group = storage.groups[group_id]
        couple = group.couple

        if not couple:
            raise ValueError('Group %s is not coupled' % group)

        logger.info('Group %s: %s' % (group, repr(group)))
        logger.info('Couple %s: %s' % (couple, repr(couple)))

        res = couple.info()
        res['groups'] = [g.info() for g in couple]

        return res

    VALID_COUPLE_INIT_STATES = (storage.Status.COUPLED, storage.Status.FROZEN)

    @h.handler
    def couple_groups(self, request):
        logger.info('----------------------------------------')
        logger.info('New couple groups request: ' + str(request))

        suitable_groups = []
        total_spaces = []

        for group_id in self.get_empty_groups(self.node):
            group = storage.groups[group_id]

            if not len(group.node_backends):
                logger.info('Group {0} cannot be used, it has '
                    'empty node list'.format(group.group_id))
                continue

            suitable = True
            for node_backend in group.node_backends:
                if node_backend.status != storage.Status.OK:
                    logger.info('Group {0} cannot be used, node backend {1} status '
                                'is {2} (not OK)'.format(group.group_id,
                                     node_backend, node.status))
                    suitable = False
                    break

            if not suitable:
                continue

            suitable_groups.append(group_id)
            total_spaces.append(group.get_stat().total_space)

        dc_by_group_id = {}
        ts_by_group_id = {}
        dc_groups_by_total_space = {}

        # bucketing groups by approximate total space
        ts_tolerance = config.get('total_space_diff_tolerance', 0.05)
        cur_ts_key = 0
        for ts in reversed(sorted(total_spaces)):
            if abs(cur_ts_key - ts) > cur_ts_key * ts_tolerance:
                cur_ts_key = ts
                dc_groups_by_total_space[cur_ts_key] = {}

        total_spaces = list(reversed(sorted(dc_groups_by_total_space.keys())))
        logger.info('group total space sizes available: {0}'.format(total_spaces))

        dc_groups_by_total_space['any'] = {}

        for group_id in suitable_groups:
            group = storage.groups[group_id]
            dc = group.node_backends[0].node.host.dc

            dc_by_group_id[group_id] = dc

            ts = group.get_stat().total_space
            for ts_key in total_spaces:
                if ts_key - ts < ts_key * ts_tolerance:
                    dc_groups_by_total_space[ts_key].setdefault(dc, []).append(group_id)
                    ts_by_group_id[group_id] = ts_key
                    dc_groups_by_total_space['any'].setdefault(dc, []).append(group_id)
                    break
            else:
                raise ValueError('Failed to find total space key for group {0}, '
                    'total space {1}'.format(group_id, ts))

        logger.info('dc by group: %s' % str(dc_by_group_id))
        logger.info('ts by group: %s' % str(ts_by_group_id))
        logger.info('dc_groups_by_total_space: %s' % str(dc_groups_by_total_space))

        size = int(request[0])
        mandatory_groups = [int(g) for g in request[1]]
        couples_num = int(request[2]) or 1
        check_space = request[3]

        if mandatory_groups and couples_num > 1:
            raise Exception('Batch couple building is prohibited when '
                'mandatory groups are set')

        mandatory_ts = None

        if mandatory_groups and check_space:
            cgroup_id = mandatory_groups[0]
            mandatory_ts = ts_by_group_id[cgroup_id]
            for group_id in mandatory_groups:
                if not ts_by_group_id[cgroup_id] == ts_by_group_id[group_id]:
                    raise Exception('Mandatory groups do not have equal '
                        'total space: group {0} total_space {1}, '
                        'group {2} total_space {3}'.format(
                            cgroup_id, storage.groups[cgroup_id].get_stat().total_space,
                            group_id, storage.groups[group_id].get_stat().total_space))

        # checking mandatory set and filtering unusable dcs
        for group_id in mandatory_groups:
            if group_id not in suitable_groups:
                raise Exception('group %d is not found in suitable groups '
                    '(is bad or coupled)' % group_id)
            dc = dc_by_group_id[group_id]
            ts = ts_by_group_id[group_id]

            if dc not in dc_groups_by_total_space['any']:
                raise Exception('mandatory groups must be in different dcs')
            del dc_groups_by_total_space['any'][dc]
            if dc not in dc_groups_by_total_space[ts]:
                raise Exception('mandatory groups must be in different dcs')
            del dc_groups_by_total_space[ts][dc]

        groups_to_couple = copy.copy(mandatory_groups)

        n_groups_to_add = size - len(groups_to_couple)
        if n_groups_to_add < 0:
            raise Exception('Too many mandatory groups')

        # filtering unusable total spaces
        for k in dc_groups_by_total_space.keys():
            if mandatory_ts and k != mandatory_ts:
                del dc_groups_by_total_space[k]
            elif not check_space and k != 'any':
                del dc_groups_by_total_space[k]
            elif check_space and k == 'any':
                del dc_groups_by_total_space[k]
            elif len(dc_groups_by_total_space[k]) < n_groups_to_add:
                del dc_groups_by_total_space[k]

        logger.info('dc_groups_by_total_space after filtering unmatching dcs '
            'and disk sizes: {0}'.format(str(dc_groups_by_total_space)))

        if not len(dc_groups_by_total_space):
            raise Exception('Not enough dcs')

        namespace = request[4]
        logger.info('namespace from request: {0}'.format(namespace))
        if not self.valid_namespace(namespace):
            raise ValueError('Namespace "{0}" is invalid'.format(namespace))

        init_state = request[5].upper()
        if not init_state in self.VALID_COUPLE_INIT_STATES:
            raise ValueError('Couple "{0}" init state is invalid'.format(init_state))

        created_couples = []
        bad_couples = []
        stop = False

        def chunks(l, n):
            if n == 0:
                yield []
            else:
                for i in xrange(0, len(l), n):
                    chunk = l[i:i + n]
                    if len(chunk) < n:
                        break
                    yield chunk

        while len(created_couples) < couples_num:

            if stop:
                break

            used_dcs = set()
            groups_found = False

            ts_dcs = [(ts, set(group_by_dc.keys()))
                      for ts, group_by_dc in dc_groups_by_total_space.iteritems()]
            ts_dcs.sort(key=lambda x: len(x[1]))

            for ts, dcs in ts_dcs:
                if stop:
                    break
                not_used_dcs = list(dcs - used_dcs)
                random.shuffle(not_used_dcs)
                logger.info('Ts: {0}, not used dcs {1}'.format(ts, not_used_dcs))
                for selected_dcs in chunks(not_used_dcs, n_groups_to_add):
                    logger.info('Ts: {0}, taken dcs {1}'.format(ts, selected_dcs))
                    # build couple from selected dcs, mark loop as successful
                    for dc in selected_dcs:
                        used_dcs.add(dc)
                        groups_to_couple.append(dc_groups_by_total_space[ts][dc].pop())

                    couple = storage.couples.add([storage.groups[g] for g in groups_to_couple])
                    (good, bad) = make_symm_group(self.node, couple, namespace)

                    if bad:
                        bad_couples.append([good, bad])
                        stop = True
                        break

                    if init_state == storage.Status.FROZEN:
                        self.__do_set_meta_freeze(couple, freeze=True)
                    elif init_state == storage.Status.COUPLED:
                        self.__do_set_meta_freeze(couple, freeze=False)

                    created_couples.append(couple)

                    groups_found = True

                    if len(created_couples) >= couples_num:
                        stop = True
                        break

                    groups_to_couple = []
                    for dc in dc_groups_by_total_space[ts].keys():
                        if not dc_groups_by_total_space[ts][dc]:
                            del dc_groups_by_total_space[ts][dc]
                    if len(dc_groups_by_total_space[ts]) < n_groups_to_add:
                        del dc_groups_by_total_space[ts]

            if not groups_found:
                stop = True

        return [c.as_tuple() for c in created_couples], bad_couples

    @h.handler
    def break_couple(self, request):
        logger.info('----------------------------------------')
        logger.info('New break couple request: ' + str(request))

        couple_str = ':'.join(map(str, sorted(request[0], key=lambda x: int(x))))
        if not couple_str in storage.couples:
            raise KeyError('Couple %s was not found' % (couple_str))

        couple = storage.couples[couple_str]
        confirm = request[1]

        logger.info('groups: %s; confirmation: "%s"' %
            (couple_str, confirm))

        correct_confirms = []
        correct_confirm = 'Yes, I want to break '
        if couple.status in storage.NOT_BAD_STATUSES:
            correct_confirm += 'good'
        else:
            correct_confirm += 'bad'

        correct_confirm += ' couple '

        correct_confirms.append(correct_confirm + couple_str)
        correct_confirms.append(correct_confirm + '[' + couple_str + ']')

        if confirm not in correct_confirms:
            raise Exception('Incorrect confirmation string')

        kill_symm_group(self.node, self.node.meta_session, couple)
        couple.destroy()

        return True

    @h.handler
    def get_next_group_number(self, request):
        groups_count = int(request)
        if groups_count < 0 or groups_count > 100:
            raise Exception('Incorrect groups count')

        try:
            max_group = int(self.node.meta_session.read_data(
                keys.MASTERMIND_MAX_GROUP_KEY).get()[0].data)
        except elliptics.NotFoundError:
            max_group = 0

        new_max_group = max_group + groups_count
        self.node.meta_session.write_data(
            keys.MASTERMIND_MAX_GROUP_KEY, str(new_max_group)).get()

        return range(max_group + 1, max_group + 1 + groups_count)

    @h.handler
    def get_config_remotes(self, request):
        nodes = config.get('elliptics', {}).get('nodes', []) or config["elliptics_nodes"]
        return tuple(nodes)

    def __get_couple(self, groups):
        couple_str = ':'.join(map(str, sorted(groups, key=lambda x: int(x))))
        try:
            couple = storage.couples[couple_str]
        except KeyError:
            raise ValueError('Couple %s not found' % couple_str)
        return couple

    def __sync_couple_meta(self, couple):

        key = keys.MASTERMIND_COUPLE_META_KEY % str(couple)

        try:
            meta = self.node.meta_session.read_latest(key).get()[0].data
        except elliptics.NotFoundError:
            meta = None
        couple.parse_meta(meta)

    def __update_couple_meta(self, couple):

        key = keys.MASTERMIND_COUPLE_META_KEY % str(couple)

        packed = msgpack.packb(couple.meta)
        logger.info('packed meta for couple %s: "%s"' % (couple, str(packed).encode('hex')))
        self.node.meta_session.write_data(key, packed).get()

        couple.update_status()

    ALPHANUM = 'a-zA-Z0-9'
    EXTRA = '\-_'
    NS_RE = re.compile('^[{alphanum}][{alphanum}{extra}]*[{alphanum}]$'.format(
        alphanum=ALPHANUM, extra=EXTRA))

    def valid_namespace(self, namespace):
        return self.NS_RE.match(namespace) is not None

    def validate_ns_settings(self, namespace, settings):

        groups_count = None
        if settings.get('groups-count'):
            groups_count = settings['groups-count']
            if groups_count <= 0:
                raise ValueError('groups-count should be positive integer')
        elif not settings.get('static-couple'):
            raise ValueError('groups-count should be set')

        try:
            port = settings['signature']['port'] = int(settings['signature']['port'])
            if not port > 0:
                raise ValueError
        except KeyError:
            pass
        except ValueError:
            raise ValueError('port should be positive integer')

        try:
            min_units = settings['min-units'] = int(settings['min-units'])
            if not min_units > 0:
                raise ValueError
        except KeyError:
            pass
        except ValueError:
            raise ValueError('min-units should be positive integer')

        try:
            content_length_threshold = settings['content_length_threshold'] = int(settings['content_length_threshold'])
            if not content_length_threshold > 0:
                raise ValueError
        except KeyError:
            pass
        except ValueError:
            raise ValueError('content length threshold should be positive integer')

        if settings.get('success-copies-num', '') not in ('any', 'quorum', 'all'):
            raise ValueError('success-copies-num allowed values are "any", '
                             '"quorum" and "all"')

        if settings.get('static-couple'):
            couple = settings['static-couple']
            groups = [storage.groups[g] for g in couple]
            ref_couple = groups[0].couple

            couple_checks = [g.couple and g.couple == ref_couple
                             for g in groups]
            logger.debug('Checking couple {0}: {1}'.format(
                couple, couple_checks))

            if (not ref_couple or not all(couple_checks)):
                raise ValueError('Couple {0} is not found'.format(couple))

            logger.debug('Checking couple {0} namespace'.format(couple))
            if ref_couple.namespace != namespace:
                raise ValueError('Couple {0} namespace is {1}, not {2}'.format(ref_couple,
                    ref_couple.namespace, namespace))

            for c in storage.couples:
                if c.namespace == namespace and c != ref_couple:
                    raise ValueError('Namespace "{0}" has several couples, '
                        'should have only 1 couple for static couple setting'.format(namespace))

            for g in ref_couple:
                if g not in groups:
                    raise ValueError('Using incomplete couple {0}, '
                        'full couple is {1}'.format(couple, ref_couple))

            if groups_count:
                if len(couple) != groups_count:
                    raise ValueError('Couple {0} does not have '
                        'length {1}'.format(couple, groups_count))
            else:
                groups_count = len(ref_couple.groups)

        settings['groups-count'] = groups_count

    ALLOWED_NS_KEYS = set(['success-copies-num', 'groups-count',
        'static-couple', 'auth-keys', 'signature', 'content_length_threshold',
        'storage-location', 'min-units'])
    ALLOWED_NS_SIGN_KEYS = set(['token', 'path_prefix', 'port'])
    ALLOWED_NS_AUTH_KEYS = set(['write', 'read'])

    def __merge_dict(self, dst, src):
        for k, val in src.iteritems():
            if not k in dst:
                dst[k] = val
            else:
                if not isinstance(val, dict):
                    dst[k] = val
                else:
                    self.__merge_dict(dst[k], src[k])

    def namespace_setup(self, request):
        try:
            namespace, overwrite, settings = request[:3]
        except Exception:
            raise ValueError('Invalid parameters')

        cur_settings = {}
        if not overwrite:
            try:
                self.infrastructure.sync_single_ns_settings(namespace)
                cur_settings = self.infrastructure.ns_settings[namespace]
            except elliptics.NotFoundError:
                pass
            except Exception as e:
                logger.error('Failed to update namespace {0} settings: '
                    '{1}\n{2}'.format(namespace, str(e), traceback.format_exc()))
                raise

        logger.info('Namespace {0}, old settings found: {1}, '
            'updating with {2}'.format(namespace, cur_settings, settings))

        self.__merge_dict(cur_settings, settings)
        settings = cur_settings

        # filtering settings
        for k in settings.keys():
            if k not in self.ALLOWED_NS_KEYS:
                del settings[k]
        for k in settings.get('signature', {}).keys():
            if k not in self.ALLOWED_NS_SIGN_KEYS:
                del settings['signature'][k]
        for k in settings.get('auth-keys', {}).keys():
            if k not in self.ALLOWED_NS_AUTH_KEYS:
                del settings['auth-keys'][k]

        if not namespace in self.__couple_namespaces():
            raise ValueError('Namespace "{0}" does not exist'.format(namespace))

        try:
            self.validate_ns_settings(namespace, settings)
        except Exception as e:
            logger.error(e)
            raise

        self.infrastructure.set_ns_settings(namespace, settings)

        return True

    def get_namespace_settings(self, request):
        try:
            namespace = request[0]
        except Exception:
            raise ValueError('Invalid parameters')

        if not namespace in self.__couple_namespaces() or not namespace in self.infrastructure.ns_settings:
            raise ValueError('Namespace "{0}" does not exist'.format(namespace))

        return self.infrastructure.ns_settings[namespace]

    def get_namespaces_settings(self, request):
        return self.infrastructure.ns_settings

    @h.handler
    def get_namespaces_statistics(self, request):
        per_dc_stat, per_ns_stat = self.statistics.per_entity_stat()
        ns_stats = {}
        for ns, stats in per_ns_stat.iteritems():
            ns_stats[ns] = self.statistics.total_stats(stats)
        return ns_stats

    @h.handler
    def freeze_couple(self, request):
        logger.info('freezing couple %s' % str(request))
        couple = self.__get_couple(request)

        self.__sync_couple_meta(couple)
        if couple.frozen:
            raise ValueError('Couple %s is already frozen' % couple)
        self.__do_set_meta_freeze(couple, freeze=True)

        return True

    @h.handler
    def unfreeze_couple(self, request):
        logger.info('unfreezing couple %s' % str(request))
        couple = self.__get_couple(request)

        self.__sync_couple_meta(couple)
        if not couple.frozen:
            raise ValueError('Couple %s is not frozen' % couple)
        self.__do_set_meta_freeze(couple, freeze=False)

        return True

    def __do_set_meta_freeze(self, couple, freeze):
        if freeze:
            couple.freeze()
        else:
            couple.unfreeze()
        self.__update_couple_meta(couple)

    @h.handler
    def get_namespaces(self, request):
        return tuple(self.__couple_namespaces().union(
                        self.infrastructure.ns_settings.keys()))

    def __couple_namespaces(self):
        namespaces = []
        for c in storage.couples:
            try:
                namespaces.append(c.namespace)
            except ValueError:
                pass
        return set(namespaces)


def handlers(b):
    handlers = []
    for attr_name in dir(b):
        attr = b.__getattribute__(attr_name)
        if not callable(attr) or attr_name.startswith('__'):
            continue
        handlers.append(attr)
    return handlers


def kill_symm_group(n, meta_session, couple):
    groups = [group.group_id for group in couple]
    logger.info('Killing symm groups: %s' % str(groups))
    s = elliptics.Session(n)
    wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
    s.set_timeout(wait_timeout)
    s.add_groups(groups)
    try:
        s.remove(keys.SYMMETRIC_GROUPS_KEY).get()
    except elliptics.NotFoundError:
        pass
    try:
        meta_session.remove(keys.MASTERMIND_COUPLE_META_KEY % str(couple)).get()
    except elliptics.NotFoundError:
        pass


def make_symm_group(n, couple, namespace):
    logger.info('writing couple info: ' + str(couple))
    logger.info('groups in couple %s are being assigned namespace "%s"' % (couple, namespace))

    s = elliptics.Session(n)
    wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
    s.set_timeout(config.get('wait_timeout', 5))
    good = []
    bad = ()
    for group in couple:
        try:
            packed = msgpack.packb(couple.compose_group_meta(namespace))
            logger.info('packed couple for group %d: "%s"' % (group.group_id, str(packed).encode('hex')))
            s.add_groups([group.group_id])
            s.write_data(keys.SYMMETRIC_GROUPS_KEY, packed).get()
            group.parse_meta(packed)
            good.append(group.group_id)
        except Exception as e:
            logger.error('Failed to write symm group info, group %d: %s\n%s'
                         % (group.group_id, str(e), traceback.format_exc()))
            bad = (group.group_id, str(e))
            break
    return (good, bad)
