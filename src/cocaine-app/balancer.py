# encoding: utf-8
from collections import defaultdict
import copy
from datetime import datetime
import itertools
import json
import logging
import operator
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
import inventory
import keys
import statistics
import storage
from sync import sync_manager
from sync.error import LockFailedError


logger = logging.getLogger('mm.balancer')

logger.info('balancer.py')


class Balancer(object):

    DT_FORMAT = '%Y-%m-%d %H:%M:%S'
    MIN_NS_UNITS = config.get('balancer_config', {}).get('min_units', 1)

    CLUSTER_CHANGES_LOCK = 'cluster'

    def __init__(self, n):
        self.node = n
        self.infrastructure = None
        self.statistics = statistics.Statistics(self)
        self.niu = None

    def _set_infrastructure(self, infrastructure):
        self.infrastructure = infrastructure

    @h.concurrent_handler
    def get_symmetric_groups(self, request):
        result = self._good_couples()
        logger.debug('good_symm_groups: ' + str(result))
        return result

    def _good_couples(self):
        return [couple.as_tuple() for couple in storage.couples if couple.status == storage.Status.OK]

    @h.concurrent_handler
    def get_bad_groups(self, request):
        result = [couple.as_tuple() for couple in storage.couples if couple.status not in storage.NOT_BAD_STATUSES]
        logger.debug('bad_symm_groups: ' + str(result))
        return result

    @h.concurrent_handler
    def get_frozen_groups(self, request):
        result = self._frozen_couples()
        logger.debug('frozen_couples: ' + str(result))
        return result

    def _frozen_couples(self):
        return [couple.as_tuple() for couple in storage.couples if couple.status == storage.Status.FROZEN]

    @h.concurrent_handler
    def get_closed_groups(self, request):
        result = self._closed_couples()

        logger.debug('closed couples: ' + str(result))
        return result

    def _closed_couples(self):
        return [couple.as_tuple() for couple in storage.couples
                if couple.status == storage.Status.FULL]

    @h.concurrent_handler
    def get_empty_groups(self, request):
        options = request and request[0] or {}

        result = self._empty_group_ids(
            in_service=options.get('in_service', False),
            status=storage.Status.BROKEN if options.get('state') == 'bad' else storage.Status.INIT)
        logger.debug('uncoupled groups: ' + str(result))
        return result

    def _empty_group_ids(self, in_service=False, status=storage.Status.INIT):
        return [group.group_id
                for group in infrastructure.infrastructure.get_good_uncoupled_groups(
                    including_in_service=in_service,
                    status=status)]

    STATES = {
        'good': [storage.Status.OK],
        'full': [storage.Status.FULL],
        'frozen': [storage.Status.FROZEN],
        'bad': [storage.Status.INIT, storage.Status.BAD],
        'broken': [storage.Status.BROKEN]
    }

    @h.concurrent_handler
    def get_couples_list(self, request):
        options = request[0]
        return self._get_couples_list(options)

    def _get_couples_list(self, options):
        couples = storage.couples.keys()

        if options.get('namespace', None):

            def f(c):
                try:
                    return c.namespace == options['namespace']
                except ValueError:
                    return False

            couples = filter(f, couples)

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

    @h.concurrent_handler
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

    @h.concurrent_handler
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

    @h.concurrent_handler
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

    @h.concurrent_handler
    def get_group_weights(self, request):
        namespaces = {}
        all_symm_group_objects = []

        try:
            ns = request[0]
        except IndexError:
            ns = None

        if ns and not ns in self.infrastructure.ns_settings:
            raise ValueError('Namespace "{0}" does not exist'.format(ns))

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

        result = {}

        namespaces = ([(ns, namespaces.get(ns, set()))]
                      if ns is not None else
                      namespaces.iteritems())

        for namespace, sizes in namespaces:
            try:
                result[namespace] = self.__namespaces_weights(
                    namespace, sizes, symm_groups=all_symm_group_objects)
            except ValueError as e:
                logger.error(e)
                continue

        if len(result) == 0:
            raise ValueError('Failed to satisfy {0} availability settings'.format(
                'namespace ' + ns if ns else 'all namespaces'))

        logger.info(str(result))
        return result

    def __namespaces_weights(self, namespace, sizes, symm_groups=[]):

        found_couples = 0

        ns_weights = {}

        # TODO: remove this crutch when get_group_weights becomes obsolete
        if isinstance(sizes, set):
            sizes = dict([(size, symm_groups) for size in sizes])

        for size, symm_groups in sizes.iteritems():
            try:
                logger.info('Namespace {0}, size {1}: calculating '
                    'cluster info'.format(namespace, size))
                (group_weights, info) = balancelogic.rawBalance(
                    symm_groups, bla.getConfig(),
                    bla._and(bla.GroupSizeEquals(size),
                             bla.GroupNamespaceEquals(namespace)))
                ns_size_weights = \
                    [([g.group_id for g in item[0].groups],) +
                         item[1:] +
                         (int(item[0].get_stat().free_space),)
                     for item in group_weights.items()]
                if len(ns_size_weights):
                    ns_weights[size] = ns_size_weights
                    found_couples += len([item for item in ns_weights[size] if item[1] > 0])
                logger.info('Namespace {0}, size {1}: '
                    'cluster info: {2}'.format(namespace, size, info))
            except Exception as e:
                logger.error('Namespace {0}, size {1}: error {2}'.format(namespace, size, e))
                continue

        ns_min_units = self.infrastructure.ns_settings.get(namespace, {}).get(
            'min-units', self.MIN_NS_UNITS)
        if found_couples < ns_min_units:
            raise ValueError('Namespace {0} has {1} available couples, '
                '{2} required'.format(namespace, found_couples, ns_min_units))
        return ns_weights

    @h.concurrent_handler
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

        namespaces = [g.meta['namespace'] for g in couple if g.meta and g.group_id != group_id]
        if namespaces and not all(ns == namespaces[0] for ns in namespaces):
            logger.error('Balancer error: namespaces of groups coupled with group %d are not the same: %s' % (group_id, namespaces))
            return {'Balancer error': 'namespaces of groups coupled with group %d are not the same: %s' % (group_id, namespaces)}

        namespace_to_use = namespaces and namespaces[0] or force_namespace
        if not namespace_to_use:
            logger.error('Balancer error: cannot identify a namespace to use for group %d' % (group_id,))
            return {'Balancer error': 'cannot identify a namespace to use for group %d' % (group_id,)}

        frozen = any([g.meta.get('frozen') for g in couple if g.meta and g.group_id != group_id])

        make_symm_group(self.node, couple, namespace_to_use, frozen)
        couple.update_status()

        return {'message': 'Successfully repaired couple', 'couple': str(couple)}

    @h.concurrent_handler
    def get_group_info(self, request):
        group = int(request)
        logger.info('get_group_info: request: %s' % (str(request),))

        if not group in storage.groups:
            raise ValueError('Group %d is not found' % group)

        logger.info('Group %d: %s' % (group, repr(storage.groups[group])))

        return storage.groups[group].info()

    @h.concurrent_handler
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

    @h.concurrent_handler
    def group_detach_node(self, request):
        group_id = int(request[0])
        node_backend_str = request[1]

        group = (group_id in storage.groups and
                 storage.groups[group_id] or
                 None)
        node_backend = (node_backend_str in storage.node_backends and
                        storage.node_backends[node_backend_str] or
                        None)

        logger.info('Node backend: {0}'.format(node_backend))
        try:
            host, port, backend_id = self.NODE_BACKEND_RE.match(node_backend_str).groups()
            port, backend_id = int(port), int(backend_id)
            logger.info('host, port, backend_id: {0}'.format((host, port, backend_id)))
        except (IndexError, ValueError, AttributeError):
            raise ValueError('Node backend should be of form <host>:<port>/<backend_id>')

        if group and node_backend and node_backend in group.node_backends:
            logger.info('Removing node backend {0} from group {1} nodes'.format(node_backend, group))
            group.remove_node_backend(node_backend)
            group.update_status_recursive()
            logger.info('Removed node backend {0} from group {1} nodes'.format(node_backend, group))

        logger.info('Removing node backend {0} from group {1} history'.format(node_backend_str, group_id))
        try:
            self.infrastructure.detach_node(group_id, host, port, backend_id)
            logger.info('Removed node backend {0} from group {1} history'.format(node_backend_str, group_id))
        except Exception as e:
            logger.error('Failed to remove {0} from group {1} history: {2}'.format(node_backend_str, group_id, str(e)))
            raise

        return True

    @h.concurrent_handler
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

    def __update_cluster_state(self, namespace=None):
        logger.info('Starting concurrent cluster info update')
        self.niu._force_nodes_update()
        if namespace:
            infrastructure.infrastructure.sync_single_ns_settings(namespace)
        logger.info('Concurrent cluster info update completed')

    def __groups_by_total_space(self, match_group_space):
        suitable_groups = []
        total_spaces = []

        for group in infrastructure.infrastructure.get_good_uncoupled_groups():

            if not len(group.node_backends):
                logger.info('Group {0} cannot be used, it has '
                    'empty node list'.format(group.group_id))
                continue

            if group.status != storage.Status.INIT:
                logger.info('Group {0} cannot be used, status is {1}, '
                    'should be {2}'.format(group.group_id, group.status, storage.Status.INIT))
                continue

            suitable = True
            for node_backend in group.node_backends:
                if node_backend.status != storage.Status.OK:
                    logger.info('Group {0} cannot be used, node backend {1} status '
                                'is {2} (not OK)'.format(group.group_id,
                                     node_backend, node_backend.status))
                    suitable = False
                    break

            if not suitable:
                continue

            suitable_groups.append(group.group_id)
            total_spaces.append(group.get_stat().total_space)

        groups_by_total_space = {}

        if match_group_space:
            # bucketing groups by approximate total space
            ts_tolerance = config.get('total_space_diff_tolerance', 0.05)
            cur_ts_key = 0
            for ts in reversed(sorted(total_spaces)):
                if abs(cur_ts_key - ts) > cur_ts_key * ts_tolerance:
                    cur_ts_key = ts
                    groups_by_total_space[cur_ts_key] = []

            total_spaces = list(reversed(sorted(groups_by_total_space.keys())))
            logger.info('group total space sizes available: {0}'.format(total_spaces))

            for group_id in suitable_groups:
                group = storage.groups[group_id]
                ts = group.get_stat().total_space
                for ts_key in total_spaces:
                    if ts_key - ts < ts_key * ts_tolerance:
                        groups_by_total_space[ts_key].append(group_id)
                        break
                else:
                    raise ValueError('Failed to find total space key for group {0}, '
                        'total space {1}'.format(group_id, ts))
        else:
            groups_by_total_space['any'] = [group_id for group_id in suitable_groups]

        return groups_by_total_space

    def __couple_groups(self, size, couples, options, ns, groups_by_total_space):

        created_couples = []
        error = None

        try:
            tree, nodes = self.infrastructure.filtered_cluster_tree(self.NODE_TYPES)
            self.infrastructure.account_ns_couples(tree, nodes, ns)

            units = self.infrastructure.groups_units(
                [storage.groups[group_id]
                    for group_ids in groups_by_total_space.itervalues()
                    for group_id in group_ids],
                self.NODE_TYPES)

            for _, mandatory_groups in itertools.izip_longest(
                    xrange(couples), options['mandatory_groups'][:couples]):

                mandatory_groups = mandatory_groups or []

                if len(mandatory_groups) > size:
                    raise ValueError("Mandatory groups list's {0} length "
                        "is greater than couple size {1}".format(mandatory_groups, size))

                for m_group in mandatory_groups:
                    if m_group not in units:
                        raise ValueError('Mandatory group {0} is either not found '
                            'in cluster, is not uncoupled, '
                            'is located on a locked host or '
                            'is unsuitable in some other way'.format(m_group))

                if mandatory_groups:
                    self.infrastructure.account_ns_groups(nodes, [storage.groups[g] for g in mandatory_groups])
                    self.infrastructure.update_groups_list(tree)

                ns_current_state = self.infrastructure.ns_current_state(nodes, self.NODE_TYPES[1:])
                groups_to_couple = self.__choose_groups_to_couple(
                    ns_current_state, units, size, groups_by_total_space, mandatory_groups)
                if not groups_to_couple:
                    logger.warn('Not enough uncoupled groups to couple')
                    break
                logger.info('Chosen groups to couple: {0}'.format(groups_to_couple))

                couple = storage.couples.add([storage.groups[g]
                                              for g in groups_to_couple])
                if not options['dry_run']:
                    try:
                        make_symm_group(self.node, couple, options['namespace'],
                            options['init_state'] == storage.Status.FROZEN)
                    except Exception as e:
                        error = e
                        couple.destroy()
                        break
                    couple.update_status()

                for ts, group_ids in groups_by_total_space.iteritems():
                    if couple.groups[0].group_id in group_ids:
                        for group in couple.groups:
                            group_ids.remove(group.group_id)
                        break

                self.infrastructure.account_ns_groups(nodes, couple.groups)
                self.infrastructure.update_groups_list(tree)

                created_couples.append(couple)
        except Exception as e:
            logger.error('Failed to build couples: {0}\n{1}'.format(
                e, traceback.format_exc()))
            error = e
            if options['dry_run']:
                for couple in created_couples:
                    couple.destroy()
            raise

        res = [c.as_tuple() for c in created_couples]

        if options['dry_run']:
            for couple in created_couples:
                couple.destroy()

        return res, error

    NODE_TYPES = ['root'] + inventory.get_balancer_node_types() + ['hdd']
    DC_NODE_TYPE = inventory.get_dc_node_type()


    def __weight_combination(self, ns_current_type_state, comb):
        comb_groups_count = copy.copy(ns_current_type_state['nodes'])
        for selected_units in comb:
            for unit in selected_units:
                comb_groups_count[unit] += 1
        return sum((c - ns_current_type_state['avg']) ** 2
                   for c in comb_groups_count.values())

    def __weight_couple_groups(self, ns_current_state, units, group_ids):
        weight = []
        for node_type in self.NODE_TYPES[1:]:
            node_groups = {}
            comb = []
            for group_id in group_ids:
                ng_keys = tuple(gu[node_type] for gu in units[group_id])
                comb.append(ng_keys)

            weight.append(self.__weight_combination(
                ns_current_state[node_type],
                comb))

        return weight

    def __choose_groups(self, ns_current_state, units, count, group_ids, levels, mandatory_groups):
        levels = levels[1:]
        node_type = levels[0]
        logger.info('Selecting {0} groups on level {1} among groups {2}'.format(
            count, node_type, group_ids))

        if count == 0:
            return []

        if len(group_ids) < count:
            logger.warn('Not enough groups for choosing on level {0}: '
                '{1} uncoupled, {2} needed'.format(node_type, len(group_ids), count))
            return []

        groups_by_level_units = {}
        for group_id in group_ids:
            level_units = tuple(gp[node_type] for gp in units[group_id])
            groups_by_level_units.setdefault(level_units, []).append(group_id)

        logger.info('Level {0} current state: avg {1}, nodes {2}'.format(node_type,
            ns_current_state[node_type]['avg'], ns_current_state[node_type]['nodes']))
        choice_list = []
        for choice, groups in groups_by_level_units.iteritems():
            choice_list.extend([choice] * min(count, len(groups)))

        logger.info('Nodes type: {0}, choice list: {1}'.format(node_type, choice_list))

        weights = {}
        mandatory_groups_units = []
        for group_id in mandatory_groups:
            level_units = [gp[node_type] for gp in units[group_id]]
            mandatory_groups_units.extend(level_units)

        comb_set = set()
        for c in itertools.combinations(choice_list, count):
            comb_set.add(c)

        for comb in comb_set:
            if config.get('forbidden_dc_sharing_among_groups', False) and node_type == self.DC_NODE_TYPE:
                comb_units = list(reduce(operator.add, comb))
                if (len(comb_units + mandatory_groups_units) !=
                    len(set(comb_units) | set(mandatory_groups_units))):
                        continue
            weights[comb] = self.__weight_combination(ns_current_state[node_type], comb)

        if not weights:
            logger.warn('Not enough groups for choosing on level {0}: '
                'could not find groups satisfying restrictions'.format(node_type))
            return []

        logger.info('Combination weights: {0}'.format(weights))
        sorted_weights = sorted(weights.items(), key=lambda x: x[1])

        logger.info('Least weight combination: {0}'.format(sorted_weights[0]))

        node_counts = {}
        for node in sorted_weights[0][0]:
            node_counts.setdefault(node, 0)
            node_counts[node] += 1

        logger.info('Level {0}: selected units: {1}'.format(node_type, node_counts))

        if len(levels) == 1:
            groups = reduce(
                operator.add,
                (groups_by_level_units[level_units][:_count]
                     for level_units, _count in node_counts.iteritems()),
                [])
        else:
            groups = reduce(
                operator.add,
                (self.__choose_groups(ns_current_state, units, _count,
                                      groups_by_level_units[level_units],
                                      levels, mandatory_groups)
                     for level_units, _count in node_counts.iteritems()),
                [])

        if len(groups) < count:
            logger.warn('Not enough groups for choosing on level {0}: '
                'could not find groups satisfying restrictions, '
                'got {1} groups, expected {2}'.format(
                    node_type, len(groups), count))
            return []

        return groups



    def __choose_groups_to_couple(self, ns_current_state, units, count, groups_by_total_space, mandatory_groups):

        candidates = []
        for ts, group_ids in groups_by_total_space.iteritems():
            if not all([mg in group_ids for mg in mandatory_groups]):
                logger.debug('Could not find mandatory groups {0} in a list '
                    'of groups with ts {1}'.format(mandatory_groups, ts))
                continue

            free_group_ids = [g for g in group_ids if g not in mandatory_groups]

            candidate = self.__choose_groups(
                ns_current_state, units, count - len(mandatory_groups),
                free_group_ids, self.NODE_TYPES, mandatory_groups)
            candidate += mandatory_groups
            if candidate:
                candidates.append(candidate)

        if not candidates:
            return None

        candidate = candidates[0]

        if len(candidates) > 1:
            weights = [(self.__weight_couple_groups(ns_current_state, units, c), c) for c in candidates]
            weights.sort()
            logger.info('Choosing candidate with least weight: {0}'.format(weights))
            candidate = weights[0][1]

        return candidate

    @h.concurrent_handler
    def build_couples(self, request):
        logger.info('----------------------------------------')
        logger.info('New build couple request: ' + str(request))

        size = int(request[0])
        couples = int(request[1])

        try:
            options = request[2]
            options['mandatory_groups'] = [[int(g) for g in mg]
                for mg in options.get('mandatory_groups', [])]
        except IndexError:
            options = {}

        options.setdefault('namespace', storage.Group.DEFAULT_NAMESPACE)
        options.setdefault('match_group_space', True)
        options.setdefault('init_state', storage.Status.COUPLED)
        options.setdefault('dry_run', False)
        options.setdefault('mandatory_groups', [])

        options['init_state'] = options['init_state'].upper()
        if not options['init_state'] in self.VALID_COUPLE_INIT_STATES:
            raise ValueError('Couple "{0}" init state is invalid'.format(options['init_state']))

        ns = options['namespace']
        logger.info('namespace from request: {0}'.format(ns))

        self.__check_namespace(ns)

        with sync_manager.lock(self.CLUSTER_CHANGES_LOCK, blocking=False):

            logger.info('Updating cluster info')
            self.__update_cluster_state(namespace=options['namespace'])
            logger.info('Updating cluster info completed')

            groups_by_total_space = self.__groups_by_total_space(
                options['match_group_space'])

            logger.info('groups by total space: {0}'.format(groups_by_total_space))

            res, error = self.__couple_groups(size, couples, options, ns, groups_by_total_space)

        return (res, str(error) if error else None)

    @h.concurrent_handler
    def break_couple(self, request):
        logger.info('----------------------------------------')
        logger.info('New break couple request: ' + str(request))

        with sync_manager.lock(self.CLUSTER_CHANGES_LOCK, blocking=False):

            logger.info('Updating cluster info')
            self.__update_cluster_state()
            logger.info('Updating cluster info completed')

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

    @h.concurrent_handler
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

    @h.concurrent_handler
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

    ALPHANUM = 'a-zA-Z0-9'
    EXTRA = '\-_'
    NS_RE = re.compile('^[{alphanum}][{alphanum}{extra}]*[{alphanum}]$'.format(
        alphanum=ALPHANUM, extra=EXTRA))

    def __valid_namespace(self, namespace):
        return self.NS_RE.match(namespace) is not None

    def __validate_ns_settings(self, namespace, settings):

        groups_count = None
        if settings.get('groups-count'):
            groups_count = settings['groups-count']
            if groups_count <= 0:
                raise ValueError('groups-count should be positive integer')
        elif not settings.get('static-couple'):
            raise ValueError('groups-count should be set')

        try:
            min_units = settings['min-units'] = int(settings['min-units'])
            if not min_units > 0:
                raise ValueError
        except KeyError:
            pass
        except ValueError:
            raise ValueError('min-units should be positive integer')

        try:
            settings['check-for-update'] = bool(settings['check-for-update'])
        except KeyError:
            pass
        except (TypeError, ValueError):
            raise ValueError('check-for-update should be boolean')

        try:
            content_length_threshold = settings['redirect']['content-length-threshold'] = int(settings['redirect']['content-length-threshold'])
            if not content_length_threshold >= -1:
                raise ValueError
        except KeyError:
            pass
        except ValueError:
            raise ValueError('redirect content length threshold should be non-negative integer or -1')

        try:
            expire_time = settings['redirect']['expire-time'] = int(settings['redirect']['expire-time'])
            if not expire_time > 0:
                raise ValueError
        except KeyError:
            pass
        except ValueError:
            raise ValueError('redirect expire time should be positive integer')

        try:
            reserved_space_percentage = settings['reserved-space-percentage'] = float(settings['reserved-space-percentage'])
            if not 0.0 <= reserved_space_percentage <= 1.0:
                raise ValueError
        except KeyError:
            pass
        except ValueError:
            raise ValueError('reserved-space-percentage should be a float in interval [0.0, 1.0]')

        if settings.get('success-copies-num', '') not in ('any', 'quorum', 'all'):
            raise ValueError('success-copies-num allowed values are "any", '
                             '"quorum" and "all"')

        if 'auth-keys' in settings:
            auth_keys_settings = settings['auth-keys']
            if not 'read' in auth_keys_settings:
                auth_keys_settings['read'] = ''
            if not 'write' in auth_keys_settings:
                auth_keys_settings['write'] = ''

        keys = (settings.get('redirect', {}).get('expire-time'),
                settings.get('signature', {}).get('token'),
                settings.get('signature', {}).get('path_prefix'))

        if not all(keys) and any(keys):
            raise ValueError('Signature token, signature path prefix '
                'and redirect expire time should be set simultaneously')

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
        'static-couple', 'auth-keys', 'signature', 'redirect',
        'min-units', 'features', 'reserved-space-percentage',
        'check-for-update'])
    ALLOWED_NS_SIGN_KEYS = set(['token', 'path_prefix'])
    ALLOWED_NS_AUTH_KEYS = set(['write', 'read'])
    ALLOWED_REDIRECT_KEYS = set(['content-length-threshold', 'expire-time'])

    def __merge_dict(self, dst, src):
        for k, val in src.iteritems():
            if not k in dst:
                dst[k] = val
            else:
                if not isinstance(val, dict):
                    dst[k] = val
                else:
                    self.__merge_dict(dst[k], src[k])

    @h.concurrent_handler
    def namespace_setup(self, request):
        try:
            namespace, overwrite, settings = request[:3]
        except Exception:
            raise ValueError('Invalid parameters')

        try:
            options = request[3]
        except IndexError:
            options = {}

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

        if cur_settings.get('__service', {}).get('is_deleted'):
            logger.info('Namespace {0} is deleted, will not merge old settings '
                'with new ones'.format(namespace))
            cur_settings = {}

        if options.get('json'):
            try:
                settings = json.loads(settings)
                logger.info('Namespace {0}: input settings {1}'.format(namespace, settings))
            except Exception as e:
                logger.error('Namespace {0}, invalid json settings: {1}'.format(namespace, e))
                raise ValueError('Invalid json settings')

        logger.info('Namespace {0}, old settings found: {1}, '
            'updating with {2}'.format(namespace, cur_settings, settings))

        self.__merge_dict(cur_settings, settings)

        if not self.__valid_namespace(namespace):
            raise ValueError('Namespace "{0}" is invalid'.format(namespace))

        settings = cur_settings

        if not options.get('skip_validation'):

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
            for k in settings.get('redirect', {}).keys():
                if k not in self.ALLOWED_REDIRECT_KEYS:
                    del settings['redirect'][k]

            try:
                self.__validate_ns_settings(namespace, settings)
            except Exception as e:
                logger.error(e)
                raise

        self.infrastructure.set_ns_settings(namespace, settings)

        logger.info('Namespace {0}, settings set to {1}'.format(namespace, settings))

        return True

    def __check_namespace(self, namespace):
        if not namespace in self.infrastructure.ns_settings:
            raise ValueError('Namespace "{0}" does not exist'.format(namespace))
        else:
            logger.info('Current namespace {0} settings: {1}'.format(namespace, self.infrastructure.ns_settings[namespace]))
            if self.infrastructure.ns_settings[namespace]['__service'].get('is_deleted'):
                raise ValueError('Namespace "{0}" is deleted'.format(namespace))

    @h.concurrent_handler
    def namespace_delete(self, request):
        try:
            namespace = request[0]
        except Exception:
            raise ValueError('Namespace is required')

        with sync_manager.lock(self.CLUSTER_CHANGES_LOCK, blocking=False):

            logger.info('Updating cluster info')
            self.__update_cluster_state(namespace=namespace)
            logger.info('Updating cluster info completed')

            self.__check_namespace(namespace)

            for couple in storage.couples:
                try:
                    ns = couple.namespace
                except ValueError:
                    continue
                if ns == namespace:
                    raise ValueError('Namespace {0} has couples ({1})'.format(namespace, couple))

            try:
                settings = self.infrastructure.ns_settings[namespace]

                settings.setdefault('__service', {})
                settings['__service']['is_deleted'] = True

                self.infrastructure.set_ns_settings(namespace, settings)
            except Exception as e:
                logger.error('Failed to delete namespace {0}: '
                    '{1}\n{2}'.format(namespace, str(e), traceback.format_exc()))
                raise

        return True

    @h.concurrent_handler
    def get_namespace_settings(self, request):
        try:
            namespace = request[0]
        except Exception:
            raise ValueError('Invalid parameters')

        try:
            options = request[1]
        except IndexError:
            options = {}

        try:
            self.__check_namespace(namespace)
        except ValueError:
            if (namespace not in self.infrastructure.ns_settings or
                not options.get('deleted')):
                    raise

        return self.infrastructure.ns_settings[namespace]

    @h.concurrent_handler
    def get_namespaces_settings(self, request):
        return self.infrastructure.ns_settings

    @h.concurrent_handler
    def get_namespaces_statistics(self, request):
        return self.statistics.per_ns_statistics()

    @h.concurrent_handler
    def freeze_couple(self, request):
        logger.info('freezing couple %s' % str(request))
        couple = self.__get_couple(request)

        if couple.frozen:
            raise ValueError('Couple {0} is already frozen'.format(couple))

        self.__do_set_meta_freeze(couple, freeze=True)
        couple.update_status()

        return True

    @h.concurrent_handler
    def unfreeze_couple(self, request):
        logger.info('unfreezing couple %s' % str(request))
        couple = self.__get_couple(request)

        if not couple.frozen:
            raise ValueError('Couple {0} is not frozen'.format(couple))

        self.__do_set_meta_freeze(couple, freeze=False)
        couple.update_status()

        return True

    def __do_set_meta_freeze(self, couple, freeze):

        group_meta = couple.compose_group_meta(couple.namespace, frozen=freeze)

        packed = msgpack.packb(group_meta)
        logger.info('packed meta for couple {0}: "{1}"'.format(
            couple, str(packed).encode('hex')))

        s = elliptics.Session(self.node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
        s.set_timeout(wait_timeout)
        s.add_groups([group.group_id for group in couple])

        _, failed_groups = h.write_retry(s, keys.SYMMETRIC_GROUPS_KEY, packed)

        if failed_groups:
            s = 'Failed to write meta key for couple {0} to groups {1}'.format(
                    couple, list(failed_groups))
            logger.error(s)
            raise RuntimeError(s)

        try:
            for group in couple:
                group.parse_meta(packed)
        except Exception as e:
            logging.error('Failed to parse meta key for groups {0}: {1}'.format(
                [g.group_id for g in couple.groups], e))
            raise

    @h.concurrent_handler
    def get_namespaces(self, request):
        return self.infrastructure.ns_settings.keys()

    @h.concurrent_handler
    def get_namespaces_states(self, request):
        default = lambda: {
            'settings': {},
            'couples': [],
            'weights': {},
            'statistics': {},
        }

        res = defaultdict(default)

        # settings
        ns_settings = self.infrastructure.ns_settings
        for ns, settings in ns_settings.items():
            res[ns]['settings'] = settings

        # couples
        symm_groups = {}
        for couple in storage.couples:
            try:
                try:
                    ns = couple.namespace
                except ValueError:
                    continue
                info = couple.info()
                info['groups'] = [g.info() for g in couple]
                # couples
                res[ns]['couples'].append(info)

                symm_groups.setdefault(couple.namespace, {})
                symm_groups[couple.namespace].setdefault(len(couple), [])

                if couple.status not in storage.GOOD_STATUSES:
                    continue

                symm_groups[couple.namespace][len(couple)].append(bla.SymmGroup(couple))
            except Exception as e:
                logger.error('Failed to include couple {0} in namespace '
                    'states: {1}'.format(str(couple), e))
                continue

        # weights
        for ns, sizes in symm_groups.iteritems():
            try:
                # TODO: convert size inside of __namespaces_weights function
                # when get_groups_weights handle is gone
                res[ns]['weights'] = dict((str(k), v)
                    for k, v in self.__namespaces_weights(ns, sizes).iteritems())
            except ValueError as e:
                logger.error(e)
                continue
            except Exception as e:
                logger.error('Failed to construct namespace {0} weights: {1}'.format(ns, e))
                continue

        # statistics
        for ns, stats in self.statistics.per_ns_statistics().iteritems():
            res[ns]['statistics'] = stats

        return dict(res)

    @h.concurrent_handler
    def storage_keys_diff(self, request):
        couples_diff = {}
        for couple in storage.couples:
            group_keys = []
            for group in couple.groups:
                if not len(group.node_backends):
                    continue
                group_keys.append(group.get_stat().files)
            if not group_keys:
                continue
            group_keys.sort(reverse=True)
            couples_diff[str(couple)] = sum([group_keys[0] - gk for gk in group_keys[1:]])
        return {'couples': couples_diff,
                'total_keys_diff': sum(couples_diff.itervalues())}


def handlers(b):
    handlers = []
    try:
        private_prefix = '_' + Balancer.__name__ + '__'
        for attr_name in dir(b):
            attr = b.__getattribute__(attr_name)
            if (not callable(attr) or
                attr.__name__.startswith('_') or
                attr_name.startswith('__') or
                attr_name.startswith(private_prefix)):
                    continue
            logger.info('adding handler: attr_name: {0}, attr.__name__ {1}'.format(attr_name, attr.__name__))
            handlers.append(attr)
    except Exception as e:
        logger.error('handler exception: {0}'.format(e))
        pass
    return handlers


def consistent_write(session, key, data, retries=3):
    s = session.clone()

    key_esc = key.replace('\0', '\\0')

    groups = set(s.groups)

    logger.debug('Performing consistent write of key {0} to groups {1}'.format(
        key_esc, list(groups)))

    suc_groups, failed_groups = h.write_retry(s, key, data, retries=retries)

    if failed_groups:
        # failed to write key to all destination groups

        logger.info('Failed to write key consistently, '
            'removing key {0} from groups {1}'.format(
                key_esc, list(suc_groups)))

        s.set_groups(suc_groups)
        _, left_groups = h.remove_retry(s, key, retries=retries)

        if left_groups:
            logger.error('Failed to remove key {0} from groups {1}'.format(
                key_esc, list(left_groups)))
        else:
            logger.info('Successfully removed key {0} from groups {1}'.format(
                key_esc, list(suc_groups)))

        raise RuntimeError('Failed to write key {0} to groups {1}'.format(
            key_esc, list(failed_groups)))


def kill_symm_group(n, meta_session, couple):
    groups = [group.group_id for group in couple]
    logger.info('Killing symm groups: %s' % str(groups))
    s = elliptics.Session(n)
    wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
    s.set_timeout(wait_timeout)
    s.add_groups(groups)

    _, failed_groups = h.remove_retry(s, keys.SYMMETRIC_GROUPS_KEY)

    if failed_groups:
        s = 'Failed to remove couple {0} meta key for from groups {1}'.format(
                couple, list(failed_groups))
        logger.error(s)
        raise RuntimeError(s)


def make_symm_group(n, couple, namespace, frozen):
    logger.info('Writing meta key for couple {0}, assigning namespace'.format(couple, namespace))

    s = elliptics.Session(n)
    wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
    s.set_timeout(config.get('wait_timeout', 5))

    s.add_groups([g.group_id for g in couple.groups])
    packed = msgpack.packb(couple.compose_group_meta(namespace, frozen))
    try:
        consistent_write(s, keys.SYMMETRIC_GROUPS_KEY, packed)
    except Exception as e:
        logger.error('Failed to write meta key for couple {0}: {1}\n{2}'.format(
                     couple, str(e), traceback.format_exc()))
        raise

    try:
        for group in couple:
            group.parse_meta(packed)
    except Exception as e:
        logging.error('Failed to parse meta key for groups {0}: {1}'.format(
            [g.group_id for g in couple.groups], e))
        raise

    return
