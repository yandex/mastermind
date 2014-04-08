# encoding: utf-8
import copy
from datetime import datetime
import logging
import re
import sys
import traceback

from cocaine.worker import Worker
import elliptics
import msgpack

import balancelogicadapter as bla
import balancelogic
from compat import EllAsyncResult, EllReadResult, EllLookupResult
from config import config
import helpers as h
import indexes
import infrastructure
import keys
import storage


logger = logging.getLogger('mm.balancer')

logger.info('balancer.py')


class Balancer(object):

    DT_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, n):
        self.node = n
        self.infrastructure = None
        self.ns_settings_idx = \
            indexes.SecondaryIndex(keys.MM_NAMESPACE_SETTINGS_IDX,
                                   keys.MM_NAMESPACE_SETTINGS_KEY_TPL,
                                   self.node.meta_session)

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
        s.set_timeout(config.get('wait_timeout', 5))
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
                'nodes': [n.info() for n in group.nodes],
            }
            for node in group_data['nodes']:
                node['path'] = infrastructure.port_to_path(
                    int(node['addr'].split(':')[1]))
            if group.couple:
                group_data.update({
                    'couple': str(group.couple),
                    'couple_status': group.couple.status})

            if not group.nodes:
                dc_groups = groups_by_dcs.setdefault('unknown', {})
                dc_groups[group.group_id] = group_data
                continue

            for node in group.nodes:
                dc_groups = groups_by_dcs.setdefault(node.host.dc, {})
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
                'nodes': [n.info() for g in couple for n in g.nodes]
            }
            couples_by_nss.setdefault(couple.namespace, []).append(couple_data)

        return couples_by_nss

    @h.handler
    def get_group_weights(self, request):
        namespaces = {}
        all_symm_group_objects = []
        for couple in storage.couples:
            if couple.status not in storage.GOOD_STATUSES:
                continue

            symm_group = bla.SymmGroup(couple)
            namespaces.setdefault(couple.namespace, set())
            namespaces[couple.namespace].add(len(couple))
            all_symm_group_objects.append(symm_group)
            # logger.debug(str(symm_group))

        result = {}

        for namespace, sizes in namespaces.iteritems():
            for size in sizes:
                (group_weights, info) = balancelogic.rawBalance(all_symm_group_objects,
                                                                bla.getConfig(),
                                                                bla._and(bla.GroupSizeEquals(size),
                                                                         bla.GroupNamespaceEquals(namespace)))
                result.setdefault(namespace, {})[size] = \
                    [([g.group_id for g in item[0].groups],) +
                         item[1:] +
                         (int(item[0].get_stat().free_space),)
                     for item in group_weights.items()]
                logger.info('Cluster info: ' + str(info))

        logger.info(str(result))
        return result

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
            raise bad[1]

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

    @h.handler
    def group_detach_node(self, request):
        group_id = int(request[0])
        node_str = request[1]

        if not group_id in storage.groups:
            raise ValueError('Group %d is not found' % group_id)

        group = storage.groups[group_id]
        node = node_str in storage.nodes and storage.nodes[node_str] or None
        try:
            host, port = node_str.split(':')
            port = int(port)
        except ValueError:
            raise ValueError('Node should have form <host>:<port>')

        if node and node in group.nodes:
            logger.info('Removing node {0} from group {1} nodes'.format(node, group))
            group.remove_node(node)
            group.update_status_recursive()
            logger.info('Removed node {0} from group {1} nodes'.format(node, group))

        logger.info('Removing node {0} from group {1} history'.format(node, group))
        try:
            self.infrastructure.detach_node(group, host, port)
            logger.info('Removed node {0} from group {1} history'.format(node, group))
        except Exception as e:
            logger.error('Failed to remove {0} from group {1} history: {2}'.format(node, group, str(e)))
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

    @h.handler
    def couple_groups(self, request):
        logger.info('----------------------------------------')
        logger.info('New couple groups request: ' + str(request))
        uncoupled_groups = self.get_empty_groups(self.node)
        dc_by_group_id = {}
        group_by_dc = {}
        for group_id in uncoupled_groups:
            group = storage.groups[group_id]

            suitable = True
            for node in group.nodes:
                if node.status != storage.Status.OK:
                    logger.info('Group {0} cannot be used, node {1} status '
                                'is {2} (not OK)'.format(group.group_id,
                                     node, node.status))
                    suitable = False
                    break
            if not suitable:
                continue

            try:
                logger.info('Fetching dc for group {0}'.format(group.group_id))
                dc = group.nodes[0].host.dc
            except IndexError:
                logger.error('Empty nodes list for group %s' % group_id)
                continue

            dc_by_group_id[group_id] = dc
            groups_in_dc = group_by_dc.setdefault(dc, [])
            groups_in_dc.append(group_id)
        logger.info('dc by group: %s' % str(dc_by_group_id))
        logger.info('group_by_dc: %s' % str(group_by_dc))
        size = int(request[0])
        mandatory_groups = [int(g) for g in request[1]]

        # check mandatory set
        for group_id in mandatory_groups:
            if group_id not in uncoupled_groups:
                raise Exception('group %d is coupled' % group_id)
            dc = dc_by_group_id[group_id]
            if dc not in group_by_dc:
                raise Exception('groups must be in different dcs')
            del group_by_dc[dc]

        groups_to_couple = copy.copy(mandatory_groups)

        # need better algorithm for this. For a while - only 1 try and random selection
        n_groups_to_add = size - len(groups_to_couple)
        if n_groups_to_add > len(group_by_dc):
            raise Exception('Not enough dcs')
        if n_groups_to_add < 0:
            raise Exception('Too many mandatory groups')

        # why not use random.sample
        some_dcs = group_by_dc.keys()[:n_groups_to_add]

        for dc in some_dcs:
            groups_to_couple.append(group_by_dc[dc].pop())

        try:
            namespace = request[2]
            logger.info('namespace from request: {0}'.format(namespace))
            if not self.valid_namespace(namespace):
                logger.info('namespace from request: {0}, RaISING EXCEPTION!!!!111'.format(namespace))
                raise ValueError('Namespace "{0}" is invalid'.format(namespace))
            logger.info('namespace from request: {0}'.format(namespace))
        except IndexError:
            namespace = storage.Group.DEFAULT_NAMESPACE

        couple = storage.couples.add([storage.groups[g] for g in groups_to_couple])
        (good, bad) = make_symm_group(self.node, couple, namespace)
        if bad:
            raise bad[1]

        return groups_to_couple

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
            max_group = int(EllAsyncResult(
                self.node.meta_session.read_data(keys.MASTERMIND_MAX_GROUP_KEY),
                EllReadResult
            ).get()[0].data)
        except elliptics.NotFoundError:
            max_group = 0

        new_max_group = max_group + groups_count
        EllAsyncResult(self.node.meta_session.write_data(
            keys.MASTERMIND_MAX_GROUP_KEY, str(new_max_group)),
            EllLookupResult).get()

        return range(max_group + 1, max_group + 1 + groups_count)

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
            meta = (EllAsyncResult(self.node.meta_session.read_latest(key),
                                   EllReadResult).get()[0].data)
        except elliptics.NotFoundError:
            meta = None
        couple.parse_meta(meta)

    def __update_couple_meta(self, couple):

        key = keys.MASTERMIND_COUPLE_META_KEY % str(couple)

        packed = msgpack.packb(couple.meta)
        logger.info('packed meta for couple %s: "%s"' % (couple, str(packed).encode('hex')))
        EllAsyncResult(self.node.meta_session.write_data(key, packed),
                       EllLookupResult).get()

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
        'static-couple', 'auth-key', 'signature'])
    ALLOWED_NS_SIGN_KEYS = set(['token', 'path_prefix', 'port'])


    def namespace_setup(self, request):
        try:
            namespace, settings = request[:2]
        except Exception:
            raise ValueError('Invalid parameters')

        # filtering settings
        for k in settings.keys():
            if k not in self.ALLOWED_NS_KEYS:
                del settings[k]
        for k in settings.get('signature', {}).keys():
            if k not in self.ALLOWED_NS_SIGN_KEYS:
                del settings['signature'][k]

        if not namespace in self.__all_namespaces():
            raise ValueError('Namespace "{0}" does not exist'.format(namespace))

        try:
            self.validate_ns_settings(namespace, settings)
        except Exception as e:
            logger.error(e)
            raise

        logger.debug('saving settings for namespace "{0}": {1}'.format(
            namespace, settings))

        settings['namespace'] = namespace
        self.ns_settings_idx[namespace] = msgpack.packb(settings)
        return True

    def get_namespace_settings(self, request):
        try:
            namespace = request[0]
        except Exception:
            raise ValueError('Invalid parameters')

        if not namespace in self.__all_namespaces():
            raise ValueError('Namespace "{0}" does not exist'.format(namespace))

        res = msgpack.unpackb(self.ns_settings_idx[namespace])
        del res['namespace']
        logger.info('Namespace "{0}" settings: {1}'.format(namespace, res))
        return res

    def get_namespaces_settings(self, request):
        res = {}
        for data in self.ns_settings_idx:
            settings = msgpack.unpackb(data)
            res[settings['namespace']] = settings
            del settings['namespace']
        return res

    @h.handler
    def freeze_couple(self, request):
        logger.info('freezing couple %s' % str(request))
        couple = self.__get_couple(request)

        self.__sync_couple_meta(couple)
        if couple.frozen:
            raise ValueError('Couple %s is already frozen' % couple)
        couple.freeze()
        self.__update_couple_meta(couple)

        return True

    @h.handler
    def unfreeze_couple(self, request):
        logger.info('unfreezing couple %s' % str(request))
        couple = self.__get_couple(request)

        self.__sync_couple_meta(couple)
        if not couple.frozen:
            raise ValueError('Couple %s is not frozen' % couple)
        couple.unfreeze()
        self.__update_couple_meta(couple)

        return True

    @h.handler
    def get_namespaces(self, request):
        return tuple(self.__all_namespaces())

    def __all_namespaces(self):
        namespaces = [c.namespace for c in storage.couples]
        return set(filter(None, namespaces))


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
    s.set_timeout(config.get('wait_timeout', 5))
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
    s.set_timeout(config.get('wait_timeout', 5))
    good = []
    bad = ()
    for group in couple:
        try:
            packed = msgpack.packb(couple.compose_group_meta(namespace))
            logger.info('packed couple for group %d: "%s"' % (group.group_id, str(packed).encode('hex')))
            s.add_groups([group.group_id])
            EllAsyncResult(s.write_data(keys.SYMMETRIC_GROUPS_KEY, packed),
                           EllLookupResult).get()
            group.parse_meta(packed)
            good.append(group.group_id)
        except Exception as e:
            logger.error('Failed to write symm group info, group %d: %s\n%s'
                         % (group.group_id, str(e), traceback.format_exc()))
            bad = (group.group_id, e)
            break
    return (good, bad)
