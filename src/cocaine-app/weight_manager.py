import itertools
import math
import random

from config import config
from infrastructure import infrastructure
from load_manager import load_manager
import logging


logger = logging.getLogger('mm.weights')


class WeightManager(object):

    MIN_NS_UNITS = config.get('weight', {}).get('min_units', 1)
    ADD_NS_UNITS = config.get('weight', {}).get('add_units', 1)

    def __init__(self):
        self.disks = {}
        self.net = {}
        self.couples = {}
        self.couples_by_disk = {}
        self.couples_by_net = {}
        self.weights = {}

    def update(self, storage):
        self.update_resources(storage)
        self.calculate_weights(storage)

    def update_resources(self, storage):
        disks = {}
        net = {}
        couples = {}
        couples_by_disk = {}
        couples_by_net = {}
        for couple in storage.couples:
            if couple.status != storage.Status.OK:
                continue
            groups_res = []
            for group in couple.groups:
                nbs_res = []
                for nb in group.node_backends:
                    nb_hostname = nb.node.host.hostname
                    net_res = net.setdefault(nb_hostname,
                                             NetResources(load_manager.net[nb_hostname]))
                    disk_key = (nb_hostname, nb.fs.fsid)
                    disk_res = disks.setdefault(disk_key,
                                                DiskResources(load_manager.disks[disk_key]))
                    nbs_res.append(
                        NodeBackendResources(
                            disk_res,
                            net_res,
                            load_manager.node_backends[nb]
                        )
                    )

                    couples_by_disk.setdefault(disk_key, set()).add(couple)
                    couples_by_net.setdefault(nb_hostname, set()).add(couple)
                groups_res.append(GroupResources(nbs_res))
            couples[couple] = CoupleResources(couple, groups_res)

        self.disks = disks
        self.net = net
        self.couples = couples
        self.couples_by_disk = couples_by_disk
        self.couples_by_net = couples_by_net

    def calculate_weights(self, storage):
        try:
            weights = {}
            for ns in self.namespaces(storage):
                ns_weights = {}
                ns_sizes = {}

                ns_settings = infrastructure.ns_settings[ns]
                ns_min_units = ns_settings.get('min-units', self.MIN_NS_UNITS)
                ns_add_units = ns_settings.get('add-units', self.ADD_NS_UNITS)

                for couple in ns.couples:
                    ns_weights.setdefault(len(couple.groups), [])
                    if couple.status != storage.Status.OK:
                        continue
                    ns_sizes.setdefault(len(couple.groups), []).append(self.couples[couple])

                required_units = WeightCalculator.ns_used_resources(ns)

                logger.debug('Ns {}, required resources: {}'.format(
                    ns.id,
                    required_units
                ))

                for ns_size, couples_res in ns_sizes.iteritems():
                    buckets = CouplesBuckets(couples_res)
                    claimed_units = WeightCalculator.zero_resources()

                    ns_weights[ns_size] = []

                    for couple_res in buckets:
                        logger.debug('Ns {}, calculating weight for couple {}'.format(
                            ns.id,
                            couple_res.couple
                        ))
                        # count couple weight, accumulate it
                        weight, couple_res_units = WeightCalculator.calculate_resources(couple_res)
                        claimed_units += couple_res_units
                        logger.debug('Ns {}, acc claimed resources: {}'.format(
                            ns.id,
                            claimed_units
                        ))
                        # change the state of resources
                        self.__claim(couple_res, couple_res_units)
                        # mark couples with shared resources
                        self.__rebucket(buckets, couple_res)
                        ns_weights[ns_size].append((
                            couple_res.couple.as_tuple(),
                            weight,
                            couple_res.couple.effective_free_space
                        ))
                        # TODO: commented for tests
                        # enough_couples = len(ns_weights[ns_size]) >= ns_min_units + ns_add_units
                        # if claimed_units >= required_units and enough_couples:
                        #     # claimed enough resouce units for namespace
                        #     break

                ns_groups_count = infrastructure.ns_settings[ns]['groups-count']
                found_couples = len(ns_weights.get(ns_groups_count, []))
                if found_couples < ns_min_units:
                    logger.error(
                        'Namespace {}, {}, has {} available couples, {} required'.format(
                            ns.id,
                            'static' if 'static-couple' in ns_settings else 'non-static',
                            found_couples,
                            ns_min_units
                        )
                    )
                    # TODO: log error message
                    continue
                else:
                    weights[ns.id] = ns_weights

            self.weights = weights
        except Exception:
            logger.exception('Failed to calculate weights')
            pass

    def __rebucket(self, buckets, couple_res):
        disk_keys = couple_res.disks_keys()
        net_keys = couple_res.net_keys()

        couples_to_rebucket = set()
        # TODO: code duplication!
        for disk_key in disk_keys:
            for couple in self.couples_by_disk.get(disk_key):
                if couple.namespace != couple_res.couple.namespace:
                    continue
                couples_to_rebucket.add(couple)
        for net_key in net_keys:
            for couple in self.couples_by_net.get(net_key):
                if couple.namespace != couple_res.couple.namespace:
                    continue
                couples_to_rebucket.add(couple)

        for couple in couples_to_rebucket:
            buckets.insert(self.couples[couple], updating=True)

    def __claim(self, couple_res, resource_units):
        disks = [self.disks[disk_key] for disk_key in couple_res.disks_keys()]
        nets = [self.net[net_key] for net_key in couple_res.net_keys()]
        for resource in itertools.chain(disks, nets):
            resource.claim(resource_units)

    @staticmethod
    def namespaces(storage):
        nss = storage.namespaces.keys()
        nss.sort(key=lambda ns: len(ns.couples))
        for ns in nss:
            if ns.id == storage.Group.CACHE_NAMESPACE:
                continue
            yield ns


class WeightCalculator(object):
    DISK_UTIL_COEF = config.get('weight', {}).get('resource_unit_disk_util', 0.05)
    DISK_NET_RATE_COEF = config.get('weight', {}).get('resource_unit_net_write_rate',
                                                      5 * (1024 ** 2))

    @staticmethod
    def ns_used_resources(ns):
        load = load_manager.namespaces[ns]
        max_coef = max(load.net_write_rate / WeightCalculator.DISK_NET_RATE_COEF,
                       load.disk_util_write / WeightCalculator.DISK_UTIL_COEF)
        return ResourceUnit(disk_util=max_coef * WeightCalculator.DISK_UTIL_COEF,
                            net_rate=max_coef * WeightCalculator.DISK_NET_RATE_COEF)

    @staticmethod
    def used_space_coef(x):
        if x <= 0.5:
            space_coef = math.e ** (-(2 * x - 1) ** 2)
        else:
            space_coef = math.e ** (-(3 * x - 1.5) ** 2)
        return space_coef

    @staticmethod
    def calculate_resources(couple_res):
        """
        Get integer weight and resource units claimed from couple

        Returns:
            A tuple of (calculated couple weight, <ResourceUnit> that should be claimed)
        """
        # TODO: get couple effective_free_space percentage
        couple = couple_res.couple
        used_space_pct = max(1.0 - float(couple.effective_free_space) / couple.effective_space,
                             0.0)
        base_coef = WeightCalculator.used_space_coef(used_space_pct)
        logger.debug('Couple {} used_space_pct: {}, base weight" {}'.format(
            couple_res.couple, used_space_pct, int(base_coef * 10000)))
        return (
            int(base_coef * 10000),
            ResourceUnit(disk_util=base_coef * WeightCalculator.DISK_UTIL_COEF,
                         net_rate=base_coef * WeightCalculator.DISK_NET_RATE_COEF)
        )

    @staticmethod
    def zero_resources():
        return ResourceUnit(disk_util=0.0, net_rate=0.0)


class CouplesBuckets(object):

    BUCKET_ORDER = ('base', 'tired')

    def __init__(self, couples_res):
        self.skip = set()
        self.buckets = [[] for _ in xrange(len(self.BUCKET_ORDER))]
        self.buckets_idx = [set() for _ in xrange(len(self.BUCKET_ORDER))]
        for couple_res in couples_res:
            self.insert(couple_res)

    @staticmethod
    def is_base(couple_res):
        return (couple_res.disk_util < 0.7 and
                couple_res.net_write_rate < 70 * (1024 ** 2) and
                couple_res.io_blocking_queue_size < 10 and
                couple_res.io_nonblocking_queue_size < 10)

    @staticmethod
    def is_tired(couple_res):
        return True

    def __iter__(self):
        for bucket in self.buckets:
            self.skip = set()
            if not bucket:
                continue
            random.shuffle(bucket)
            for couple_res in bucket:
                if couple_res.couple not in self.skip:
                    yield couple_res

    def insert(self, couple_res, updating=False):
        bucket_id = None
        for i, bucket in enumerate(self.BUCKET_ORDER):
            checker = getattr(self, 'is_' + bucket)
            if checker(couple_res):
                bucket_id = i
                break
        else:
            raise ValueError(
                'No appropriate bucket found for couple {}'.format(couple_res.couple)
            )
        if couple_res.couple not in self.buckets_idx[bucket_id]:
            self.buckets[bucket_id].append(couple_res)
            self.buckets_idx[bucket_id].add(couple_res.couple)
            if updating:
                for i, bucket in enumerate(self.buckets):
                    if i == bucket_id:
                        continue
                    if couple_res.couple in self.buckets_idx[i]:
                        self.skip.add(couple_res.couple)
                        self.buckets_idx[i].remove(couple_res.couple)
                        break


class ResourceUnit(object):
    def __init__(self, disk_util, net_rate):
        self.disk_util = disk_util
        self.net_rate = net_rate

    def __gt__(self, other):
        return (self.disk_util > other.disk_util and
                self.net_rate > other.net_rate)

    def __ge__(self, other):
        return (self.disk_util >= other.disk_util and
                self.net_rate >= other.net_rate)

    def __add__(self, other):
        return ResourceUnit(disk_util=self.disk_util + other.disk_util,
                            net_rate=self.net_rate + other.net_rate)

    def __iadd__(self, other):
        self.disk_util += other.disk_util
        self.net_rate += other.net_rate
        return self

    def __repr__(self):
        return '<ResourceUnits: disk_util: {}, net_rate: {}>'.format(
            self.disk_util,
            self.net_rate
        )


class CoupleResources(object):
    def __init__(self, couple, groups_res):
        self.couple = couple
        self.groups_res = groups_res

    @property
    def disk_util(self):
        return max(g_res.disk_util for g_res in self.groups_res)

    @property
    def net_write_rate(self):
        return max(g_res.net_write_rate for g_res in self.groups_res)

    @property
    def io_blocking_queue_size(self):
        return max(g_res.io_blocking_queue_size for g_res in self.groups_res)

    @property
    def io_nonblocking_queue_size(self):
        return max(g_res.io_nonblocking_queue_size for g_res in self.groups_res)

    def __get_resource_keys(self, key_mapper):
        keys = set()
        for group in self.couple.groups:
            for nb in group.node_backends:
                keys.add(key_mapper(nb))
        return keys

    def disks_keys(self):
        for disk_key in self.__get_resource_keys(DiskResources.key):
            yield disk_key

    def net_keys(self):
        for net_key in self.__get_resource_keys(NetResources.key):
            yield net_key


class GroupResources(object):
    def __init__(self, nbs_res):
        self.node_backends_res = nbs_res

    @property
    def disk_util(self):
        return sum(nb_res.disk_util for nb_res in self.node_backends_res)

    @property
    def net_write_rate(self):
        return sum(nb_res.net_write_rate for nb_res in self.node_backends_res)

    @property
    def io_blocking_queue_size(self):
        return sum(nb_res.io_blocking_queue_size for nb_res in self.node_backends_res)

    @property
    def io_nonblocking_queue_size(self):
        return sum(nb_res.io_nonblocking_queue_size for nb_res in self.node_backends_res)


class NodeBackendResources(object):
    def __init__(self, disk_res, node_res, node_backend_load):
        self.disk_res = disk_res
        self.node_res = node_res
        self.io_blocking_queue_size = node_backend_load.io_blocking_queue_size
        self.io_nonblocking_queue_size = node_backend_load.io_nonblocking_queue_size

    @property
    def disk_util(self):
        return self.disk_res.disk_util

    @property
    def net_write_rate(self):
        return self.node_res.net_write_rate


class DiskResources(object):
    def __init__(self, disk_load):

        ext_write_rate = disk_load.write_rate - disk_load.ell_write_rate
        ext_write_rate_ratio = (
            ext_write_rate / disk_load.write_rate
            if disk_load.write_rate else
            1.0
        )
        ext_write_disk_util = ext_write_rate_ratio * disk_load.disk_util_write

        self.disk_util = min(disk_load.disk_util_read + ext_write_disk_util, 1.0)

    @staticmethod
    def key(nb):
        return (nb.node.host.hostname, nb.fs.fsid)

    def claim(self, resource_units):
        self.disk_util = max(self.disk_util + resource_units.disk_util,
                             1.0)


class NetResources(object):
    def __init__(self, net_load):
        self.net_write_rate = net_load.write_rate - net_load.ell_write_rate

    @staticmethod
    def key(nb):
        return nb.node.host.hostname

    def claim(self, resource_units):
        self.net_write_rate = self.net_write_rate + resource_units.net_rate


weight_manager = WeightManager()
