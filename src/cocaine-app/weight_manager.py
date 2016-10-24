import itertools
import math
import random

from jobs.job import Job
from jobs.job_types import JobTypes
from load_manager import load_manager
import logging
from mastermind_core.config import config


logger = logging.getLogger('mm.weights')

WEIGHT_CFG = config.get('weight', {})


class WeightManager(object):

    MIN_NS_UNITS = WEIGHT_CFG.get('min_units', 1)
    ADD_NS_UNITS = WEIGHT_CFG.get('add_units', 1)

    def __init__(self):
        self.disks = {}
        self.net = {}
        self.couples = {}
        self.couples_by_disk = {}
        self.couples_by_net = {}
        self.weights = {}

    def update(self, storage, namespaces_settings):
        self.update_resources(storage)
        self.calculate_weights(storage, namespaces_settings)

    def update_resources(self, storage):
        disks = {}
        net = {}
        couples = {}
        couples_by_disk = {}
        couples_by_net = {}
        for couple in storage.replicas_groupsets:
            # TODO: add other couples (e.g. FULL) to account their load
            if couple.status != storage.Status.OK:
                continue
            groups_res = []
            for group in couple.groups:
                nbs_res = []
                for nb in group.node_backends:
                    nb_hostname = nb.node.host.hostname
                    net_res = net.setdefault(
                        nb_hostname,
                        NetResources(nb_hostname, load_manager.net[nb_hostname])
                    )
                    disk_key = (nb_hostname, nb.fs.fsid)
                    disk_res = disks.setdefault(
                        disk_key,
                        DiskResources(disk_key, load_manager.disks[disk_key])
                    )
                    disk_res.account_node_backend(nb)
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

    def calculate_weights(self, storage, namespaces_settings):
        ns_settings_by_ns = {
            ns_settings.namespace: ns_settings
            for ns_settings in namespaces_settings
        }
        try:
            weights = {}
            min_couple_res_units = WeightCalculator.min_couple_resources()
            zero_res_units = WeightCalculator.zero_resources()
            for ns in self.namespaces(storage, ns_settings_by_ns):
                ns_weights = {}
                ns_sizes = {}

                ns_settings = ns_settings_by_ns[ns]
                ns_min_units = ns_settings.min_units or self.MIN_NS_UNITS
                ns_add_units = ns_settings.add_units or self.ADD_NS_UNITS

                for couple in ns.couples:
                    ns_weights.setdefault(len(couple.groups), [])
                    if couple.status != storage.Status.OK:
                        continue
                    if couple not in self.couples:
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
                    skip_couples = set()

                    for couple_res in buckets:
                        logger.debug('Ns {}, calculating weight for couple {}'.format(
                            ns.id,
                            couple_res.couple
                        ))
                        # count couple weight, accumulate it
                        weight, couple_res_units = WeightCalculator.calculate_resources(couple_res)
                        # change the state of resources
                        if required_units - claimed_units > zero_res_units:
                            claim_res_units = min(
                                couple_res_units,
                                required_units - claimed_units
                            )
                        else:
                            claim_res_units = min_couple_res_units
                        self.__claim(couple_res, claim_res_units)
                        # mark couples with shared resources
                        claimed_units += claim_res_units
                        logger.debug('Ns {}, acc claimed resources: {}'.format(
                            ns.id,
                            claimed_units
                        ))
                        skip_couples.add(couple_res.couple)
                        self.__rebucket(buckets, couple_res, skip_couples, ns_size)
                        ns_weights[ns_size].append((
                            couple_res.couple.as_tuple(),
                            weight,
                            couple_res.couple.effective_free_space
                        ))
                        enough_couples = len(ns_weights[ns_size]) >= ns_min_units + ns_add_units
                        if claimed_units >= required_units and enough_couples:
                            # claimed enough resouce units for namespace
                            break

                ns_groups_count = ns_settings.groups_count
                found_couples = len(ns_weights.get(ns_groups_count, []))
                if found_couples < ns_min_units:
                    logger.error(
                        'Namespace {}, {}, has {} available couples, {} required'.format(
                            ns.id,
                            'static' if ns_settings.static_couple else 'non-static',
                            found_couples,
                            ns_min_units
                        )
                    )
                    continue
                else:
                    weights[ns.id] = ns_weights

            self.weights = weights
        except Exception:
            logger.exception('Failed to calculate weights')
            pass

    def __rebucket(self, buckets, couple_res, skip_couples, couple_size):
        disk_keys = couple_res.disks_keys()
        net_keys = couple_res.net_keys()

        couples_to_rebucket = set()

        def populate_neighbours(keys, couples_by_key_container, neighbours):
            for key in keys:
                for couple in couples_by_key_container.get(key, []):
                    if couple.namespace != couple_res.couple.namespace:
                        continue
                    if len(couple.groups) != couple_size:
                        continue
                    if couple in skip_couples:
                        continue
                    neighbours.add(couple)

        populate_neighbours(disk_keys, self.couples_by_disk, couples_to_rebucket)
        populate_neighbours(net_keys, self.couples_by_net, couples_to_rebucket)

        for couple in couples_to_rebucket:
            buckets.insert(self.couples[couple], updating=True)

    def __claim(self, couple_res, resource_units):
        disks = [self.disks[disk_key] for disk_key in couple_res.disks_keys()]
        nets = [self.net[net_key] for net_key in couple_res.net_keys()]
        for resource in itertools.chain(disks, nets):
            resource.claim(resource_units)
            logger.debug('Ns {}, claimed {} resource units from resource {}'.format(
                couple_res.couple.namespace.id,
                resource_units,
                resource
            ))

    @staticmethod
    def namespaces(storage, ns_settings_by_ns):
        nss = storage.namespaces.keys()
        nss.sort(key=lambda ns: len(ns.couples))
        for ns in nss:
            if ns.id == storage.Group.CACHE_NAMESPACE:
                continue
            if ns.id not in ns_settings_by_ns:
                # namespace does not have settings
                continue
            yield ns


class WeightCalculator(object):
    """
    Calculates weights for couples

    NOTE: Common linear decrease formula for a parameter:
        Let 'top_y' be the top (start) coefficient value, let 'delta_y' be the difference
        between top (start) and bottom (end) coefficient value, let 'delta_x' be the
        difference between start and end value of measured parameter, let 'offset_x' be
        the difference between current measured parameter value and its start value, then
        the formula has the following form:
            top_y -
            delta_y *
            (
                offset_x /
                delta_x
            )
    """
    DISK_UTIL_COEF = WEIGHT_CFG.get('resource_unit_disk_util', 0.05)
    DISK_NET_RATE_COEF = WEIGHT_CFG.get('resource_unit_net_write_rate', 5 * (1024 ** 2))

    ADD_RES_UNITS_REL = WEIGHT_CFG.get('add_resource_units_relative', 0.15)
    ADD_RES_UNITS_ABS = WEIGHT_CFG.get('add_resource_units_absolute', 0.20)

    @staticmethod
    def ns_used_resources(ns):
        load = load_manager.namespaces[ns]
        max_coef = max(load.net_write_rate / WeightCalculator.DISK_NET_RATE_COEF,
                       load.disk_util_write / WeightCalculator.DISK_UTIL_COEF)
        # add reserve resource units
        max_coef = max(max_coef * (1.0 + WeightCalculator.ADD_RES_UNITS_REL),
                       max_coef + WeightCalculator.ADD_RES_UNITS_ABS)
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
    def write_net_coef(write_rate):
        base_threshold_min_coef = 0.1
        min_coef = 0.01
        if write_rate <= NetResources.WRITE_RATE_THRESHOLD * 0.5:
            return 1.0
        if write_rate <= NetResources.WRITE_RATE_THRESHOLD:
            return (
                1.0 -                                                                              # top_y
                (1.0 - base_threshold_min_coef) *                                                  # delta_y
                (
                    (write_rate - NetResources.WRITE_RATE_THRESHOLD * 0.5) /                       # offset_x
                    (NetResources.WRITE_RATE_THRESHOLD - NetResources.WRITE_RATE_THRESHOLD * 0.5)  # delta_x
                )
            )
        return max(
            (
                base_threshold_min_coef -                                              # top_y
                (base_threshold_min_coef - min_coef) *                                 # delta_y
                (
                    (write_rate - NetResources.WRITE_RATE_THRESHOLD) /                 # offset_x
                    (NetResources.MAX_WRITE_RATE - NetResources.WRITE_RATE_THRESHOLD)  # delta_x
                )
            ),
            min_coef
        )

    @staticmethod
    def read_net_coef(read_rate):
        """
        Calculate read network coefficient.

        For network read rate:
            from 0 b/s to READ_RATE_THRESHOLD: 1.0
            from READ_RATE_THRESHOLD to MAX_READ_RATE: linear decrease from 1.0 to 0.01
            from MAX_READ_RATE to inf: 0.01
        """
        min_coef = 0.01
        if read_rate <= NetResources.READ_RATE_THRESHOLD:
            return 1.0
        return max(
            (
                1.0 -                                     # top_y
                (1.0 - min_coef) *                        # delta_y
                (
                    read_rate /                           # offset_x
                    NetResources.MAX_READ_RATE            # delta_x
                )
            ),
            min_coef
        )

    @staticmethod
    def disk_coef(x):
        def normal_mode_coef(x):
            return math.e ** (-12 * (x ** 2))

        if x <= DiskResources.DISK_UTIL_THRESHOLD:
            return normal_mode_coef(x)
        else:
            base_point = normal_mode_coef(DiskResources.DISK_UTIL_THRESHOLD)
            min_coef = 0.01
            return max(
                (
                    base_point - (
                        (base_point - min_coef) *
                        (
                            (x - DiskResources.DISK_UTIL_THRESHOLD) /
                            (1.0 - DiskResources.DISK_UTIL_THRESHOLD)
                        )
                    )
                ),
                min_coef
            )

    @staticmethod
    def weight(base_coef, write_net_coef, read_net_coef, disk_coef):
        return int(1000000 * base_coef * write_net_coef * read_net_coef * disk_coef)

    @staticmethod
    def resource_coef(base_coef, write_net_coef, disk_coef):
        return base_coef * write_net_coef * disk_coef

    @staticmethod
    def calculate_resources(couple_res):
        """
        Get integer weight and resource units claimed from couple

        Returns:
            A tuple of (calculated couple weight, <ResourceUnit> that should be claimed)
        """
        couple = couple_res.couple
        used_space_pct = max(1.0 - float(couple.effective_free_space) / couple.effective_space,
                             0.0)
        base_coef = WeightCalculator.used_space_coef(used_space_pct)
        write_net_coef = WeightCalculator.write_net_coef(couple_res.net_write_rate)
        read_net_coef = WeightCalculator.read_net_coef(couple_res.net_read_rate)
        disk_coef = WeightCalculator.disk_coef(couple_res.disk_util)

        weight = WeightCalculator.weight(base_coef, write_net_coef, read_net_coef, disk_coef)

        logger.info(
            'Ns {}, couple {} used_space_pct: {}, base coef {}, '
            'net_write_rate: {} Mb/s, write_net_coef {}, '
            'disk_util: {}, disk_coef {}, '
            'weight = {}'.format(
                couple_res.couple.namespace.id,
                couple_res.couple,
                used_space_pct,
                base_coef,
                couple_res.net_write_rate,
                write_net_coef,
                couple_res.disk_util,
                disk_coef,
                weight
            )
        )

        resource_coef = WeightCalculator.resource_coef(base_coef, write_net_coef, disk_coef)
        return (
            weight,
            ResourceUnit(disk_util=resource_coef * WeightCalculator.DISK_UTIL_COEF,
                         net_rate=resource_coef * WeightCalculator.DISK_NET_RATE_COEF)
        )

    @staticmethod
    def zero_resources():
        return ResourceUnit(disk_util=0.0, net_rate=0.0)

    @staticmethod
    def min_couple_resources():
        return ResourceUnit(disk_util=0.0025, net_rate=float(250 * 1024))


class CouplesBuckets(object):

    # a couple is checked against buckets in order
    # described by BUCKET_ORDER. Each bucket should have
    # 'is_<bucket_id>' method. Couple falls into a first
    # bucket for which is passes the check.
    BUCKET_ORDER = ('base', 'on_defragmenting_disk', 'tired')

    def __init__(self, couples_res):
        self.skip = set()
        self.buckets = [[] for _ in xrange(len(self.BUCKET_ORDER))]
        self.buckets_idx = [set() for _ in xrange(len(self.BUCKET_ORDER))]
        for couple_res in couples_res:
            self.insert(couple_res)

    @staticmethod
    def utilized(couple_res):
        return (couple_res.disk_util >= DiskResources.DISK_UTIL_THRESHOLD or
                couple_res.net_write_rate >= NetResources.WRITE_RATE_THRESHOLD or
                couple_res.net_read_rate >= NetResources.READ_RATE_THRESHOLD or
                couple_res.io_blocking_queue_size >= 10 or
                couple_res.io_nonblocking_queue_size >= 10)

    @staticmethod
    def is_base(couple_res):
        logger.debug(
            'Couple {couple}: disk_util: {disk_util}, net_write_rate {net_write_rate}, '
            'net_read_rate {net_read_rate}, nbr_res {nbr_res}'.format(
                couple=couple_res.couple,
                disk_util=couple_res.disk_util,
                net_write_rate=couple_res.net_write_rate,
                net_read_rate=couple_res.net_read_rate,
                nbr_res=[
                    nbr.net_write_rate
                    for gr in couple_res.groups_res
                    for nbr in gr.node_backends_res
                ],
            )
        )
        return not CouplesBuckets.utilized(couple_res) and not couple_res.on_defragmenting_disk

    @staticmethod
    def is_on_defragmenting_disk(couple_res):
        return not CouplesBuckets.utilized(couple_res) and couple_res.on_defragmenting_disk

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
            logger.debug('Couple {} goes into bucket {} ({}){}'.format(
                couple_res.couple,
                bucket_id + 1,
                self.BUCKET_ORDER[bucket_id],
                ' (rebucketing)' if updating else ''
            ))
            if updating:
                for i, bucket in enumerate(self.buckets):
                    if i == bucket_id:
                        break
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

    def __sub__(self, other):
        return ResourceUnit(disk_util=max(self.disk_util - other.disk_util, 0.0),
                            net_rate=max(self.net_rate - other.net_rate, 0.0))

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
    def on_defragmenting_disk(self):
        return any(g_res.on_defragmenting_disk for g_res in self.groups_res)

    @property
    def net_write_rate(self):
        return max(g_res.net_write_rate for g_res in self.groups_res)

    @property
    def net_read_rate(self):
        return max(g_res.net_read_rate for g_res in self.groups_res)

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
    def on_defragmenting_disk(self):
        return any(nb_res.is_defragmentation_running for nb_res in self.node_backends_res)

    @property
    def net_write_rate(self):
        return sum(nb_res.net_write_rate for nb_res in self.node_backends_res)

    @property
    def net_read_rate(self):
        return sum(nb_res.net_read_rate for nb_res in self.node_backends_res)

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
    def is_defragmentation_running(self):
        return self.disk_res.is_defragmentation_running

    @property
    def net_write_rate(self):
        return self.node_res.net_write_rate

    @property
    def net_read_rate(self):
        return self.node_res.net_read_rate


class DiskResources(object):

    DISK_UTIL_THRESHOLD = WEIGHT_CFG.get('disk', {}).get('disk_util_threshold', 0.3)
    MAX_DISK_UTIL = 1.0

    def __init__(self, key, disk_load):
        self.key = key

        ext_write_rate = disk_load.write_rate - disk_load.ell_write_rate
        ext_write_rate_ratio = (
            ext_write_rate / disk_load.write_rate
            if disk_load.write_rate else
            1.0
        )
        ext_write_disk_util = ext_write_rate_ratio * disk_load.disk_util_write

        self.disk_util = min(disk_load.disk_util_read + ext_write_disk_util, self.MAX_DISK_UTIL)
        self.is_defragmentation_running = False

    def account_node_backend(self, nb):
        """Account node backend that resides on the disk.

        NB: This method is guaranteed to be called once per backend.
        """
        if nb.stat.defrag_state == 1:
            self.is_defragmentation_running = True
        else:
            active_job = nb.group.couple.active_job
            if active_job is not None:
                is_defrag_job = active_job['type'] == JobTypes.TYPE_COUPLE_DEFRAG_JOB
                is_executing_job = active_job['status'] in (Job.STATUS_NEW, Job.STATUS_EXECUTING)
                if is_defrag_job and is_executing_job:
                    self.is_defragmentation_running = True

    @staticmethod
    def key(nb):
        return (nb.node.host.hostname, nb.fs.fsid)

    def claim(self, resource_units):
        self.disk_util = min(
            self.disk_util + resource_units.disk_util,
            self.MAX_DISK_UTIL
        )

    def __repr__(self):
        return '<Disk {}: disk_util: {:.4f}>'.format(self.key, self.disk_util)


class NetResources(object):

    WRITE_RATE_THRESHOLD = WEIGHT_CFG.get('net', {}).get('write_rate_threshold', 70 * (1024 ** 2))
    READ_RATE_THRESHOLD = WEIGHT_CFG.get('net', {}).get('read_rate_threshold', 70 * (1024 ** 2))
    MAX_WRITE_RATE = WEIGHT_CFG.get('net', {}).get('max_write_rate', 100 * (1024 ** 2))
    MAX_READ_RATE = WEIGHT_CFG.get('net', {}).get('max_read_rate', 100 * (1024 ** 2))

    def __init__(self, key, net_load):
        self.key = key
        # elliptics write rate is not taken into account since we assume
        # that all write load is controlled by our weights calculation
        # algorithm, therefore we need to consider only external write rate when
        # beginning to distribute write load
        self.net_write_rate = net_load.write_rate - net_load.ell_write_rate
        self.net_read_rate = net_load.read_rate

    @staticmethod
    def key(nb):
        return nb.node.host.hostname

    def claim(self, resource_units):
        # net_read_rate is not affected by resource claiming

        # NOTE: net write rate is not claimed since this can lead to lock shared net
        # resources by namespaces that will not actually use them
        # self.net_write_rate = self.net_write_rate + resource_units.net_rate

        pass

    def __repr__(self):
        return (
            '<Net {key}: net_write_rate: {write_rate:.4f} Mb/s, '
            'net_read_rate: {read_rate:.4f} Mb/s>'.format(
                key=self.key,
                write_rate=self.net_write_rate / float(1024 ** 2),
                read_rate=self.net_read_rate / float(1024 ** 2),
            )
        )


weight_manager = WeightManager()
