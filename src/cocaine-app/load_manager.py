class LoadManager(object):
    def __init__(self):
        self.namespaces = {}
        self.couples = {}
        self.groups = {}
        self.node_backends = {}
        self.disks = {}
        self.net = {}

    def update(self, storage):
        namespaces = {}
        couples = {}
        groups = {}
        node_backends = {}
        disks = {}
        net = {}
        for ns in storage.namespaces:
            namespaces[ns] = nsl = NamespaceLoad()
            for couple in ns.couples:
                couples[couple] = cl = CoupleLoad()
                for group in couple.groups:
                    groups[group] = gl = GroupLoad()
                    for nb in group.node_backends:
                        node_backends[nb] = nbl = NodeBackendLoad()

                        nb_hostname = nb.node.host.hostname
                        if nb_hostname not in net:
                            net[nb_hostname] = NetLoad(nb.node.stat)
                        if (nb_hostname, nb.fs.fsid) not in disks:
                            disks[(nb_hostname, nb.fs.fsid)] = DiskLoad(nb.fs.stat)

                        nbl.set(nb.stat, disks[(nb_hostname, nb.fs.fsid)])
                        gl.add_backend(nbl)

                    cl.add_group(gl)
                nsl.add_couple(cl)
        self.namespaces = namespaces
        self.couples = couples
        self.groups = groups
        self.node_backends = node_backends
        self.disks = disks
        self.net = net


load_manager = LoadManager()


class EllipticsLoad(object):
    def __init__(self):
        self.io_blocking_queue_size = 0
        self.io_nonblocking_queue_size = 0

        self.disk_read_rate = 0.0
        self.disk_write_rate = 0.0
        self.net_read_rate = 0.0
        self.net_write_rate = 0.0

        self.disk_util = 0.0
        self.disk_util_read = 0.0
        self.disk_util_write = 0.0

class NamespaceLoad(EllipticsLoad):
    def add_couple(self, couple):
        self.io_blocking_queue_size += couple.io_blocking_queue_size
        self.io_nonblocking_queue_size += couple.io_nonblocking_queue_size
        self.disk_read_rate += couple.disk_read_rate
        self.disk_write_rate += couple.disk_write_rate
        self.net_read_rate += couple.net_read_rate
        self.net_write_rate += couple.net_write_rate

        self.disk_util += couple.disk_util
        self.disk_util_read += couple.disk_util_read
        self.disk_util_write += couple.disk_util_write


class CoupleLoad(EllipticsLoad):
    def add_group(self, group):
        self.io_blocking_queue_size = max(self.io_blocking_queue_size,
                                          group.io_blocking_queue_size)
        self.io_nonblocking_queue_size = max(self.io_nonblocking_queue_size,
                                             group.io_nonblocking_queue_size)
        # read-key operation is commonly performed on one group, so we can sum
        # all groups' reads to get couple read rate
        self.disk_read_rate = self.disk_read_rate + group.disk_read_rate
        self.net_read_rate = self.net_read_rate + group.net_read_rate
        # write-key operation is performed to all groups at once to guarantee
        # redundancy, so we take the maximum write rate of all groups
        self.disk_write_rate = max(self.disk_write_rate, group.disk_write_rate)
        self.net_write_rate = max(self.net_write_rate, group.net_write_rate)

        self.disk_util += group.disk_util
        self.disk_util_read += group.disk_util_read
        # TODO: do we need to account a single copy disk_util_write or a sum?
        self.disk_util_write = max(self.disk_util_write, group.disk_util_write)


class GroupLoad(EllipticsLoad):
    def add_backend(self, nb):
        self.io_blocking_queue_size += nb.io_blocking_queue_size
        self.io_nonblocking_queue_size += nb.io_nonblocking_queue_size
        self.disk_read_rate += nb.disk_read_rate
        self.disk_write_rate += nb.disk_write_rate
        self.net_read_rate += nb.net_read_rate
        self.net_write_rate += nb.net_write_rate

        self.disk_util += nb.disk_util
        self.disk_util_read += nb.disk_util_read
        self.disk_util_write += nb.disk_util_write


class NodeBackendLoad(EllipticsLoad):
    def set(self, nb_stat, disk_load):
        self.io_blocking_queue_size = nb_stat.io_blocking_size
        self.io_nonblocking_queue_size = nb_stat.io_nonblocking_size

        self.disk_read_rate = nb_stat.commands_stat.ell_disk_read_rate
        self.disk_write_rate = nb_stat.commands_stat.ell_disk_write_rate
        self.net_read_rate = nb_stat.commands_stat.ell_net_read_rate
        self.net_write_rate = nb_stat.commands_stat.ell_net_write_rate

        disk_io_rate = disk_load.write_rate + disk_load.read_rate
        disk_io_rate_ratio = (
            (self.disk_read_rate + self.disk_write_rate) / disk_io_rate
            if disk_io_rate else
            0.0
        )
        self.disk_util = disk_load.disk_util * disk_io_rate_ratio

        disk_read_rate_ratio = (
            self.disk_read_rate / disk_load.read_rate
            if disk_load.read_rate else
            0.0
        )
        self.disk_read_util = disk_load.disk_util_read * disk_read_rate_ratio

        disk_write_rate_ratio = (
            self.disk_write_rate / disk_load.write_rate
            if disk_load.write_rate else
            0.0
        )
        self.disk_write_util = disk_load.disk_util_write * disk_write_rate_ratio


class NetLoad(object):
    def __init__(self, node_stat):
        self.read_rate = max(node_stat.tx_rate,
                             node_stat.commands_stat.ell_net_read_rate)
        self.ell_read_rate = min(node_stat.tx_rate,
                                 node_stat.commands_stat.ell_net_read_rate)
        self.write_rate = max(node_stat.rx_rate,
                              node_stat.commands_stat.ell_net_write_rate)
        self.ell_write_rate = min(node_stat.rx_rate,
                                  node_stat.commands_stat.ell_net_write_rate)
        self.ext_read_rate = self.read_rate - self.ell_read_rate
        self.ext_write_rate = self.write_rate - self.ell_write_rate


class DiskLoad(object):
    def __init__(self, fs_stat):
        self.disk_util = fs_stat.disk_util
        self.disk_util_read = fs_stat.disk_util_read
        self.disk_util_write = fs_stat.disk_util_write

        self.read_rate = max(fs_stat.disk_read_rate,
                             fs_stat.commands_stat.ell_disk_read_rate)
        self.ell_read_rate = min(fs_stat.disk_read_rate,
                                 fs_stat.commands_stat.ell_disk_read_rate)

        self.write_rate = max(fs_stat.disk_write_rate,
                              fs_stat.commands_stat.ell_disk_write_rate)
        self.ell_write_rate = min(fs_stat.disk_write_rate,
                                  fs_stat.commands_stat.ell_disk_write_rate)

        self.ext_read_rate = self.read_rate - self.ell_read_rate
        self.ext_write_rate = self.write_rate - self.ell_write_rate
