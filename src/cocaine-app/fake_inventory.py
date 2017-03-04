# encoding: utf-8
import socket


def get_dc_by_host(addr):
    '''
    This is a fake implementation that always returns hostname.
    Please provide your own version that uses your server management framework.
    '''
    host = socket.gethostbyaddr(addr)[0]
    return host


def get_host_tree(host):
    '''
    This is a fake implementation that always one-level host infrastructure tree.
    Please provide your own version that uses your server management framework.

    Return format example:
    {
        'name': 'hostname.domain.com',
        'type': 'host',
        'parent': {
            'name': 'alpha',
            'type': 'dc',
        }
    }

    Outer level type 'host' is mandatory, parents' types are voluntary.
    '''
    return {
        'name': host,
        'type': 'host',
    }


DC_NODE_TYPE = 'host'
BALANCER_NODE_TYPES = [DC_NODE_TYPE]


def get_balancer_node_types():
    '''
    A list of node types that are used by balancer to create fault-tolerant
    namespaces. When creating new couple for a namespace balancer takes into
    account the current distribution of open couples and tries to use
    cluster nodes that are least used by the namespace.

    All node types used should be presented in a host tree for of a host
    (inventory get_host_tree function).

    Example: ['dc', 'host']
    '''
    return BALANCER_NODE_TYPES


def get_node_types():
    '''
    A list of infrastructure node types.
    Node types represent hardware hierarchy and are used to build cluster tree.
    Each node type represent a corresponding level of the cluster tree.
    NOTE: node types should be sorted in top-to-bottom order, e.g ['dc', 'router', 'host'].
    '''
    return [DC_NODE_TYPE]


def get_dc_node_type():
    '''
    Returns dc node type.
    Mastermind should know the dc node type identificator to prevent
    dc sharing among couples if corresponding setting is on.

    Example: 'dc'
    '''
    return DC_NODE_TYPE


def node_start_command(host, port, family):
    '''
    Starting elliptics node is too complex to provide a fake implementation for.
    If you really want to be able to use this functionality, you should
    provide your own implementation that uses your server management framework.
    '''
    return None


def node_shutdown_command(host, port, family):
    '''
    This is a fake implementation that shuts node down via dnet_ioclient command.
    Please provide your own version that uses your server management framework
    '''
    cmd = 'dnet_ioclient -r {host}:{port}:{family} -U 1'
    return cmd.format(host=host, port=port, family=family)


def node_reconfigure(host, port, family):
    '''
    Command that is executed on elliptics node for elliptics configs regeneration.
    E. g., reconfiguration is required for backend restart with updated group id.
    '''
    return None


def set_net_monitoring_downtime(host):
    '''
    If your infrastructure monitors network activity, it can cause alerts
    on every move/restore job that involves a certain host. This inventory
    function allows you to implement network activity downtime setting
    for the running time of rsync command.
    NB: This function should not throw an exception if net monitoring downtime
    is already set.
    '''
    return None


def remove_net_monitoring_downtime(host):
    '''
    See "set_net_monitoring_downtime" doc string.
    '''
    return None


def get_host_ip_addresses(hostname):
    '''
    Resolves hostname to ip(v6) addresses

    Mastermind will preferably use address with a family corresponding
    to elliptics client connection settings.

    Returns:
        {
            socket.AF_INET: [
                '1.2.3.4',
                '5.6.7.8',
            ],
            socket.AF_INET6: [
                '2001:db8:0:1',
            ]
        }
    '''
    ip_addresses = {}
    host, port, family, socktype = hostname, None, socket.AF_UNSPEC, socket.SOL_TCP
    records = socket.getaddrinfo(host, port, family, socktype)
    for record in records:
        # record format is (family, socktype, proto, canonname, sockaddr),
        # sockaddr format depends on family of the socket:
        # socket.AF_INET - (address, port),
        # socket.AF_INET6 - (address, port, flow info, scope id).
        # See docs for more info: https://docs.python.org/2/library/socket.html#socket.getaddrinfo
        family, sockaddr = record[0], record[4]
        ip_address = sockaddr[0]
        ip_addresses.setdefault(family, []).append(ip_address)
    return ip_addresses


def get_new_group_files(group_id, total_space):
    '''
    Get files required for the new group to be created

    Files will be created on a filesystem in group's base directory by mastermind-minion.
    They can be helpful if elliptics is configured by automatic scripts
    that examine the contents of group's base directory.
    Filename should be relative to group's base directory.

    Returns:
        {
            <filename1>: <file content>,
            <filename2>: <file content>,
            ...
        }
    '''
    return {}


def get_node_config_path(node):
    '''
    Get path to config file of node <node>

    This config path can be used by mastermind-minion for fetching
    any elliptics config parameters.
    '''
    return '/etc/elliptics/elliptics.conf'


def make_external_storage_convert_command(dst_groups,
                                          groupset_type,
                                          groupset_settings,
                                          src_storage,
                                          src_storage_options,
                                          trace_id=None):
    '''
    Construct command that will be executed by minion to convert @src_storage data unit
    to a @groupset.

    Parameters:
        @dst_groups (list of lists of storage.Group): a list of destination groups that
            will store converted data, where each nested list contains groups for some
            new groupset.
        @groupset_type: destination groupset type
        @groupset_settings (dict): destination groupset settings
        @src_storage (str): source storage id
        @src_storage_options (dict): source storage options (data unit to convert, etc.)
        @trace_id (str): trace id that should be used by the command if tracing is
            implemented

    '''
    raise NotImplementedError()


def make_external_storage_validate_command(couple_id,
                                           dst_groups,
                                           groupset_type,
                                           groupset_settings,
                                           src_storage,
                                           src_storage_options,
                                           check_dst_groups=None,
                                           additional_backends=None,
                                           trace_id=None):
    '''
    Construct command that will be executed by minion to validate data converted from
    @src_storage to a @groupset.

    Parameters:
        @couple_id (int): id of a couple that will store converted data
        @dst_groups (list of lists of storage.Group): a list of destination groups that
            store converted data, where each nested list contains groups for some new
            groupset.
        @groupset_type: destination groupset type
        @groupset_settings (dict): destination groupset settings
        @src_storage (str): source storage id
        @src_storage_options (dict): source storage options (data unit to convert, etc.)
        @trace_id (str): trace id that should be used by the command if tracing is
            implemented
        @check_dst_groups (list of lists of storage.Group): a list of original destination
            groups that were used during converting, where each nested list contains groups
            for some new groupset. This list will be validated against meta data in lrc groupset
            if not None, otherwise dst_groups will be used
        @additional_backends (list): additional backends to use for validation tasks
            (like validation after groupset restore when group was moved to another host)

    '''
    raise NotImplementedError()


def make_external_storage_data_size_command(groupset_type,
                                            groupset_settings,
                                            src_storage,
                                            src_storage_options,
                                            trace_id=None):
    '''
    Construct command that will be executed by minion to determine @src_storage data size.

    Parameters:
        @groupset_type: destination groupset type
        @groupset_settings (dict): destination groupset settings
        @src_storage (str): source storage id
        @src_storage_options (dict): source storage options (data unit to convert, etc.)
        @trace_id (str): trace id that should be used by the command if tracing is
            implemented

    '''
    raise NotImplementedError()


def is_external_storage_ready(src_storage, src_storage_options, src_storage_id, convert_queue):
    '''
    Check if external storage is ready to be converted.

    Parameters:
        @src_storage: type of source storage
        @src_storage_options: source storage options
        @src_storage_id: source storage id to check
        @convert_queue: mongo collection to implement custom check logic
    '''
    return True


def external_storage_task_retry_ts(task, src_storage, src_storage_options):
    '''
    Get the next retry attempt timestamp for the task.

    Parameters:
        @task: failed task
        @src_storage: type of source storage
        @src_storage_options: source storage options
    '''
    return None


def is_external_storage_task_ready_for_retry(task, src_storage, src_storage_options, storage, processor):
    '''
    Determine if task is ready to be retried.

    Parameters:
        @task: failed task
        @src_storage: type of source storage
        @src_storage_options: source storage options
        @storage: storage module
        @processor: instance of job processor
    '''
    return True
