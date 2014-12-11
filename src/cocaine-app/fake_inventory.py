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
