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
