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


def dnet_client_backend_command(command):
    def wrapper(host, port, family, backend_id):
        cmd = 'dnet_client backend -r {host}:{port}:{family} {command} --backend {backend_id}'
        return cmd.format(command=command,
            host=host, port=port, family=family, backend_id=backend_id)
    return wrapper


enable_node_backend_cmd = dnet_client_backend_command('enable')
disable_node_backend_command = dnet_client_backend_command('disable')


def node_reconfigure(host, port, family):
    '''
    Command that is executed on elliptics node for elliptics configs regeneration.
    E. g., reconfiguration is required for backend restart with updated group id.
    '''
    return None
