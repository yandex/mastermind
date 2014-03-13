# encoding: utf-8
import socket


def get_dc_by_host(addr):
    '''
    This is a fake implementation that always returns hostname.
    Please write your own version that uses your server management framework.
    '''
    host = socket.gethostbyaddr(addr)[0]
    return host

def get_host_tree(host):
    '''
    This is a fake implementation that always one-level host infrastructure tree.
    Please write your own version that uses your server management framework.

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
