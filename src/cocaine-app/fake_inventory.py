# encoding: utf-8
import socket


def get_dc_by_host(addr):
    '''
    This is a fake implementation that always returns hostname.
    Please write your own version that uses your server management framework.
    '''
    host = socket.gethostbyaddr(addr)[0]
    return host
