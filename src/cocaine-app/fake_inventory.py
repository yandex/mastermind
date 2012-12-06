# encoding: utf-8
import socket

'''
    This is a fake implementation that always returns hostname.
    Please write your own version that uses your server management framework.
'''
def get_dc_by_host(addr):
    host = socket.gethostbyaddr(addr)[0]
    return host

