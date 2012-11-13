#!/usr/bin/python

import sys, traceback
from cocaine.client import Client
import msgpack

import xml.dom.minidom
import urllib2
import socket

def get_dc_by_host(addr):
    host = socket.gethostbyaddr(addr)[0]
    hostxml = urllib2.urlopen("http://c.yandex-team.ru/api/hosts/" + host)
    hostinfo = xml.dom.minidom.parse(hostxml)
    return hostinfo.getElementsByTagName('data')[0].getElementsByTagName('item')[0].getElementsByTagName('root_datacenter')[0].firstChild.data

#print get_dc_by_host("elisto18f.dev.yandex.net")

c = Client('/home/toshik/cocaine_config.json')

id = [x for x in xrange(64)]
res = c.get('balancer/balance', {'meta': {}, 'request': (2, 1, id)})
print msgpack.unpackb(res[0])
#print balancer.get_groups(n)

res = c.get('balancer/get_groups', {'meta': {}, 'request': ''})
print msgpack.unpackb(res[0])

res = c.get('balancer/get_symmetric_groups', {'meta': {}, 'request': ''})
print msgpack.unpackb(res[0])

res = c.get('balancer/get_bad_groups', {'meta': {}, 'request': ''})
print msgpack.unpackb(res[0])

res = c.get('balancer/get_empty_groups', {'meta': {}, 'request': ''})
print msgpack.unpackb(res[0])

