# encoding: utf-8
from copy import deepcopy
import json

manifest = {'config': '/etc/elliptics/mastermind.conf'}

with open(manifest["config"], 'r') as config_file:
    config = json.load(config_file)

params = {}

try:
    params = deepcopy(config['cache']['transport'])
    mod, obj = params.pop('class').rsplit('.', 1)
    transport = __import__(mod, fromlist=[obj])
    Transport = getattr(transport, obj)

except (ImportError, KeyError):
    from fake_transport import Transport

for k, v in params.iteritems():
    if isinstance(v, unicode):
        params[k] = v.encode('utf-8')


transport = Transport(**params)
