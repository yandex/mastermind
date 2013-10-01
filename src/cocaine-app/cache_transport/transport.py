# encoding: utf-8
from copy import deepcopy
import json

from importer import import_object


manifest = {'config': '/etc/elliptics/mastermind.conf'}

with open(manifest["config"], 'r') as config_file:
    config = json.load(config_file)

params = {}

try:
    params = deepcopy(config['cache']['transport'])
    Transport = import_object(params.pop('class'))
except (ImportError, KeyError):
    from fake_transport import Transport


def encode_dict(params):
    return dict([(k, v if not isinstance(v, unicode) else v.encode('utf-8'))
                 for k, v in params.iteritems()])

transport = Transport(**encode_dict(params))
