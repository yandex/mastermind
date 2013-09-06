# encoding: utf-8

import json

manifest = {'config': '/etc/elliptics/mastermind.conf'}

with open(manifest["config"], 'r') as config_file:
    config = json.load(config_file)

try:
    inv = __import__(config['inventory'])
except (ImportError, KeyError):
    import fake_inventory as inv

get_dc_by_host = inv.get_dc_by_host
