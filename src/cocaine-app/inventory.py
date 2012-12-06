# encoding: utf-8

import json
#from cocaine.context import Log, manifest
def manifest():
    return {"config" : "/etc/elliptics/mastermind.config"}


with open(manifest()["config"], 'r') as config_file:
    config = json.load(config_file)

if 'inventory' in config:
    inventory = __import__(config['inventory'])
else:
    inventory = __import__('fake_inventory')
    

def get_dc_by_host(host):
    return inventory.get_dc_by_host(host)
    
