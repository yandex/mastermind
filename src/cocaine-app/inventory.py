import json

from config import config
from importer import import_object


try:
    inv = import_object(config['inventory'])
except (ImportError, KeyError):
    import fake_inventory as inv

get_dc_by_host = inv.get_dc_by_host
get_host_tree = inv.get_host_tree
node_shutdown_command = inv.node_shutdown_command
node_start_command = inv.node_start_command
