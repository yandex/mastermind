import json

with open('/etc/elliptics/mastermind.conf', 'r') as config_file:
    config = json.load(config_file)
