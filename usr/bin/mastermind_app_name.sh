#!/usr/bin/env python
import json
import sys


CONFIG_PATH = '/etc/elliptics/mastermind.conf'


def main():
    try:

        with open(CONFIG_PATH, 'r') as config_file:
            config = json.load(config_file)

    except Exception as e:
        # raise ValueError('Failed to load config file %s: %s' % (CONFIG_PATH, e))
        config = {}

    sys.stdout.write(config.get('app_name', 'mastermind2.26'))


if __name__ == '__main__':
    main()
