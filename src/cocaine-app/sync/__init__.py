# encoding: utf-8
from copy import deepcopy
import json
import logging

from config import config
from importer import import_object


logger = logging.getLogger('mm.sync')

params = {}

try:
    params = deepcopy(config['sync'])
    SyncManager = import_object(params.pop('class'))
except (ImportError, KeyError) as e:
    logger.error(e)
    from fake_sync import SyncManager


logger.info('Sync manager being used: {0}'.format(SyncManager))
sync_manager = SyncManager(**params)
