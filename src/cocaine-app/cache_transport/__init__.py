# encoding: utf-8
from copy import deepcopy
import json
import logging

from config import config
from importer import import_object


logger = logging.getLogger('mm.sync')

params = {}

try:
    params = deepcopy(config['cache']['manager'])
    CacheTaskManager = import_object(params.pop('class'))
except (ImportError, KeyError) as e:
    logger.error(e)
    from fake_transport import Transport as CacheTaskManager


logger.info('Cache task manager being used: {0}'.format(CacheTaskManager))
cache_task_manager = CacheTaskManager(**params)
