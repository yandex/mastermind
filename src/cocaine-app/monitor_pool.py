import elliptics

from mastermind.pool import Pool
from mastermind.monitor_pool import MonitorStatParseWorker

from config import config


MONITOR_CFG = config.get('monitor', {})

MONITOR_STAT_CATEGORIES = (
    elliptics.monitor_stat_categories.procfs |
    elliptics.monitor_stat_categories.backend |
    elliptics.monitor_stat_categories.io |
    elliptics.monitor_stat_categories.stats |
    elliptics.monitor_stat_categories.commands
)

monitor_pool = Pool(
    worker=MonitorStatParseWorker,
    w_initkwds={
        'max_http_clients': MONITOR_CFG.get('max_http_clients', 30),
        'monitor_stat_categories': MONITOR_STAT_CATEGORIES,
        'monitor_port': config.get('elliptics', {}).get('monitor_port', 10025),
        'connect_timeout': MONITOR_CFG.get('connect_timeout', 5.0),
        'request_timeout': MONITOR_CFG.get('request_timeout', 5.0),
    },
    processes=MONITOR_CFG.get('pool_size', 5),
)
