from datetime import datetime
import math
from time import time


MINUTE = 60
HOUR = 60 * 60
DAY = 24 * 60 * 60


def periodic_timer(seconds=0, minutes=0, hours=0, days=0):
    if not seconds and not minutes and not hours and not days:
        raise ValueError('Timer period should be configured')
    period = (days * DAY + hours * HOUR + minutes * MINUTE + seconds)
    while True:
        yield math.ceil(time() / period) * period


def periodic_datetimer(**kwargs):
    timer = periodic_timer(**kwargs)
    while True:
        yield datetime.fromtimestamp(timer.next())
