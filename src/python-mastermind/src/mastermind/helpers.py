
def elliptics_time_to_ts(t):
    if isinstance(t, dict) and 'tv_sec' in t:
        return t['tv_sec'] + t.get('tv_usec', 0) / float(10 ** 6)
    elif hasattr(t, 'tsec'):
        # instance of elliptics.Time
        return t.tsec + t.tnsec / float(10 ** 9)
    raise TypeError('Invalid elliptics time object: {}'.format(t))
