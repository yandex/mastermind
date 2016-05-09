class CacheUpstreamError(Exception):
    """
    Indicates that upstream request failed.
    By default original exception is logged by caching facilities.
    """
    pass


ELLIPTICS_NOT_FOUND = -2
ELLIPTICS_GROUP_NOT_IN_ROUTE_LIST = -6
ELLIPTICS_TIMEOUT = -110
