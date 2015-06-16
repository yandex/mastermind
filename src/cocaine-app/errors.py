

class MinionApiError(Exception):
    pass

class NotReadyError(Exception):
    pass

class CacheUpstreamError(Exception):
    """
    Indicates that upstream request failed.
    By default original exception is logged by caching facilities.
    """
    pass
