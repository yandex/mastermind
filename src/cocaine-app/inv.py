import helpers as h
from importer import import_object
from mastermind_core.config import config


try:
    inv = import_object(config['inventory'])
except (ImportError, KeyError):
    import fake_inventory as inv


class Inventory(object):
    """General interface to inventory specification

    Inventory is designed as a general tool to provide
    information about specific infrastructure environment
    to mastermind.

    Inventory implementation can be provided using
    'inventory' config section using module name as a value
    (NB: this module should be importable by python interpreter).
    If either custom inventory is not provided or is not importable
    mastermind will fall back to default implementation of
    @fake_inventory module.

    Inventory API is fully described in @fake_inventory module.

    TODO: move API description to @Inventory class;
    TODO: add support of all inventory methods as worker handles.
    """
    @staticmethod
    @h.concurrent_handler
    def get_dc_by_host(host):
        return inv.get_dc_by_host(host)
