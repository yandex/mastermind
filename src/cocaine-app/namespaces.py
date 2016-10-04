import logging

from config import config
from db.mongo.pool import Collection
from mastermind_core.namespaces.settings import NamespaceSettings
import storage


logger = logging.getLogger('mm.namespaces')


class NamespacesSettings(object):
    def __init__(self, meta_db):
        try:
            keys_db_uri = config['metadata']['namespaces']['db']
        except KeyError:
            logger.error('Config parameter metadata.namespaces.db is required '
                         'for namespace settings')
            self._settings_db = None
        else:
            self._settings_db = Collection(meta_db[keys_db_uri], 'settings')

        self._cache = {}

    @property
    def settings_db(self):
        if self._settings_db is None:
            raise RuntimeError('Namespace settings database is not set up')
        return self._settings_db

    def _on_save(self, ns_settings, dirty):
        if not dirty:
            return

        res = self.settings_db.update(
            spec={'namespace': ns_settings.namespace},
            document=ns_settings.dump(),
            upsert=True,
        )
        if 'ok' not in res or res['ok'] != 1:
            logger.error(
                'Failed to save namespace settings for namespace "{}": {}'.format(
                    ns_settings.namespace,
                    res,
                )
            )
            raise ValueError(
                'Failed to save namespace settings for namespace "{}"'.format(
                    ns_settings.namespace
                )
            )

    def make(self, namespace_id):
        ns_settings = NamespaceSettings(
            {
                'namespace': namespace_id,
            },
            on_save_callback=self._on_save,
        )
        return ns_settings

    def fetch(self):
        namespaces_settings = []
        for settings_dump in self.settings_db.find({}, fields={'_id': False}):
            try:
                ns_settings = NamespaceSettings(
                    settings_dump,
                    on_save_callback=self._on_save,
                )
                namespaces_settings.append(ns_settings)
                self._cache[ns_settings.namespace] = ns_settings
            except Exception:
                logger.exception('Failed to construct namespace settings object')
                continue
        return namespaces_settings

    def get(self, namespace_id):
        settings_dump = self.settings_db.find_one(
            spec_or_id={'namespace': namespace_id},
            fields={'_id': False}
        )
        if not settings_dump:
            raise ValueError('Namespace "{}" is not found'.format(namespace_id))
        ns_settings = NamespaceSettings(
            settings_dump,
            on_save_callback=self._on_save,
        )
        self._cache[ns_settings.namespace] = ns_settings
        return ns_settings

    def get_cached(self, namespace_id):
        if namespace_id not in self._cache:
            raise ValueError('Namespace "{}" is not found'.format(namespace_id))
        return self._cache[namespace_id]
