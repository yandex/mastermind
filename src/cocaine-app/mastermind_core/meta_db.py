# coding=utf-8
from __future__ import unicode_literals

from mastermind_core.config import METADATA_OPTIONS, METADATA_URL
from mastermind_core.db.mongo.pool import MongoReplicaSetClient


def init_meta_db():
    return MongoReplicaSetClient(METADATA_URL, **METADATA_OPTIONS) if METADATA_URL else None


meta_db = init_meta_db()
