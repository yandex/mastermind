from mastermind_core.config import config
from mastermind_core.db.mongo.pool import MongoReplicaSetClient


mrsc_options = config['metadata'].get('options', {})

meta_db = None
if config['metadata'].get('url'):
    meta_db = MongoReplicaSetClient(config['metadata']['url'], **mrsc_options)
