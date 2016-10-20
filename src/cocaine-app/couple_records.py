from mastermind_core import helpers
from mastermind_core.config import config
from mastermind_core.db.mongo import MongoObject
from mastermind_core.db.mongo.pool import Collection
import storage


class CoupleRecord(MongoObject):

    ID = 'id'
    SETTINGS = 'settings'

    PRIMARY_ID_KEY = ID

    def __init__(self, **init_params):
        super(CoupleRecord, self).__init__()

        self.couple_id = init_params[self.ID]
        self.settings = init_params.get(self.SETTINGS, {})

    @property
    def id(self):
        return self.couple_id

    @classmethod
    def new(cls, **kwargs):
        couple_record = cls(**kwargs)
        couple_record._dirty = True
        return couple_record

    def set_settings(self, settings, update=True):
        if update:
            old_settings = self.settings
        else:
            old_settings = {}
        new_settings = helpers.merge_dict(old_settings, settings)
        CoupleSettingsValidator.validate_settings(new_settings)
        self.settings = new_settings
        self._dirty = True

    def dump(self):
        return {
            'id': self.id,
            'settings': self.settings,
        }


class CoupleSettingsValidator(object):

    @staticmethod
    def validate_settings(settings):
        if storage.Couple.READ_PREFERENCE not in settings:
            raise ValueError('Couple requires "read_preference" setting')
        for setting_name, setting in settings.iteritems():
            if setting_name == storage.Couple.READ_PREFERENCE:
                CoupleSettingsValidator._validate_read_preference(
                    settings[storage.Couple.READ_PREFERENCE]
                )
            else:
                raise ValueError('Invalid couple setting: "{}"'.format(setting_name))

    @staticmethod
    def _validate_read_preference(rp_settings):
        if not isinstance(rp_settings, (tuple, list)):
            raise ValueError('"read_preference" is "{type}", expected a list or a tuple'.format(
                type=type(rp_settings).__name__,
            ))

        unique_rp = set()
        for read_preference in rp_settings:
            if read_preference not in storage.GROUPSET_IDS:
                raise ValueError('Invalid groupset id: {}'.format(read_preference))
            if read_preference in unique_rp:
                raise ValueError(
                    'Read preference should contain unique list of groupset ids, '
                    'value "{value}" is found more than once'.format(
                        value=read_preference,
                    )
                )
            unique_rp.add(read_preference)


class CoupleRecordNotFoundError(Exception):
    pass


class CoupleRecordFinder(object):
    def __init__(self, db):
        self.collection = Collection(db[config['metadata']['couples']['db']], 'couples')

    def _get_couple_record(self, couple):
        couple_id = couple.as_tuple()[0]
        couple_records_list = (
            self.collection
            .list(**{CoupleRecord.ID: couple_id})
            .limit(1)
        )
        try:
            couple_record = CoupleRecord(**couple_records_list[0])
        except IndexError:
            raise CoupleRecordNotFoundError(
                'Couple record for couple {} is not found'.format(couple_id)
            )
        couple_record.collection = self.collection
        return couple_record

    def couple_record(self, couple):
        try:
            return self._get_couple_record(couple)
        except CoupleRecordNotFoundError:
            couple_id = couple.as_tuple()[0]
            couple_record = CoupleRecord.new(**{CoupleRecord.ID: couple_id})
            couple_record.collection = self.collection
            return couple_record

    def couple_records(self, ids=None):
        records = [
            CoupleRecord(**gh)
            for gh in self.collection.list(id=ids)
        ]
        for r in records:
            r.collection = self.collection
            r._dirty = False
        return records
