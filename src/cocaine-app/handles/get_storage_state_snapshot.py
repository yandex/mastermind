import json
import logging
import time

from handles import Handle
from handles.builder import FlatbuffersBuilder
import helpers as h
from mastermind_core.flatbuffers.mmc import Couple
from mastermind_core.flatbuffers.mmc import Groupset, LrcSettings
from mastermind_core.flatbuffers.mmc import GroupsetType, GroupsetStatus, GroupsetSettings
from mastermind_core.flatbuffers.mmc import Namespace, Statistics, NamespaceSettings, WritePolicy
from mastermind_core.flatbuffers.mmc import CoupleWeight
from mastermind_core.flatbuffers.mmc import UnitToCouples
from mastermind_core.flatbuffers.mmc import StorageInfo
from mastermind_core.response import CachedGzipResponse
import storage


logger = logging.getLogger('mm.handle.get_storage_state_snapshot')


class GetStorageStateSnapshotHandle(Handle):

    def __init__(self, *args, **kwargs):
        super(GetStorageStateSnapshotHandle, self).__init__(*args, **kwargs)
        self._response = CachedGzipResponse()

    @h.handler_wne
    def __call__(self, request):
        request = request or {}
        return self.get_storage_state_snapshot_flatbuffers(compressed=request.get('gzip', False))

    def get_storage_state_snapshot_flatbuffers(self, compressed=False):
        return self._response.get_result(compressed=compressed)

    def update(self,
               namespaces_settings,
               weight_manager,
               namespaces_statistics,
               external_storage_mapping,
               timestamp=None):

        try:
            builder = StorageStateSnapshotFlatbuffersBuilder(
                namespaces_settings=namespaces_settings,
                weight_manager=weight_manager,
                namespaces_statistics=namespaces_statistics,
                external_storage_mapping=external_storage_mapping,
                timestamp=timestamp or time.time(),
            )
            self._response.set_result(builder.build(), ts=timestamp)
        except Exception as e:
            self._response.set_exception(e)
            pass


class StorageStateSnapshotFlatbuffersBuilder(FlatbuffersBuilder):

    def __init__(self,
                 namespaces_settings,
                 weight_manager,
                 namespaces_statistics,
                 external_storage_mapping,
                 timestamp,
                 *args,
                 **kwargs):

        super(StorageStateSnapshotFlatbuffersBuilder, self).__init__(*args, **kwargs)

        self.namespaces_settings = {
            ns_settings.namespace: ns_settings
            for ns_settings in namespaces_settings
        }
        self.weights = weight_manager.weights
        self.namespaces_statistics = namespaces_statistics
        self.external_storage_mapping = external_storage_mapping

        self.timestamp = timestamp

    def _save_lrc_groupset_settings(self, groupset):
        settings = groupset.groupset_settings

        fb_scheme_offset = self._save_string(settings['scheme'])

        LrcSettings.LrcSettingsStart(self.builder)
        LrcSettings.LrcSettingsAddScheme(self.builder, fb_scheme_offset)
        LrcSettings.LrcSettingsAddPartSize(self.builder, settings['part_size'])
        return LrcSettings.LrcSettingsEnd(self.builder)

    def _save_groupset_group_ids(self, groupset):
        group_ids = tuple(group.group_id for group in groupset.groups)
        Groupset.GroupsetStartGroupIdsVector(self.builder, len(group_ids))
        for i in reversed(group_ids):
            self.builder.PrependUint32(i)
        return self.builder.EndVector(len(group_ids))

    @staticmethod
    def convert_groupset_type(type_str):
        if type_str == storage.GROUPSET_REPLICAS:
            return GroupsetType.GroupsetType.NATIVE
        return GroupsetType.GroupsetType.__dict__.get(
            type_str.upper(),
            GroupsetType.GroupsetType.UNKNOWN
        )

    @staticmethod
    def convert_groupset_status(status_str):
        return (
            GroupsetStatus.GroupsetStatus.BAD
            if status_str == 'BAD' else
            GroupsetStatus.GroupsetStatus.OK
        )

    def _save_groupset(self, groupset):
        fb_groupset_type = self.convert_groupset_type(groupset.type)

        if fb_groupset_type == GroupsetType.GroupsetType.NATIVE:
            fb_settings_type = GroupsetSettings.GroupsetSettings.NativeSettings
            fb_settings_offset = None
        elif fb_groupset_type == GroupsetType.GroupsetType.LRC:
            fb_settings_type = GroupsetSettings.GroupsetSettings.LrcSettings
            fb_settings_offset = self._save_lrc_groupset_settings(groupset)
        else:
            raise ValueError('Unsupported groupset type {}'.format(groupset.type))

        fb_id_offset = self._save_string(str(groupset))
        fb_group_ids_offset = self._save_groupset_group_ids(groupset)

        Groupset.GroupsetStart(self.builder)
        Groupset.GroupsetAddGroupIds(self.builder, fb_group_ids_offset)
        if fb_settings_offset is not None:
            Groupset.GroupsetAddSettingsType(self.builder, fb_settings_type)
            Groupset.GroupsetAddSettings(self.builder, fb_settings_offset)
        Groupset.GroupsetAddStatus(self.builder, self.convert_groupset_status(groupset.status))
        Groupset.GroupsetAddId(self.builder, fb_id_offset)
        Groupset.GroupsetAddType(self.builder, fb_groupset_type)
        return Groupset.GroupsetEnd(self.builder)

    # Couple

    def _save_groupsets(self, couple):
        fb_groupset_offsets = []

        has_replicas_groupset = any(
            nb.status != storage.Status.INIT
            for g in couple.groups
            for nb in g.node_backends
        )

        if has_replicas_groupset:
            # NOTE: fix when replicas groupset is moved to a separate object
            replicas_groupset = couple
            fb_groupset_offsets.append(
                self._save_groupset(replicas_groupset)
            )

        if couple.lrc822v1_groupset:
            fb_groupset_offsets.append(
                self._save_groupset(couple.lrc822v1_groupset)
            )

        Couple.CoupleStartGroupsetsVector(self.builder, len(fb_groupset_offsets))
        for i in reversed(fb_groupset_offsets):
            self.builder.PrependUOffsetTRelative(i)
        return self.builder.EndVector(len(fb_groupset_offsets))

    def _save_read_preference(self, couple):

        fb_read_preference_offsets = self._save_strings(
            couple.settings[storage.Couple.READ_PREFERENCE]
        )

        Couple.CoupleStartReadPreferenceVector(self.builder, len(fb_read_preference_offsets))
        for i in reversed(fb_read_preference_offsets):
            self.builder.PrependUOffsetTRelative(i)
        return self.builder.EndVector(len(fb_read_preference_offsets))

    def _save_couple(self, couple):

        fb_groupsets_offsets = self._save_groupsets(couple)
        fb_read_preference_offset = self._save_read_preference(couple)
        fb_status_offset = self._save_string(couple.status)

        Couple.CoupleStart(self.builder)
        Couple.CoupleAddGroupsets(self.builder, fb_groupsets_offsets)
        Couple.CoupleAddReadPreference(self.builder, fb_read_preference_offset)
        Couple.CoupleAddFreeEffectiveSpace(self.builder, couple.effective_free_space)
        Couple.CoupleAddFreeReservedSpace(self.builder, couple.free_reserved_space)
        Couple.CoupleAddStatus(self.builder, fb_status_offset)
        # TODO: change to 'couple_id' property
        Couple.CoupleAddId(self.builder, couple.groups[0].group_id)
        return Couple.CoupleEnd(self.builder)

    # Namespace

    @staticmethod
    def _convert_success_copies_num(value_str):
        return WritePolicy.WritePolicy.__dict__.get(value_str, WritePolicy.WritePolicy.all)

    def _save_ns_settings_static_couple(self, ns_settings):
        group_ids = ns_settings.static_couple or []
        NamespaceSettings.NamespaceSettingsStartStaticCoupleVector(self.builder, len(group_ids))
        for group_id in group_ids:
            self.builder.PrependUint32(group_id)
        return self.builder.EndVector(len(group_ids))

    def _save_ns_settings(self, ns_settings):
        fb_user_settings_offset = self._save_string(json.dumps(ns_settings._settings))
        fb_success_copies_num = self._convert_success_copies_num(ns_settings.success_copies_num)
        fb_static_couple_offset = self._save_ns_settings_static_couple(ns_settings)

        NamespaceSettings.NamespaceSettingsStart(self.builder)
        NamespaceSettings.NamespaceSettingsAddUserSettings(self.builder, fb_user_settings_offset)
        NamespaceSettings.NamespaceSettingsAddStaticCouple(self.builder, fb_static_couple_offset)
        NamespaceSettings.NamespaceSettingsAddSuccessCopiesNum(self.builder, fb_success_copies_num)
        NamespaceSettings.NamespaceSettingsAddReplicaCount(self.builder, ns_settings.groups_count)
        return NamespaceSettings.NamespaceSettingsEnd(self.builder)

    def _save_ns_couples(self, namespace):

        couples = namespace.couples

        couple_offsets = [self._save_couple(couple) for couple in couples]

        Namespace.NamespaceStartCouplesVector(self.builder, len(couple_offsets))
        for i in reversed(couple_offsets):
            self.builder.PrependUOffsetTRelative(i)

        logger.info(
            'Storage state: namespace {}, packed {} couples total'.format(namespace.id, len(couple_offsets))
        )

        return self.builder.EndVector(len(couple_offsets))

    def _save_ns_couple_weight_group_ids(self, group_ids):
        Groupset.GroupsetStartGroupIdsVector(self.builder, len(group_ids))
        for i in reversed(group_ids):
            self.builder.PrependUint32(i)

        return self.builder.EndVector(len(group_ids))

    def _save_ns_couple_weight(self, w):
        group_ids, weight, effective_free_space = w
        couple_id = group_ids[0]

        fb_group_ids_offset = self._save_ns_couple_weight_group_ids(group_ids)

        CoupleWeight.CoupleWeightStart(self.builder)
        CoupleWeight.CoupleWeightAddMemory(self.builder, effective_free_space)
        CoupleWeight.CoupleWeightAddWeight(self.builder, weight)
        CoupleWeight.CoupleWeightAddGroupIds(self.builder, fb_group_ids_offset)
        CoupleWeight.CoupleWeightAddCoupleId(self.builder, couple_id)
        return CoupleWeight.CoupleWeightEnd(self.builder)

    def _save_ns_couple_weights(self, namespace, ns_settings):
        weights = self.weights.get(namespace.id, {})

        ns_groups_count = ns_settings.groups_count

        fb_couple_weight_offsets = [
            self._save_ns_couple_weight(w)
            for w in weights.get(ns_groups_count, [])
        ]
        Namespace.NamespaceStartCoupleWeightsVector(self.builder, len(fb_couple_weight_offsets))
        for i in reversed(fb_couple_weight_offsets):
            self.builder.PrependUOffsetTRelative(i)

        logger.info(
            'Storage state: namespace {}, packed {} couples for write'.format(namespace.id, len(fb_couple_weight_offsets))
        )

        return self.builder.EndVector(len(fb_couple_weight_offsets))

    def _save_ns_statistics(self, namespace):
        statistics = self.namespaces_statistics.get(namespace.id, {})

        fb_raw_data_offset = self._save_string(json.dumps(statistics))

        Statistics.StatisticsStart(self.builder)
        Statistics.StatisticsAddRawData(self.builder, fb_raw_data_offset)
        Statistics.StatisticsAddIsFull(self.builder, statistics.get('is_full', False))
        return Statistics.StatisticsEnd(self.builder)

    def _save_namespace(self, namespace):
        fb_id_offset = self._save_string(namespace.id)
        fb_couples_offset = self._save_ns_couples(namespace)

        ns_settings = self.namespaces_settings[namespace]
        fb_settings_offset = self._save_ns_settings(ns_settings)
        fb_statistics_offset = self._save_ns_statistics(namespace)
        fb_weights_offset = self._save_ns_couple_weights(namespace, ns_settings)

        Namespace.NamespaceStart(self.builder)
        Namespace.NamespaceAddCoupleWeights(self.builder, fb_weights_offset)
        Namespace.NamespaceAddStatistics(self.builder, fb_statistics_offset)
        Namespace.NamespaceAddSettings(self.builder, fb_settings_offset)
        Namespace.NamespaceAddCouples(self.builder, fb_couples_offset)
        Namespace.NamespaceAddDeleted(self.builder, ns_settings.deleted)
        Namespace.NamespaceAddName(self.builder, fb_id_offset)
        return Namespace.NamespaceEnd(self.builder)

    def _save_unit_mapping_couple_ids(self, mapping):
        UnitToCouples.UnitToCouplesStartCoupleIdsVector(self.builder, len(mapping['couples']))
        for couple_id in reversed(mapping['couples']):
            self.builder.PrependUint32(couple_id)
        return self.builder.EndVector(len(mapping['couples']))

    def _save_unit_mapping(self, mapping):
        fb_service_prefix_offset = self._save_string(
            mapping['external_storage_options']['mulca_service_stid_prefix'],
            shared=True,
        )
        fb_couple_ids_offset = self._save_unit_mapping_couple_ids(mapping)

        UnitToCouples.UnitToCouplesStart(self.builder)
        UnitToCouples.UnitToCouplesAddCoupleIds(self.builder, fb_couple_ids_offset)
        UnitToCouples.UnitToCouplesAddServicePrefix(self.builder, fb_service_prefix_offset)
        UnitToCouples.UnitToCouplesAddUnitId(self.builder, int(mapping['external_storage_options']['mulca_unit']))
        return UnitToCouples.UnitToCouplesEnd(self.builder)

    def build(self):

        fb_namespace_offsets = []
        for namespace in storage.namespaces.keys():
            try:
                fb_namespace_offsets.append(self._save_namespace(namespace))
            except Exception:
                logger.error('Failed to dump storage state for namespace {}'.format(namespace))
                raise

        StorageInfo.StorageInfoStartNamespacesVector(self.builder, len(fb_namespace_offsets))
        for i in reversed(fb_namespace_offsets):
            self.builder.PrependUOffsetTRelative(i)
        fb_namespaces_offset = self.builder.EndVector(len(fb_namespace_offsets))

        logger.info('Storage state: packed {} namespaces'.format(len(fb_namespace_offsets)))

        fb_unit_mapping_offsets = []
        for mapping in self.external_storage_mapping:
            if mapping['external_storage'] != 'mulca':
                continue
            fb_unit_mapping_offsets.append(self._save_unit_mapping(mapping))

        StorageInfo.StorageInfoStartUnitMappingVector(self.builder, len(fb_unit_mapping_offsets))
        for i in reversed(fb_unit_mapping_offsets):
            self.builder.PrependUOffsetTRelative(i)
        fb_unit_mapping_offset = self.builder.EndVector(len(fb_unit_mapping_offsets))

        logger.info('Storage state: packed {} unit mappings'.format(len(fb_unit_mapping_offsets)))

        StorageInfo.StorageInfoStart(self.builder)
        StorageInfo.StorageInfoAddUnitMapping(self.builder, fb_unit_mapping_offset)
        StorageInfo.StorageInfoAddNamespaces(self.builder, fb_namespaces_offset)
        StorageInfo.StorageInfoAddTimestamp(self.builder, int(self.timestamp))
        root_offset = StorageInfo.StorageInfoEnd(self.builder)

        self.builder.Finish(root_offset)

        return str(self.builder.Output())


handle = GetStorageStateSnapshotHandle('get_storage_state_snapshot')


def get_storage_state_snapshot(request):
    return handle(request)
