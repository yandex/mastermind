import pytest


@pytest.mark.usefixtures('bench_settings')
@pytest.mark.mm_bench_settings
class TestEmptyBench(object):

    def test_groups(self, mm_client):
        groups = [g for g in mm_client.groups]
        assert len(groups) > 0, 'no groups found'

    def test_groups_init_status(self, mm_client):
        for g in mm_client.groups:
            assert g.status == 'INIT'
