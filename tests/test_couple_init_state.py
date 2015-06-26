import pytest


@pytest.mark.mm_bench_settings(
    namespaces={'ns-3': {
        'settings': {
            'success_copies': 'all',
            'groups_count': 2,
        },
        'couples': (
            {'number_of_couples': 1,
             'init_state': 'coupled'},
            {'number_of_couples': 1,
             'init_state': 'frozen'}
        )
    }}
)
@pytest.mark.usefixtures('bench_settings')
class TestCoupleInit(object):
    def test_couples_number(self, mm_client):
        assert len(mm_client.couples.filter(namespace='ns-3')) == 2

    def test_couples_statuses(self, mm_client):
        assert set(c.status for c in mm_client.couples) == set(['OK', 'FROZEN'])
