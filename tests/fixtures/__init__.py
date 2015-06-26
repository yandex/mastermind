import pytest

from mastermind import MastermindClient


BENCH_SETTINGS_MARKER_NAME = 'mm_bench_settings'


@pytest.fixture(scope='class')
def mm_client():
    return MastermindClient()


@pytest.yield_fixture(scope='class')
def bench_settings(request, mm_client):
    # import pdb; pdb.set_trace()

    def fail_test():
        pytest.xfail('Request {} has no bench settings pytest marker '
                     'supplied'.format(request))

    if not hasattr(request.node.obj, 'pytestmark'):
        fail_test()
    marker = _find_marker(request, BENCH_SETTINGS_MARKER_NAME)
    if not marker:
        fail_test()
    request.applymarker(marker)
    settings = request.node.get_marker('mm_bench_settings')
    settings = settings.kwargs if settings else {}
    _setup_bench(mm_client, settings)
    yield mm_client
    _teardown_bench(mm_client, settings)


def _setup_bench(mm_client, settings):
    for ns, params in settings.get('namespaces', {}).iteritems():
        namespace = mm_client.namespaces.setup(ns, **params['settings'])
        couples_settings = params.get('couples')
        if isinstance(couples_settings, dict):
            couples_settings = tuple([couples_settings])
        for cs in couples_settings:
            cs = cs.copy()
            args = (cs.pop('couple_size', params['settings']['groups_count']),
                    cs.pop('init_state', 'coupled'),
                    cs.pop('number_of_couples', 1))
            namespace.build_couples(*args, **cs)


def _teardown_bench(mm_client, settings):
    for ns in settings.get('namespaces', {}).iterkeys():
        namespace = mm_client.namespaces[ns]
        for c in namespace.couples:
            del mm_client.couples[c]
        del mm_client.namespaces[ns]


def _find_marker(request, name):
    marker = request.node.obj.pytestmark
    if isinstance(marker, list):
        marker = filter(lambda m: m.name == BENCH_SETTINGS_MARKER_NAME, marker)
        if not len(marker):
            return None
        marker = marker[0]
    return marker
