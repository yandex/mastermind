from mastermind import MastermindClient


@pytest.fixture
def mm_client():
    return MastermindClient()
