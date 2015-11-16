import random
import string

import pytest


@pytest.fixture(scope='function')
def ascii_data(data_size):
    return ''.join(
        random.choice(
            string.ascii_letters + string.digits
        ) for _ in xrange(data_size)
    )
