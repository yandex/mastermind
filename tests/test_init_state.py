import pytest

from fixtures import *


def test_groups(mm_client):
    groups = [g for g in mm_client.groups]
    assert len(groups) > 0, 'no groups found'


def test_groups_init_status(mm_client):
    for g in mm_client.groups:
        assert g.status == 'INIT'
