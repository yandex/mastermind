from fixtures import *


def pytest_addoption(parser):
    parser.addoption('--clean', action='store_true', dest='clean',
                     help='reset all bench entities to their init state')


def pytest_configure(config):
    print "Clean option: {}".format(config.getoption('clean'))
