import pytest


def pytest_addoption(parser):
    parser.addoption('--aws_access_key_id', action='store', required=True)
    parser.addoption('--aws_secret_access_key', action='store', required=True)
    parser.addoption('--test_bucket', action='store', required=True)
    parser.addoption('--test_prefix', action='store', default='test')
    parser.addoption('--test_region', action='store', default='us-east-1')


@pytest.fixture('session')
def aws_access_key_id(request):
    return request.config.getoption('--aws_access_key_id')


@pytest.fixture('session')
def aws_secret_access_key(request):
    return request.config.getoption('--aws_secret_access_key')


@pytest.fixture('session')
def test_bucket(request):
    return request.config.getoption('--test_bucket')


@pytest.fixture('session')
def test_prefix(request):
    return request.config.getoption('--test_prefix')


@pytest.fixture('session')
def test_region(request):
    return request.config.getoption('--test_region')
