import pytest


def pytest_addoption(parser):
    parser.addoption('--aws_access_key_id', action='store', default=None)
    parser.addoption('--aws_secret_access_key', action='store', default=None)
    parser.addoption('--test_bucket', action='store', default=None)
    parser.addoption('--test_prefix', action='store', default='test')
    parser.addoption('--test_region', action='store', default='us-east-1')
    parser.addoption('--postgres_host', action='store', default=None)
    parser.addoption('--postgres_password', action='store', default=None)
    parser.addoption('--postgres_database_name', action='store', default='test')


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


@pytest.fixture('session')
def postgres_host(request):
    return request.config.getoption('--postgres_host')


@pytest.fixture('session')
def postgres_password(request):
    return request.config.getoption('--postgres_password')


@pytest.fixture('session')
def postgres_database_name(request):
    return request.config.getoption('--postgres_database_name')
