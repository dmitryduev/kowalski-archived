import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--username", action="store", default="", help="username"
    )
    parser.addoption(
        "--password", action="store", default="", help="password"
    )

    parser.addoption(
        "--benchmark", action="store_true", help="run benchmark?"
    )


@pytest.fixture
def username(request):
    return request.config.getoption("--username")


@pytest.fixture
def password(request):
    return request.config.getoption("--password")


@pytest.fixture
def benchmark(request):
    return request.config.getoption("--benchmark")
