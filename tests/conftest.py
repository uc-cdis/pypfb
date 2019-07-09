import os

import pytest
from click.testing import CliRunner

from pfb.cli import main


@pytest.fixture
def path_join():
    tests_path = os.path.abspath(os.path.dirname(__file__))

    def join(*args):
        return os.path.join(tests_path, *args)

    return join


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def invoke(runner):
    def _invoke(*args, **kwargs):
        return runner.invoke(main, args, **kwargs)

    return _invoke


@pytest.fixture
def test_avro(path_join):
    with open(path_join("pfb-data", "test.avro"), 'rb') as f:
        return f.read()
