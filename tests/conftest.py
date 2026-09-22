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
    with open(path_join("pfb-data", "test.avro"), "rb") as f:
        return f.read()


@pytest.fixture
def linkml_schema(path_join):
    """Return path to LinkML test schema."""
    return path_join("linkml", "test_schema.yaml")


@pytest.fixture
def linkml_data(path_join):
    """Return path to LinkML test data directory."""
    return path_join("linkml", "data")


@pytest.fixture(params=["gen3", "linkml"], ids=["gen3", "linkml"])
def kf_schema_only_pfb(request, runner, invoke, path_join):
    """Schema-only PFB (no data records).

    gen3:   ``pfb from dict <URL>`` – downloads the KF 1.1.0 dictionary.
    linkml: ``pfb from linkml -s kf_schema.yaml <empty_dir>`` – writes only
            the Metadata record when no data files are present.
    """
    source = request.param
    with runner.isolated_filesystem():
        if source == "gen3":
            result = invoke(
                "from",
                "-o",
                "out.avro",
                "dict",
                "http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json",
            )
        else:
            os.makedirs("empty")
            result = invoke(
                "from",
                "-o",
                "out.avro",
                "linkml",
                "-s",
                path_join("schema", "kf_schema.yaml"),
                "empty",
                "--program",
                "DEV",
                "--project",
                "test",
            )
        assert result.exit_code == 0, result.output
        with open("out.avro", "rb") as f:
            return source, f.read()


@pytest.fixture(params=["gen3", "linkml"], ids=["gen3", "linkml"])
def kf_json_pfb(request, runner, invoke, path_join):
    """PFB built from the KF JSON data files.

    gen3:   ``pfb from json tests/data/ -s kf.avro``
    linkml: ``pfb from linkml -s kf_schema.yaml tests/data/``
    """
    source = request.param
    with runner.isolated_filesystem():
        if source == "gen3":
            result = invoke(
                "from",
                "-o",
                "out.avro",
                "json",
                path_join("data"),
                "-s",
                path_join("schema", "kf.avro"),
                "--program",
                "DEV",
                "--project",
                "test",
            )
        else:
            result = invoke(
                "from",
                "-o",
                "out.avro",
                "linkml",
                "-s",
                path_join("schema", "kf_schema.yaml"),
                path_join("data"),
                "--program",
                "DEV",
                "--project",
                "test",
            )
        assert result.exit_code == 0, result.output
        with open("out.avro", "rb") as f:
            return source, f.read()


@pytest.fixture(params=["gen3", "linkml"], ids=["gen3", "linkml"])
def kf_tsv_pfb(request, runner, invoke, path_join):
    """PFB built from the KF TSV data files.

    gen3:   ``pfb from tsv tests/tsv_data/ -s kf.avro``
    linkml: ``pfb from linkml -s kf_schema.yaml tests/tsv_data/``
    """
    source = request.param
    with runner.isolated_filesystem():
        if source == "gen3":
            result = invoke(
                "from",
                "-o",
                "out.avro",
                "tsv",
                path_join("tsv_data"),
                "-s",
                path_join("schema", "kf.avro"),
                "--program",
                "DEV",
                "--project",
                "test",
            )
        else:
            result = invoke(
                "from",
                "-o",
                "out.avro",
                "linkml",
                "-s",
                path_join("schema", "kf_schema.yaml"),
                path_join("tsv_data"),
                "--program",
                "DEV",
                "--project",
                "test",
            )
        assert result.exit_code == 0, result.output
        with open("out.avro", "rb") as f:
            return source, f.read()
