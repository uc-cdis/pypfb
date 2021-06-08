import csv
import gzip
import json
import os
import shutil

from fastavro import reader

from pfb.base import decode_enum, encode_enum, str_hook


def _test_schema(r):
    for node in r.writer_schema["fields"][2]["type"]:
        if node["name"] == "experiment_metadata":
            for field in node["fields"]:
                if field["name"] == "state":
                    assert decode_enum(field["default"]) == "validated"
                    for type_ in field["type"]:
                        if isinstance(type_, dict) and type_["type"] == "enum":
                            for symbol in type_["symbols"]:
                                decode_enum(symbol)


def test_from_dict_niaid(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/ndhdictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_bhc(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/bhcdictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_gtex(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/gtexdictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_genomel(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/genomel-dictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_kf(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/develop/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_acct(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/acctdictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_charlie(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/charlie-datadictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_canine(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/canine_dictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_covid19(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/covid19-datadictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_bhc(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/bhcdictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_bpa(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/bpadictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_midrc(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/midrc_dictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_flu(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/flu-datadictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_heal(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/heal_dictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_ibdgc(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/ibdgc-dictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_tcga(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/tcga_dictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_jcoin(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/jcoin_datadictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_microbiome(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/microbiome_datadictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_tb(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/tb-datadictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_dict_va(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            "https://s3.amazonaws.com/dictionary-artifacts/vadictionary/master/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1
