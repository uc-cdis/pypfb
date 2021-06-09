import csv
import gzip
import json
import os
import shutil

import pytest

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


my_dictionaries = [
    "https://s3.amazonaws.com/dictionary-artifacts/ndhdictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/bhcdictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/gtexdictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/genomel-dictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/develop/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/acctdictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/charlie-datadictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/canine_dictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/covid19-datadictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/midrc_dictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/flu-datadictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/heal_dictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/ibdgc-dictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/tcga_dictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/jcoin_datadictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/microbiome_datadictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/tb-datadictionary/master/schema.json",
    "https://s3.amazonaws.com/dictionary-artifacts/vadictionary/master/schema.json",
]


@pytest.mark.parametrize("dictionary", my_dictionaries)
def test_dictionary(runner, invoke, dictionary):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "test.avro",
            "dict",
            dictionary,
        )
        assert result.exit_code == 0, result.output

        with open("test.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1
