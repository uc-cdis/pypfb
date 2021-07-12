# -*- coding: utf-8 -*-

import sys
import uuid
import re
from collections import deque

if sys.version_info[0] == 3:
    PY3 = True
else:
    PY3 = False


# https://stackoverflow.com/a/42377964/1030110
# required to load JSON without 'unicode' keys and values in Python 2
#
# TODO: remove in Python 3
def str_hook(obj):
    if PY3:
        return {k: v for k, v in obj}
    return {
        k.encode("utf-8")
        if isinstance(k, unicode)
        else k: v.encode("utf-8")
        if isinstance(v, unicode)
        else v
        for k, v in obj
    }


def unicode_encode(matchobj):
    unicodeRep = str(hex(ord(matchobj.group(0))))[2:]
    return "_" + unicodeRep + "_"


def unicode_decode(matchobj):
    unicodeRep = matchobj.group(0)
    unicodeRep = unicodeRep.strip("_")

    if len(unicodeRep) == 2:
        unicodeRep = "\\u00" + unicodeRep
    elif len(unicodeRep) == 3:
        unicodeRep = "\\u0" + unicodeRep
    else:
        unicodeRep = "\\u" + unicodeRep

    unicodeRep = unicodeRep.encode().decode("unicode_escape")
    return unicodeRep


def encode_enum(enumValue):
    encodedValue = re.sub(
        "^[0-9]|[^A-Za-z0-9]", unicode_encode, str(enumValue), flags=re.UNICODE
    )
    return encodedValue


def decode_enum(enumValue):
    decodedValue = re.sub("_[a-z0-9]+_", unicode_decode, enumValue, flags=re.UNICODE)
    return decodedValue


def is_enum(data_type):
    data_type = [data_type]
    while data_type:
        type_ = data_type.pop()
        if isinstance(type_, list):
            data_type.extend(type_)
        elif isinstance(type_, dict) and type_["type"] == "enum":
            return True
        elif (
            isinstance(type_, dict)
            and type_["type"] == "array"
            and isinstance(type_["items"], dict)
            and type_["items"]["type"] == "enum"
        ):
            return True
    return False


def handle_schema_field_unicode(field, encode=True):
    method = encode_enum if encode else decode_enum
    is_enum_ = False
    stack = deque([field["type"]])
    while stack:
        t = stack.pop()
        if isinstance(t, list):
            stack.extend(t)
        elif isinstance(t, dict) and t["type"] == "enum":
            is_enum_ = True
            symbols = []
            for symbol in t["symbols"]:
                if symbol == None:
                    continue
                symbols.append(method(symbol))
            t["symbols"] = symbols
        elif isinstance(t, dict) and t["type"] == "array":
            if isinstance(t["items"], dict) and t["items"]["type"] == "enum":
                is_enum = True
                symbols = []
                for symbol in t["items"]["symbols"]:
                    if symbol == None:
                        continue
                    symbols.append(method(symbol))
                if "name" in t["items"]:
                    t["items"]["name"] = method(t["items"]["name"])
                t["items"]["symbols"] = symbols
    default = field.get("default")
    if is_enum_ and default:
        field["default"] = method(default)


def avro_record(node_id, node_name, values, relations):
    node = {"id": node_id, "name": node_name, "object": values, "relations": relations}
    return node


class PFBBase(object):
    open_mode = "r"

    def __init__(self, file_or_path):
        self._file_or_path = file_or_path
        self._is_encode = None
        self._schema = None
        self._metadata = None
        self._encoded_schema = None

    def __enter__(self):
        self._file_obj = (
            self._file_or_path
            if hasattr(self._file_or_path, "read")
            else open(self._file_or_path, self.open_mode)
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file_obj.close()

    @property
    def name(self):
        return self._file_obj.name

    @property
    def isatty(self):
        return self._file_obj.isatty()

    @property
    def schema(self):
        return self._schema

    @property
    def metadata(self):
        return self._metadata

    def set_schema(self, schema):
        self._schema = schema

    def set_encoded_schema(self, encoded_schema):
        self._encoded_schema = encoded_schema

    def set_metadata(self, metadata):
        self._metadata = metadata

    def prepare_encode_cache(self):
        if self._is_encode is None:
            self._is_encode = {}
            for node in self._schema:
                self._is_encode[node["name"]] = fields = {}
                for field in node["fields"]:
                    fields[field["name"]] = is_enum(field["type"])

    def is_encode(self, node_name, field_name):
        self.prepare_encode_cache()
        return self._is_encode[node_name].get(field_name, False)

    def make_empty_record(self, node_name):
        values = {}
        for node in self.schema:
            if node["name"] == node_name:
                for field in node["fields"]:
                    val = field.get("default", "")
                    stack = deque([field["type"]])
                    while stack:
                        t = stack.pop()
                        if isinstance(t, list):
                            stack.extend(t)
                        elif t in ["long", "float"]:
                            val = 0
                            break
                        elif t in ["string"]:
                            val = ""
                            break
                        elif isinstance(t, dict) and t["type"] == "enum":
                            val = t["symbols"][0]
                            break

                    values[field["name"]] = val

        return avro_record(str(uuid.uuid4()), node_name, values, [])
