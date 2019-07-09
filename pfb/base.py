import base64
import sys
import uuid
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
        return obj
    return {
        k.encode("utf-8")
        if isinstance(k, unicode)
        else k: v.encode("utf-8")
        if isinstance(v, unicode)
        else v
        for k, v in obj
    }


def b64_encode(raw_value):
    return base64.b64encode(raw_value).rstrip("=")


def b64_decode(encoded_value):
    return base64.b64decode(encoded_value + "=" * (-len(encoded_value) % 4))


def is_enum(data_type):
    data_type = [data_type]
    while data_type:
        type_ = data_type.pop()
        if isinstance(type_, list):
            data_type.extend(type_)
        elif isinstance(type_, dict) and type_["type"] == "enum":
            return True
    return False


def handle_schema_field_b64(field, encode=True):
    method = b64_encode if encode else b64_decode
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
                symbols.append(method(symbol))
            t["symbols"] = symbols
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
        self._is_base64 = None
        self._schema = None
        self._metadata = None

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

    def set_metadata(self, metadata):
        self._metadata = metadata

    def prepare_base64_cache(self):
        if self._is_base64 is None:
            self._is_base64 = {}
            for node in self._schema:
                self._is_base64[node["name"]] = fields = {}
                for field in node["fields"]:
                    fields[field["name"]] = is_enum(field["type"])

    def is_base64(self, node_name, field_name):
        self.prepare_base64_cache()
        return self._is_base64[node_name].get(field_name, False)

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
