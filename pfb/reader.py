import json
from copy import deepcopy

from fastavro import reader

from .base import PY3, PFBBase, b64_decode, handle_schema_field_b64, str_hook


class PFBReader(PFBBase):
    open_mode = "rb"

    def __enter__(self):
        rv = super(PFBReader, self).__enter__()
        self._reader = reader(self._file_obj)
        schema = []
        for f in self._reader.writer_schema["fields"]:
            if f["name"] == "object":
                it = iter(f["type"])
                # skip metadata
                next(it)
                for node in it:
                    node = deepcopy(node)
                    schema.append(node)
                    for field in node["fields"]:
                        handle_schema_field_b64(field, encode=False)
        self.set_schema(json.loads(json.dumps(schema), object_pairs_hook=str_hook))
        self.set_metadata(next(self._reader)["object"])
        return rv

    def __iter__(self):
        return self

    def __next__(self):
        rv = next(self._reader)
        obj = rv["object"]
        to_update = {}
        for name, value in obj.iteritems():
            if value and self.is_base64(rv["name"], name):
                to_update[name] = b64_decode(value)
        obj.update(to_update)
        return rv

    if not PY3:
        next = __next__
