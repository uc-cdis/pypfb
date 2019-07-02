import csv
import gzip
import os
from uuid import uuid4

from fastavro import reader

from .avro_utils.avro_types import is_enum
from .utils.str import decode


class PFBExporter(object):
    def __init__(self, file_obj):
        self.reader = reader(file_obj)
        self.fields_by_name = {}

    def __enter__(self):
        for field in self.reader.writer_schema["fields"]:
            if field["name"] == "object":
                it = iter(field["type"])
                # skip metadata schema
                next(it)
                for t in it:
                    self.fields_by_name[t["name"]] = t["fields"]
        return self


class PFB2GremlinExporter(PFBExporter):
    type_mapping = dict(
        boolean="Boolean",
        enum="String",
        integer="Long",
        long="Long",
        md5sum="String",
        float="Double",
        number="Double",
        string="String",
    )

    def __init__(self, file_obj):
        super(PFB2GremlinExporter, self).__init__(file_obj)
        self._handlers_by_name = {}

    def __exit__(self, exc_type, exc_val, exc_tb):
        for f, w in self._handlers_by_name.itervalues():
            f.close()

    def export_files(self, dir_path, gzipped=False):
        uuids = {}
        project_ids = []
        edges = []

        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        path = os.path.join(dir_path, "gremlin_edges.csv")
        if gzipped:
            f = gzip.open(path + ".gz", "wb")
        else:
            f = open(path, "wb")
        ew = csv.writer(f)
        ew.writerow(["~id", "~from", "~to", "~label"])
        self._handlers_by_name["~edges"] = f, ew

        it = iter(self.reader)
        # skip metadata
        next(it)
        for row in it:
            name = row["name"]
            fields = self.fields_by_name[name]

            row_id = row["id"]
            obj = row["object"]
            relations = row["relations"]

            pair = self._handlers_by_name.get(name)
            if pair is None:
                header_row = ["~label", "~id"]
                for field in fields:
                    field_type = "string"
                    for field_type in field["type"]:
                        if field_type != "null":
                            break
                    if isinstance(field_type, (str, unicode)):
                        field_type = self.type_mapping[field_type]
                    else:
                        field_type = "String"
                    header_row.append(field["name"] + ":" + field_type)
                path = os.path.join(dir_path, name + ".csv")
                if gzipped:
                    f = gzip.open(path + ".gz", "wb")
                else:
                    f = open(path, "wb")
                w = csv.writer(f)
                w.writerow(header_row)
                self._handlers_by_name[name] = f, w
            else:
                w = pair[1]

            uuid = uuids[(name, row_id)] = str(uuid4())
            row = [name, uuid]
            for field in fields:
                if field["name"] == "project_id":
                    project_ids.append([name, uuid, obj["project_id"]])
                    row.append(obj[field["name"]])
                else:
                    value = obj[field["name"]]
                    if value and is_enum(field['type']):
                        value = decode(value)
                    row.append(value)
            w.writerow(row)
            for relation in relations:
                key = relation["dst_name"], relation["dst_id"]
                to_uuid = uuids.get(key)
                if to_uuid is None:
                    edges.append((key, uuid))
                else:
                    ew.writerow([str(uuid4()), uuid, to_uuid, relation["dst_name"]])
        for edge in edges:
            ew.writerow([str(uuid4()), edge[1], uuids[edge[0]], edge[0][0]])
