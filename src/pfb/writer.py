from copy import deepcopy

from fastavro import writer

from .base import PFBBase, encode_enum, avro_record, handle_schema_field_unicode


# def add(pfbFile, parField, newField, newFieldType, newFieldDefault):
#     pfb = open(pfbFile, "rb")
#     avro_reader = reader(pfb)
#
#     schema = avro_reader.schema
#
#     newFieldDict = {
#         u"default": u"" + newFieldDefault,
#         u"type": u"" + newFieldType,
#         u"name": u"" + newField,
#     }
#
#     print(
#         "updating records from PFB by addding "
#         + newField
#         + " with default value of "
#         + newFieldDefault
#     )
#     records = []
#     for record in avro_reader:
#         if record["name"] == parField:
#             record["val"][u"" + newField] = u"" + newFieldDefault
#         records.append(record)
#     print("records updated with new field \n")
#
#     print("updating schema with " + newField)
#     x = 0
#     schemaParents = len(schema["fields"][2]["type"])
#     while x < schemaParents:
#         if schema["fields"][2]["type"][x]["name"] == parField:
#             schema["fields"][2]["type"][x]["fields"].append(newFieldDict)
#             break
#         x += 1
#     print("schema updated with new field")
#
#     with open("new.pfb", "wb+") as out:
#         writer(out, schema, records)
#
#
# def remove(pfbFile, parField, rmField):
#     pfb = open(pfbFile, "rb")
#     avro_reader = reader(pfb)
#
#     schema = avro_reader.schema
#
#     print("updating records from PFB file by removing " + rmField)
#     records = []
#     for record in avro_reader:
#         if record["name"] == parField:
#             del record["val"][rmField]
#         records.append(record)
#
#     x = 0
#     schemaParents = len(schema["fields"][2]["type"])
#     while x < schemaParents:
#         if schema["fields"][2]["type"][x]["name"] == parField:
#             for y in schema["fields"][2]["type"][x]["fields"]:
#                 if y["name"] == rmField:
#                     print("removing " + rmField + " from schema")
#                     schema["fields"][2]["type"][x]["fields"].remove(y)
#                     break
#             break
#         x += 1
#
#     print("writing to new file rm.pfb")
#     with open("rm.pfb", "wb+") as out:
#         writer(out, schema, records)


def make_avro_schema(schema):
    encoded_schema = []
    for node in schema:
        node = deepcopy(node)
        encoded_schema.append(node)
        for field in node["fields"]:
            handle_schema_field_unicode(field, encode=True)

    return {
        "type": "record",
        "name": "Entity",
        "fields": [
            {"name": "id", "type": ["null", "string"], "default": None},
            {"name": "name", "type": "string"},
            {
                "name": "object",
                "type": [
                    {
                        "type": "record",
                        "name": "Metadata",
                        "fields": [
                            {
                                "name": "nodes",
                                "type": {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "Node",
                                        "fields": [
                                            {"name": "name", "type": "string"},
                                            {
                                                "name": "ontology_reference",
                                                "type": "string",
                                            },
                                            {
                                                "name": "values",
                                                "type": {
                                                    "type": "map",
                                                    "values": "string",
                                                },
                                            },
                                            {
                                                "name": "links",
                                                "type": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "record",
                                                        "name": "Link",
                                                        "fields": [
                                                            {
                                                                "name": "multiplicity",
                                                                "type": {
                                                                    "type": "enum",
                                                                    "name": "Multiplicity",
                                                                    "symbols": [
                                                                        "ONE_TO_ONE",
                                                                        "ONE_TO_MANY",
                                                                        "MANY_TO_ONE",
                                                                        "MANY_TO_MANY",
                                                                    ],
                                                                },
                                                            },
                                                            {
                                                                "name": "dst",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "name",
                                                                "type": "string",
                                                            },
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                "name": "properties",
                                                "type": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "record",
                                                        "name": "Property",
                                                        "fields": [
                                                            {
                                                                "name": "name",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "ontology_reference",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "values",
                                                                "type": {
                                                                    "type": "map",
                                                                    "values": "string",
                                                                },
                                                            },
                                                        ],
                                                    },
                                                },
                                            },
                                        ],
                                    },
                                },
                            },
                            {
                                "name": "misc",
                                "type": {"type": "map", "values": "string"},
                            },
                        ],
                    }
                ]
                + encoded_schema,
            },
            {
                "name": "relations",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "Relation",
                        "fields": [
                            {"name": "dst_id", "type": "string"},
                            {"name": "dst_name", "type": "string"},
                        ],
                    },
                },
                "default": [],
            },
        ],
    }


class PFBWriter(PFBBase):
    open_mode = "wb"

    def __init__(self, file_or_path):
        super(PFBWriter, self).__init__(file_or_path)
        self._hooks = []

    def copy_schema(self, reader):
        self.set_schema(deepcopy(reader.schema))
        self.set_metadata(reader.metadata)

    def write(self, iterable=None, metadata=True):
        def _iter():
            if metadata:
                yield avro_record(None, "Metadata", self._metadata, [])
            if iterable is not None:
                for record in iterable:
                    obj = record["object"]
                    name = record["name"]
                    record["object"] = (name, obj)
                    for hook in self._hooks:
                        record = hook(record)

                    to_update = {}
                    for field, value in list(obj.items()):
                        if value is not None and self.is_encode(name, field):
                            if isinstance(value, list):
                                # this is needed to not encode list brackets
                                obj[field] = []
                                for element in value:
                                    obj[field].append(encode_enum(element))
                            else:
                                obj[field] = encode_enum(value)
                    obj.update(to_update)
                    yield record

        writer(self._file_obj, make_avro_schema(self.schema), _iter())

    def rename_node(self, name_from, name_to):
        if type(name_from) == bytes:
            name_from = name_from.decode()
        if type(name_to) == bytes:
            name_to = name_to.decode()
        for node in self.schema:
            if node["name"] == name_from:
                node["aliases"] = node.get("aliases", []) + [name_from]
                node["name"] = name_to
                for fields in node["fields"]:
                    for type_ in fields["type"]:
                        if isinstance(type_, dict) and type_.get("type") == "enum":
                            type_["name"] = type_["name"].replace(name_from, name_to)

        def _rename_node(record):
            if record["name"] == name_from:
                record["name"] = name_to
                record["object"] = (name_to, record["object"][1])
            return record

        self._hooks.append(_rename_node)

    def rename_enum(self, field_name, val_from, val_to):
        if type(val_from) == bytes:
            val_from = val_from.decode()
        if type(val_to) == bytes:
            val_to = val_to.decode()
        renamed = set()
        for node in self.schema:
            for field in node["fields"]:
                if isinstance(field, dict) and field["name"] == field_name:
                    for element in field.get("type", []):
                        if isinstance(element, dict) and element.get("type") == "enum":
                            try:
                                idx = element["symbols"].index(val_from)
                            except ValueError:
                                pass
                            else:
                                element["symbols"][idx] = val_to
                                renamed.add(node["name"])
                    if node["name"] in renamed and field["default"] == val_from:
                        field["default"] = val_to

        def _rename_enum(record):
            obj = record["object"][1]
            if obj.get(field_name) == val_from:
                obj[field_name] = val_to
            return record

        self._hooks.append(_rename_enum)
