import json
import logging

from fastavro import writer, parse_schema

from .avro_types import get_avro_type, record
from ..utils.str import encode

log = logging.getLogger(__name__)


class AvroSchema:
    def __init__(self, avro_schema, ontology_references, links):
        self.avro_schema = avro_schema
        self.ontology_references = ontology_references
        self.links = links

    def write_schema(self, f):
        json_avro_schema = json.dumps(self.avro_schema)
        f.write(json_avro_schema)

    def write(self, output):
        metadata = self.get_ontology_references()
        # json_schema = json.load(open('schema/Metadata.json'), object_pairs_hook=str_hook)
        # json_schema.append(self.avro_schema)
        parsed_schema = parse_schema(self.avro_schema)
        writer(output, parsed_schema, metadata)

    def get_ontology_references(self):
        nodes_json = []

        for node_name, node_value in self.ontology_references.iteritems():
            properties = self.get_ontologies_from_node(node_value)
            links = self.get_links_for_node(self.links[node_name])

            node_json = {
                "name": node_name,
                "ontology_reference": "",
                "values": {},
                "links": links,
                "properties": properties,
            }
            nodes_json.append(node_json)

        metadata = {
            "name": "metadata",
            "object": ("Metadata", {"nodes": nodes_json, "misc": {}}),
        }

        return [metadata]

    @staticmethod
    def get_link(node_link):
        add = {}
        if "multiplicity" in node_link:
            add["multiplicity"] = node_link["multiplicity"].upper()

        if "name" in node_link and "target_type" in node_link:
            add["name"] = node_link["name"]
            add["dst"] = node_link["target_type"]

        return add

    @staticmethod
    def get_links_for_node(node_links):
        links = []

        for link in node_links:
            if "subgroup" in link:
                add = AvroSchema.get_links_for_node(link["subgroup"])
                links.extend(add)

            else:
                add = AvroSchema.get_link(link)
                links.append(add)

        return links

    @staticmethod
    def get_ontologies_from_node(node_value):
        properties = []
        for property_name, property_value in node_value.iteritems():
            ontology_reference = property_value.get("term", None)

            # set ontology reference to empty string if "term" is already "None"
            ontology_reference = (
                ontology_reference if ontology_reference is not None else ""
            )

            # "values" maps to all properties except the "term"
            property_json = {
                "name": property_name,
                "ontology_reference": ontology_reference,
                "values": {
                    k: str(v)
                    for k, v in property_value.iteritems()
                    if k not in ["term"] and v is not None
                },
            }

            properties.append(property_json)

        return properties

    @staticmethod
    def from_dictionary(schema):
        records = []
        ontology_references = {}
        links = {}

        for record_name, record_types in schema.iteritems():
            types = []
            ontology_references_for_record = {}
            properties = record_types["properties"]

            for property_name, property_type in properties.iteritems():
                if property_name in ["id", "type"]:
                    continue

                avro_type = get_avro_type(property_name, property_type, record_name)

                # "None" represent an unsupported type in dictionary
                if avro_type is not None:
                    if isinstance(avro_type, list):
                        new_avro_type = []
                        for item in avro_type:
                            if item not in new_avro_type:
                                new_avro_type.append(item)

                        avro_type = new_avro_type

                    if not isinstance(avro_type, list):
                        if "default" in property_type:
                            avro_type = [avro_type, "null"]
                        else:
                            avro_type = ["null", avro_type]
                    elif "null" not in avro_type:
                        if "default" in property_type:
                            avro_type.append("null")
                        else:
                            avro_type.insert(0, "null")

                    t = {"name": property_name, "type": avro_type}

                    # if property_name in ['error_type', 'availability_type']:
                    #     t['type'] = ['null', avro_type]
                    #     t['default'] = None

                    if "default" in property_type:
                        if isinstance(property_type["default"], str):
                            t["default"] = encode(property_type["default"])
                        else:
                            t["default"] = property_type["default"]
                    elif avro_type == "string":
                        t["default"] = ""
                    else:
                        t["default"] = None

                    types.append(t)

                record_has_ontology = (
                    "term" in property_type and "termDef" in property_type["term"]
                )
                if record_has_ontology:
                    ontology_references_for_record[property_name] = property_type[
                        "term"
                    ]["termDef"]

            records.append(record(record_name, types))

            if "links" in record_types:
                links[record_name] = record_types["links"]

            ontology_references[record_name] = ontology_references_for_record

        avro_schema = {
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
                    ],
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
        avro_schema["fields"][2]["type"].extend(records)

        return AvroSchema(
            avro_schema=avro_schema,
            ontology_references=ontology_references,
            links=links,
        )
