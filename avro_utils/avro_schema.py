import json
import logging

from fastavro import writer, parse_schema

from avro_utils.avro_types import get_avro_type, record

log = logging.getLogger(__name__)


# https://stackoverflow.com/a/42377964/1030110
# required to load JSON without 'unicode' keys and values
def str_hook(obj):
    return {k.encode('utf-8') if isinstance(k, unicode) else k: v.encode('utf-8') if isinstance(v, unicode) else v for
            k, v in obj}


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
        json_schema = json.load(open('schema/Metadata.json'), object_pairs_hook=str_hook)
        json_schema.append(self.avro_schema)
        parsed_schema = parse_schema(json_schema)
        writer(output, parsed_schema, metadata)

    def get_ontology_references(self):
        nodes_json = []

        for node_name, node_value in self.ontology_references.iteritems():
            properties = self.get_ontologies_from_node(node_value)
            links = self.get_links_for_node(self.links[node_name])

            node_json = {'name': node_name, 'ontology_reference': '', 'links': links, 'properties': properties}
            nodes_json.append(node_json)

        metadata = {'name': 'metadata', 'object': {'nodes': nodes_json}}

        return [metadata]

    @staticmethod
    def get_link(node_link):
        add = {}
        if 'multiplicity' in node_link:
            add['multiplicity'] = node_link['multiplicity'].upper()

        if 'name' in node_link:
            add['dst'] = node_link['name']

        return add

    @staticmethod
    def get_links_for_node(node_links):
        links = []

        for link in node_links:
            if 'subgroup' in link:
                add = AvroSchema.get_links_for_node(link['subgroup'])
                links.extend(add)

            else:
                add = AvroSchema.get_link(link)
                links.append(add)

        return links

    @staticmethod
    def get_ontologies_from_node(node_value):
        properties = []
        for property_name, property_value in node_value.iteritems():
            ontology_reference = property_value.get('term', None)

            # set ontology reference to empty string if "term" is already "None"
            ontology_reference = ontology_reference if ontology_reference is not None else ''

            # "values" maps to all properties except the "term"
            property_json = {'name': property_name,
                             'ontology_reference': ontology_reference,
                             'values': {k: str(v) for k, v in property_value.iteritems() if
                                        k not in ['term'] and v is not None}}

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
            properties = record_types['properties']

            for property_name, property_type in properties.iteritems():
                if property_name in ['id', 'type', 'attribution']:
                    continue

                avro_type = get_avro_type(property_name, property_type)

                # "None" represent an unsupported type in dictionary
                if avro_type is not None:
                    t = {'name': property_name, 'type': avro_type}
                    if 'default' in property_type:
                        t['default'] = property_type['default']

                    types.append(t)

                record_has_ontology = 'term' in property_type and 'termDef' in property_type['term']
                if record_has_ontology:
                    ontology_references_for_record[property_name] = property_type['term']['termDef']

            records.append(record(record_name, types))

            if 'links' in record_types:
                links[record_name] = record_types['links']

            if ontology_references_for_record != {}:
                ontology_references[record_name] = ontology_references_for_record

        avro_schema = json.load(open('schema/Entity.json'), object_pairs_hook=str_hook)
        avro_schema['fields'][2]['type'].extend(records)

        return AvroSchema(avro_schema=avro_schema, ontology_references=ontology_references, links=links)
