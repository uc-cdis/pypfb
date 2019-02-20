import json
import os

from fastavro import reader, writer, parse_schema
from fastavro.validation import validate, validate_many


# https://stackoverflow.com/a/42377964/1030110
def str_hook(obj):
    return {k.encode('utf-8') if isinstance(k, unicode) else k: v.encode('utf-8') if isinstance(v, unicode) else v for
            k, v in obj}


def replace_everything(identifier):
    replace = ' -()&,./;<>{}[]'

    for r in replace:
        identifier = identifier.replace(r, '__')

    if identifier[0].isdigit():
        identifier = '_' + identifier

    return identifier


if __name__ == "__main__":
    with open('dd_schema.avro', 'rb') as in_data:
        in_reader = reader(in_data)
        metadata = []

        for record in in_reader:
            record = {
                'id': record['id'],
                'name': record['name'],
                'object': record['object'],
                'relations': []
            }
            metadata.append(record)

        schema = in_reader.schema
        schema = json.loads(json.dumps(schema), object_pairs_hook=str_hook)
        parsed_schema = parse_schema(schema)
        # print(parsed_schema)

    with open('dd_psql.avro', 'w+b') as data:
        writer(data, parsed_schema, [])

    data_dir = '/Users/andrewprokhorenkov/Documents/data/10000'

    order = open(os.path.join(data_dir, 'DataImportOrder.txt')).readlines()

    total = len(order)
    i = 1

    for o in order:
        input_data = []

        o = o.strip()
        print('{}/{}: {}'.format(i, total, o))
        i = i + 1

        json_data = json.load(open(os.path.join(data_dir, o + '.json'), 'r'))

        node_name = o

        for json_record in json_data:
            relations = []
            node_id = json_record['submitter_id']
            vals = json_record

            if 'file_size' in json_record:
                vals['error_type'] = 'md5sum'
                vals['object_id'] = ''

            to_del = None
            for item in json_record:
                if type(json_record[item]) == dict and 'submitter_id' in json_record[item]:
                    to_del = item
                    v = item
                    relations.append({'dst_id': json_record[item]['submitter_id'], 'dst_name': v})

                if type(json_record[item]) == unicode:
                    json_record[item] = replace_everything(json_record[item])

            if to_del in vals:
                del vals[to_del]

            vals['project_id'] = 'jnkns-jenkins'

            vals['created_datetime'] = None
            vals['updated_datetime'] = None

            record = {
                'id': node_id,
                'name': node_name,
                'object': (node_name, vals),
                'relations': relations
            }

            input_data.append(record)

        with open('dd_psql.avro', 'a+b') as data:
            writer(data, parsed_schema, input_data)
