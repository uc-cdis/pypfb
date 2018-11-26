def get_avro_type(property_name, property_type):
    if 'type' in property_type:
        return plain_type(property_type['type'])

    if 'enum' in property_type:
        return enum(property_name, property_type['enum'])

    if 'oneOf' in property_type:
        return union(property_name, property_type['oneOf'])

    return None


def plain_type(property_type):
    if isinstance(property_type, list):
        property_type = list(map(python_avro_types, property_type))
        property_type.reverse()
    else:
        property_type = python_avro_types(property_type)

    return property_type


def enum(property_name, symbols):
    avro_type = {
        'type': 'enum',
        'name': property_name,
        'symbols': list(map(replace_everything, symbols))
    }
    return avro_type


def union(property_name, types):
    output_type = list(
        map(lambda (position, subtype): get_avro_type('{}_{}'.format(property_name, position), subtype),
            enumerate(types)))
    return output_type


def python_avro_types(property_type):
    avro_types = {
        'integer': 'long',
        'number': 'float'
    }
    return avro_types.get(property_type, property_type)


def replace_everything(identifier):
    replace = ' -()&,./;'

    for r in replace:
        identifier = identifier.replace(r, '__')

    if identifier[0].isdigit():
        identifier = '_' + identifier

    return identifier


def record(name, types):
    return {
        'type': 'record',
        'name': name,
        'fields': types
    }
