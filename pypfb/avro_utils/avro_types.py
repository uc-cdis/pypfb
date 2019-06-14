from ..utils.str import encode


def get_avro_type(property_name, property_type, name):
    if "type" in property_type:
        if property_type["type"] == "array":
            return array_type(property_type)
        return plain_type(property_type["type"])

    if "enum" in property_type:
        return enum(property_name, property_type["enum"], name)

    if "oneOf" in property_type:
        return union(property_name, property_type["oneOf"], name)

    return None


def array_type(property_type):
    property_type = {
        "items": property_type["items"]["type"],
        "type": property_type["type"],
    }
    return property_type


def plain_type(property_type):
    if isinstance(property_type, list):
        property_type = list(map(python_avro_types, property_type))
        property_type.reverse()
    else:
        property_type = python_avro_types(property_type)

    return property_type


def enum(property_name, symbols, name):
    avro_type = {
        "type": "enum",
        "name": "{}_{}".format(name, property_name),
        "symbols": list(map(lambda s: encode(str(s)), symbols)),
    }
    # avro_type = {
    #     'type': 'string',
    #     'name': property_name
    # }

    return avro_type


def union(property_name, types, name):
    output_type = list(
        map(
            lambda x: (
                lambda position, subtype: get_avro_type(
                    "{}_{}_{}".format(name, property_name, position), subtype, name
                )
            )(*x),
            enumerate(types),
        )
    )
    return output_type


def python_avro_types(property_type):
    avro_types = {"integer": "long", "number": "float"}
    return avro_types.get(property_type, property_type)


def record(name, types):
    return {"type": "record", "name": name, "fields": types}
