from fastavro import reader, writer


# https://stackoverflow.com/a/42377964/1030110
# required to load JSON without 'unicode' keys and values
def str_hook(obj):
    return {
        k.encode("utf-8")
        if isinstance(k, unicode)
        else k: v.encode("utf-8")
        if isinstance(v, unicode)
        else v
        for k, v in obj
    }


def add(pfbFile, parField, newField, newFieldType, newFieldDefault):
    pfb = open(pfbFile, "rb")
    avro_reader = reader(pfb)

    schema = avro_reader.schema

    newFieldDict = {
        u"default": u"" + newFieldDefault,
        u"type": u"" + newFieldType,
        u"name": u"" + newField,
    }

    print(
        "updating records from PFB by addding "
        + newField
        + " with default value of "
        + newFieldDefault
    )
    records = []
    for record in avro_reader:
        if record["name"] == parField:
            record["val"][u"" + newField] = u"" + newFieldDefault
        records.append(record)
    print("records updated with new field \n")

    print("updating schema with " + newField)
    x = 0
    schemaParents = len(schema["fields"][2]["type"])
    while x < schemaParents:
        if schema["fields"][2]["type"][x]["name"] == parField:
            schema["fields"][2]["type"][x]["fields"].append(newFieldDict)
            break
        x += 1
    print("schema updated with new field")

    with open("new.pfb", "wb+") as out:
        writer(out, schema, records)


def remove(pfbFile, parField, rmField):
    pfb = open(pfbFile, "rb")
    avro_reader = reader(pfb)

    schema = avro_reader.schema

    print("updating records from PFB file by removing " + rmField)
    records = []
    for record in avro_reader:
        if record["name"] == parField:
            del record["val"][rmField]
        records.append(record)

    x = 0
    schemaParents = len(schema["fields"][2]["type"])
    while x < schemaParents:
        if schema["fields"][2]["type"][x]["name"] == parField:
            for y in schema["fields"][2]["type"][x]["fields"]:
                if y["name"] == rmField:
                    print("removing " + rmField + " from schema")
                    schema["fields"][2]["type"][x]["fields"].remove(y)
                    break
            break
        x += 1

    print("writing to new file rm.pfb")
    with open("rm.pfb", "wb+") as out:
        writer(out, schema, records)
