import click
from dictionaryutils import DataDictionary, dictionary

from ..cli import from_command

_AVRO_TYPES = {"integer": "long", "number": "float"}


@from_command.command(
    "dict", short_help="Convert Gen3 data dictionary into a PFB file."
)
@click.argument("url_or_path", metavar="DICTIONARY")
@click.pass_context
def from_dict(ctx, url_or_path):
    """Convert Gen3 data DICTIONARY into a PFB file.

    If DICTIONARY is a HTTP URL, it will be downloaded and parsed as JSON; or it will be
    treated as a local path to a directory containing YAML files.
    """
    try:
        with ctx.obj["writer"] as writer:
            _from_dict(writer, url_or_path)
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise


def _from_dict(writer, url_or_path):
    if writer.isatty:
        click.secho("Error: cannot output to TTY.", fg="red", bold=True, err=True)
        return

    click.secho("Loading dictionary: {}".format(url_or_path), fg="cyan", err=True)
    if url_or_path.lower().startswith("http"):
        d = DataDictionary(url=url_or_path)
    elif url_or_path.lower().endswith(".json"):
        d = DataDictionary(local_file=str(url_or_path))
    else:
        d = DataDictionary(root_dir=url_or_path)
    dictionary.init(d)

    click.secho("Parsing dictionary...", fg="cyan", err=True)
    records, ontology_references, links = _parse_dictionary(d)
    metadata = _get_ontology_references(ontology_references, links)

    click.secho("Writing PFB...", fg="blue", err=True)
    writer.set_schema(records)
    writer.set_metadata(metadata)
    writer.write()

    click.secho(
        "Done, created PFB file at: ", fg="green", err=True, nl=False, bold=True
    )
    click.secho(writer.name, fg="white", err=True, bold=True)


def _get_ontology_references(ontology_references, all_links):
    nodes_json = []

    for node_name, node_value in ontology_references.iteritems():
        properties = _get_ontologies_from_node(node_value)
        links = _get_links_for_node(all_links[node_name])

        node_json = {
            "name": node_name,
            "ontology_reference": "",
            "values": {},
            "links": links,
            "properties": properties,
        }
        nodes_json.append(node_json)

    return {"nodes": nodes_json, "misc": {}}


def _get_link(node_link):
    add = {}
    if "multiplicity" in node_link:
        add["multiplicity"] = node_link["multiplicity"].upper()

    if "name" in node_link and "target_type" in node_link:
        add["name"] = node_link["name"]
        add["dst"] = node_link["target_type"]

    return add


def _get_links_for_node(node_links):
    links = []

    for link in node_links:
        if "subgroup" in link:
            add = _get_links_for_node(link["subgroup"])
            links.extend(add)

        else:
            add = _get_link(link)
            links.append(add)

    return links


def _get_ontologies_from_node(node_value):
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


def _parse_dictionary(d):
    records = []
    ontology_references = {}
    links = {}

    for record_name, record_types in d.schema.iteritems():
        types = []
        ontology_references_for_record = {}
        properties = record_types["properties"]

        for property_name, property_type in properties.iteritems():
            if property_name in ["id", "type"]:
                continue

            # Need to reorder the property_types so the default value is a part of the first list of enums as per avro spec
            if "default" in property_type:
                if "oneOf" in property_type:
                    default = property_type["default"]
                    for enum in property_type["oneOf"]:
                        if default in enum["enum"]:
                            property_type["oneOf"].insert(0, property_type["oneOf"].pop(property_type["oneOf"].index(enum)))
                            break

            avro_type = _get_avro_type(property_name, property_type, record_name)

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
                    t["default"] = property_type["default"]
                elif avro_type == "string":
                    t["default"] = ""
                else:
                    # if theres no default and null is not the first type then we need to fix order per avro spec
                    if isinstance(avro_type, list) and avro_type[0] != "null":
                        avro_type.insert(0, avro_type.pop(avro_type.index("null")))
                        t["type"] = avro_type
                    t["default"] = None

                types.append(t)

            record_has_ontology = (
                "term" in property_type and "termDef" in property_type["term"]
            )
            if record_has_ontology:
                ontology_references_for_record[property_name] = property_type["term"][
                    "termDef"
                ]

        records.append(_record_type(record_name, types))

        if "links" in record_types:
            links[record_name] = record_types["links"]

        ontology_references[record_name] = ontology_references_for_record

    return records, ontology_references, links


def _get_avro_type(property_name, property_type, name):
    if "type" in property_type:
        if property_type["type"] == "array":
            return _array_type(property_type)
        return _plain_type(property_type["type"])

    if "enum" in property_type:
        return _enum_type(property_name, property_type["enum"], name)

    if "oneOf" in property_type:
        return _union_type(property_name, property_type["oneOf"], name)

    return None


def _array_type(property_type):
    return {"items": property_type["items"]["type"], "type": property_type["type"]}


def _plain_type(property_type):
    if isinstance(property_type, list):
        property_type = list(map(_python_avro_types, property_type))
        property_type.reverse()
    else:
        property_type = _python_avro_types(property_type)

    return property_type


def _enum_type(property_name, symbols, name):
    return {
        "type": "enum",
        "name": "{}_{}".format(name, property_name),
        "symbols": symbols,
    }


def _union_type(property_name, types, name):
    return [
        _get_avro_type("{}_{}_{}".format(name, property_name, position), subtype, name)
        for position, subtype in enumerate(types)
    ]


def _python_avro_types(property_type):
    return _AVRO_TYPES.get(property_type, property_type)


def _record_type(name, types):
    return {"type": "record", "name": name, "fields": types}
