"""LinkML schema and data importer for PFB."""

from __future__ import absolute_import

import glob
import json
import os
import csv
from pathlib import Path

import click
from dictionaryutils import DataDictionary, dictionary

from ..base import avro_record
from ..cli import from_command
from ..reader import PFBReader
from .linkml_utils import linkml2gen3_dict
from .gen3dict import (
    write_from_dict,
    _get_ontology_references,
    _get_links_for_node,
    _get_link,
)


@from_command.command(
    "linkml", short_help="Convert LinkML schema and data into a PFB file."
)
@click.argument("path", default=".", type=click.Path(exists=True, file_okay=False))
@click.option(
    "-s",
    "--schema",
    required=True,
    type=click.Path(exists=True, file_okay=True),
    help="Path to LinkML schema file (YAML).",
)
@click.option("--program", required=True, help="Name of the program.")
@click.option("--project", required=True, help="Name of the project.")
@click.pass_context
def from_linkml(ctx, path, schema, program, project):
    """Convert LinkML schema and data files into a PFB file.

    Accepts a LinkML schema (YAML file) and a directory containing JSON or TSV data files.
    The data files are expected to be directly under PATH, matching "*.json" or "*.tsv".
    Each file should be a single JSON list of objects or a TSV table that matches the
    specified schema. Objects should contain at least the "submitter_id" field.
    """
    try:
        with ctx.obj["writer"] as writer:
            if writer.isatty:
                click.secho(
                    "Error: cannot output to TTY.", fg="red", bold=True, err=True
                )
                return

            click.secho(
                "Converting LinkML schema to Gen3 format...", fg="cyan", err=True
            )
            # Convert LinkML to Gen3 dict format
            gen3_dict_data = linkml2gen3_dict(schema)

            # Create a temporary Gen3 DataDictionary-like structure
            click.secho("Loading schema...", fg="cyan", err=True)
            _write_from_linkml(writer, gen3_dict_data, path, program, project)
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    else:
        click.secho("Done!", fg="green", err=True, bold=True)


def _get_avro_type(json_type, json_format=None):
    """Convert JSON schema type to AVRO type.

    Args:
        json_type: JSON schema type string
        json_format: Optional JSON schema format (e.g., "date-time", "uri")

    Returns:
        AVRO type string
    """
    type_map = {
        "string": "string",
        "integer": "long",
        "number": "double",
        "boolean": "boolean",
    }
    return type_map.get(json_type, "string")


def _write_from_linkml(writer, gen3_dict_data, data_path, program, project):
    """Write PFB from LinkML schema and data.

    Args:
        writer: PFBWriter instance
        gen3_dict_data: Dict of Gen3 schema nodes
        data_path: Path to data directory
        program: Program name
        project: Project name
    """
    # Convert gen3_dict_data to AVRO schema format
    # Build list of node schemas for set_schema
    node_schemas = []
    for node_name, node_def in gen3_dict_data.items():
        # Create an AVRO-compatible node schema
        node_schema = {"name": node_name, "type": "record", "fields": []}

        # Add properties as fields
        if "properties" in node_def:
            for prop_name, prop_def in node_def["properties"].items():
                # Skip special properties that aren't real fields
                if prop_name in ["$ref", "type"]:
                    continue

                # Create field definition
                field = {
                    "name": prop_name,
                    "type": ["null", "string"],  # Default to nullable string
                }

                # Try to extract type information
                if isinstance(prop_def, dict):
                    if "type" in prop_def:
                        prop_type = prop_def["type"]
                        prop_format = prop_def.get("format")

                        # Handle "array" string type (from linkml_utils conversion)
                        if prop_type == "array":
                            # Array type - create proper AVRO array
                            field["type"] = [
                                "null",
                                {"type": "array", "items": "string"},
                            ]
                        # Handle complex types like arrays (dict format)
                        elif isinstance(prop_type, dict):
                            # Complex type (e.g., array definition)
                            if prop_type.get("type") == "array":
                                # Create proper AVRO array type
                                items_type = prop_type.get("items", {"type": "string"})
                                if isinstance(items_type, str):
                                    items_type = {"type": items_type}
                                # For array items, extract just the type info
                                if (
                                    isinstance(items_type, dict)
                                    and "type" in items_type
                                ):
                                    items_avro_type = _get_avro_type(items_type["type"])
                                    field["type"] = [
                                        "null",
                                        {"type": "array", "items": items_avro_type},
                                    ]
                                else:
                                    field["type"] = [
                                        "null",
                                        {"type": "array", "items": "string"},
                                    ]
                            else:
                                # Other complex types - default to string
                                field["type"] = ["null", "string"]
                        else:
                            # Simple type string - convert to AVRO type
                            avro_type = _get_avro_type(prop_type, prop_format)
                            field["type"] = ["null", avro_type]
                    if "description" in prop_def:
                        field["doc"] = prop_def["description"]

                node_schema["fields"].append(field)

        node_schemas.append(node_schema)

    # Create nodes metadata
    nodes_json = []
    for node_name, node_def in gen3_dict_data.items():
        properties = []

        # Extract properties from node definition
        if "properties" in node_def:
            for prop_name, prop_def in node_def["properties"].items():
                if prop_name not in ["$ref", "type"]:
                    ontology_reference = ""
                    values = {}
                    if isinstance(prop_def, dict):
                        values = {
                            k: str(v)
                            for k, v in prop_def.items()
                            if k not in ["$ref", "description"] and v is not None
                        }
                        if "description" in prop_def:
                            values["description"] = prop_def["description"]

                    prop_json = {
                        "name": prop_name,
                        "ontology_reference": ontology_reference,
                        "values": values,
                    }
                    properties.append(prop_json)

        # Extract links from node definition
        links = []
        if "links" in node_def:
            for link in node_def["links"]:
                link_def = {
                    "name": link.get("name"),
                    "dst": link.get("target_type"),
                }
                if "multiplicity" in link:
                    link_def["multiplicity"] = link["multiplicity"].upper()
                links.append(link_def)

        node_json = {
            "name": node_name,
            "ontology_reference": "",
            "values": node_def.get("values", {}),
            "links": links,
            "properties": properties,
        }
        nodes_json.append(node_json)

    metadata = {"nodes": nodes_json, "misc": {}}

    # Set schema and metadata on writer
    click.secho("Setting schema...", fg="cyan", err=True)
    writer.set_schema(node_schemas)
    writer.set_metadata(metadata)

    # Detect data format and process accordingly
    json_files = glob.glob(os.path.join(data_path, "*.json"))
    tsv_files = glob.glob(os.path.join(data_path, "*.tsv"))

    if json_files:
        click.secho("Loading JSON data...", fg="cyan", err=True)
        writer.write(_from_linkml_json(metadata, data_path, program, project))
    elif tsv_files:
        click.secho("Loading TSV data...", fg="cyan", err=True)
        writer.write(
            _from_linkml_tsv(metadata, writer.schema, data_path, program, project)
        )
    else:
        click.secho("No JSON or TSV data files found.", fg="yellow", err=True)
        writer.write()


def _from_linkml_json(metadata, path, program, project):
    """Load JSON data for LinkML-based PFB.

    Args:
        metadata: PFB metadata with nodes definition
        path: Path to data directory
        program: Program name
        project: Project name

    Yields:
        AVRO records
    """
    link_dests = {
        node["name"]: {link["name"]: link["dst"] for link in node["links"]}
        for node in metadata["nodes"]
    }

    # Build a mapping from snake_case filenames to node names in metadata
    # This handles both snake_case and CamelCase filenames
    available_nodes = {node["name"]: node["name"] for node in metadata["nodes"]}

    order = glob.glob(os.path.join(path, "*.json"))
    total = len(order)

    for i, o in enumerate(order):
        file_base = os.path.basename(o).replace(".json", "").strip()

        # Try to find the node in three ways:
        # 1. Direct match (already in snake_case)
        # 2. Convert from CamelCase to snake_case
        # 3. Case-insensitive match as fallback
        node_name = available_nodes.get(file_base)

        if node_name is None:
            # Try converting CamelCase to snake_case
            from .linkml_utils import to_snake_case

            snake_case_name = to_snake_case(file_base)
            node_name = available_nodes.get(snake_case_name)

        if node_name is None:
            # Try case-insensitive lookup
            for available in available_nodes.keys():
                if available.lower() == file_base.lower():
                    node_name = available
                    break

        if node_name is None:
            click.secho("{}/{}: ".format(i + 1, total), fg="blue", nl=False, err=True)
            click.secho(
                "SKIPPED (not in schema): {}".format(file_base), fg="yellow", err=True
            )
            continue

        click.secho("{}/{}: ".format(i + 1, total), fg="blue", nl=False, err=True)
        click.secho(node_name, fg="white", err=True)

        with open(o, "r") as f:
            json_data = json.load(f)

        if isinstance(json_data, dict):
            json_data = [json_data]

        for json_record in json_data:
            record = _convert_json_record(
                node_name, json_record, program, project, link_dests
            )
            yield record


def _convert_json_record(node_name, json_record, program, project, link_dests):
    """Convert a JSON record to an AVRO record.

    Args:
        node_name: Name of the node type
        json_record: JSON record data
        program: Program name
        project: Project name
        link_dests: Dict mapping node names to link destinations

    Returns:
        AVRO record
    """
    relations = []
    try:
        node_id = json_record["submitter_id"]
    except KeyError:
        node_id = json_record.get("code")

    vals = json_record.copy()

    to_del = None
    for item in json_record:
        if isinstance(json_record[item], dict) and "submitter_id" in json_record[item]:
            to_del = item
            v = item
            if node_name in link_dests and v in link_dests[node_name]:
                relations.append(
                    {
                        "dst_id": json_record[item]["submitter_id"],
                        "dst_name": link_dests[node_name][v],
                    }
                )

    if to_del and to_del in vals:
        del vals[to_del]

    vals["project_id"] = "{}-{}".format(program, project)
    vals["created_datetime"] = None
    vals["updated_datetime"] = None

    return avro_record(node_id, node_name, vals, relations)


def _from_linkml_tsv(metadata, schema, path, program, project):
    """Load TSV data for LinkML-based PFB.

    Args:
        metadata: PFB metadata with nodes definition
        schema: PFB schema
        path: Path to data directory
        program: Program name
        project: Project name

    Yields:
        AVRO records
    """
    from .linkml_utils import to_snake_case

    link_dests = {
        node["name"]: {link["name"]: link["dst"] for link in node["links"]}
        for node in metadata["nodes"]
    }

    order = glob.glob(os.path.join(path, "*.tsv"))
    total = len(order)

    for i, o in enumerate(order):
        node_name = os.path.basename(o).replace(".tsv", "").strip()
        click.secho("{}/{}: ".format(i + 1, total), fg="blue", nl=False, err=True)
        click.secho(node_name, fg="white", err=True)

        tsv_data = list(csv.DictReader(open(o), delimiter="\t"))

        if isinstance(tsv_data, dict):
            tsv_data = [tsv_data]

        for tsv_record in tsv_data:
            # Build a column name mapping to handle both TitleCase and snake_case
            # Get the schema node to find valid field names
            node_schema = None
            for n in schema:
                if n["name"] == node_name:
                    node_schema = n
                    break

            if node_schema is None:
                continue

            # Create a mapping from snake_case field names to actual schema field names
            valid_fields = {f["name"]: f["name"] for f in node_schema["fields"]}

            # Normalize TSV column headers: try exact match first, then snake_case conversion
            normalized_record = {}
            for col_name, col_value in tsv_record.items():
                # Try direct match first (already snake_case)
                if col_name in valid_fields:
                    normalized_record[col_name] = col_value
                else:
                    # Try converting from TitleCase/PascalCase to snake_case
                    snake_col_name = to_snake_case(col_name)
                    if snake_col_name in valid_fields:
                        normalized_record[snake_col_name] = col_value
                    else:
                        # Try case-insensitive match as fallback
                        for valid_field in valid_fields.keys():
                            if valid_field.lower() == col_name.lower():
                                normalized_record[valid_field] = col_value
                                break

            # Convert types for normalized record
            for k, v in normalized_record.items():
                field_type = _get_type_from_schema(schema, node_name, k)
                normalized_record[k] = _convert_types(v, field_type)

            record = _convert_tsv_record(
                node_name, normalized_record, program, project, link_dests
            )
            yield record


def _convert_types(val, field_type):
    """Convert TSV string values to appropriate Python types.

    Args:
        val: String value from TSV
        field_type: Target field type (can be a string or dict for arrays)

    Returns:
        Converted value
    """
    import json

    # Handle array types (dict with 'type': 'array')
    if isinstance(field_type, dict) and field_type.get("type") == "array":
        if val is None or val.strip() == "":
            return None
        val_stripped = val.strip()
        # Try to parse as JSON array
        if val_stripped.startswith("[") and val_stripped.endswith("]"):
            try:
                parsed = json.loads(val_stripped)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                # If JSON parsing fails, fall through to manual parsing
                pass
        # Fallback to manual parsing for non-JSON formats
        arrayStrip = val_stripped.strip("[]'\"")
        if arrayStrip:
            return [item.strip() for item in arrayStrip.split(",")]
        return None

    if field_type == "string" or field_type == "enum":
        if val is None or val.strip() == "":
            return None
        return str(val)
    elif field_type == "double":
        if (
            val is None
            or val.strip() == ""
            or val.strip() == "null"
            or val.strip() == "Null"
        ):
            return None
        return float(val)
    elif field_type == "integer" or field_type == "long":
        if (
            val is None
            or val.strip() == ""
            or val.strip() == "null"
            or val.strip() == "Null"
        ):
            return None
        return int(val)
    elif field_type == "boolean":
        if val is None or val.strip() == "":
            return None
        if val.lower() == "false":
            return False
        if val.lower() == "true":
            return True
    else:
        # finally if the type doesn't match any case then we return the supplied value
        return val


def _get_type_from_schema(schema, node, field):
    """Get the field type from schema.

    Args:
        schema: PFB schema
        node: Node name
        field: Field name

    Returns:
        Field type string or dict (for array types)
    """
    nodes = None
    for n in schema:
        if n["name"] == node:
            nodes = n
            break
    if nodes is None:
        return None

    field_type = None
    for f in nodes["fields"]:
        if f["name"] == field:
            # usually the first type is "null" to allow for empty values
            # the second value is the type that the field should conform to. i.e. string, number
            for t in f["type"]:
                if t == "null":
                    continue
                else:
                    if isinstance(t, dict):
                        # Check if it's an array type or an enum
                        if t.get("type") == "array":
                            # Return the array definition dict
                            field_type = t
                        else:
                            # It's an enum or other complex type
                            field_type = "enum"
                    else:
                        field_type = t
        if field_type:
            break

    return field_type


def _convert_tsv_record(node_name, tsv_record, program, project, link_dests):
    """Convert a TSV record to an AVRO record.

    Args:
        node_name: Name of the node type
        tsv_record: TSV record data
        program: Program name
        project: Project name
        link_dests: Dict mapping node names to link destinations

    Returns:
        AVRO record
    """
    relations = []
    try:
        node_id = tsv_record["submitter_id"]
    except KeyError:
        if node_name == "program":
            node_id = tsv_record["dbgap_accession_number"]
        else:
            node_id = tsv_record.get("code")

    vals = tsv_record.copy()

    to_del = None
    for item in tsv_record:
        if isinstance(tsv_record[item], dict) and "submitter_id" in tsv_record[item]:
            to_del = item
            v = item
            if node_name in link_dests and v in link_dests[node_name]:
                relations.append(
                    {
                        "dst_id": tsv_record[item]["submitter_id"],
                        "dst_name": link_dests[node_name][v],
                    }
                )

    if to_del and to_del in vals:
        del vals[to_del]

    vals["project_id"] = "{}-{}".format(program, project)
    vals["created_datetime"] = None
    vals["updated_datetime"] = None

    return avro_record(node_id, node_name, vals, relations)
