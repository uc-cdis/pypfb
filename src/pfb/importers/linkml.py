"""LinkML schema and data importer for PFB."""

from __future__ import absolute_import

import glob
import json
import logging
import os
import csv
import tempfile

import click
from dictionaryutils import DataDictionary, dictionary

from ..base import avro_record
from ..cli import from_command
from .gen3_definitions import GEN3_DEFINITIONS
from .linkml_utils import iter_link_members, linkml2gen3_dict, to_snake_case
from .tsv import convert_types, get_type_from_schema

logger = logging.getLogger(__name__)


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


def _identifier_fields(gen3_dict_data):
    """Map node id -> the name of its LinkML identifier slot, where there is one.

    linkml2gen3_dict records a class's identifier slot as the first single-slot
    entry in the node's ``uniqueKeys``, so that is what we read here.

    When a schema declares no identifier the first single-slot key is Gen3's own
    ``["id"]``.  That is harmless: Gen3 excludes ``id`` from a node's AVRO
    fields, so records generally carry no ``id`` value and :func:`_entity_id`
    simply falls through to its next candidate.
    """
    id_fields = {}
    for node_id, node in gen3_dict_data.items():
        for key in node.get("uniqueKeys") or []:
            if len(key) == 1:
                id_fields[node_id] = key[0]
                break
    return id_fields


def _build_data_dictionary(gen3_dict_data):
    """Load LinkML-derived Gen3 nodes into a ``dictionaryutils.DataDictionary``.

    ``dictionaryutils`` only knows how to load a dictionary from a directory, a
    URL, or a JSON file, so the in-memory nodes produced by
    :func:`linkml2gen3_dict` are serialised to a temporary JSON file in the same
    ``{"<node>.yaml": {...}}`` shape that ``pfb from dict`` consumes.
    :data:`GEN3_DEFINITIONS` is included under the ``_definitions.yaml`` key so
    the ``$ref``s emitted by :func:`linkml2gen3_dict` resolve.

    Args:
        gen3_dict_data: Dict of Gen3 schema nodes keyed by snake_case node id

    Returns:
        An initialised DataDictionary with all ``$ref``s resolved
    """
    payload = {
        "_definitions.yaml": GEN3_DEFINITIONS,
        "_settings.yaml": {},
    }
    for node_id, node in gen3_dict_data.items():
        payload["{}.yaml".format(node_id)] = node

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp_file:
            json.dump(payload, tmp_file)
            tmp_path = tmp_file.name
        d = DataDictionary(local_file=tmp_path)
    finally:
        if tmp_path is not None:
            os.unlink(tmp_path)

    dictionary.init(d)
    return d


def _write_from_linkml(writer, gen3_dict_data, data_path, program, project):
    """Write PFB from LinkML schema and data.

    The AVRO schema and metadata are produced by the very same
    ``gen3dict`` routines that back ``pfb from dict``, so a LinkML schema and the
    equivalent Gen3 data dictionary yield the same PFB schema -- including Gen3's
    AVRO enum encoding, resolved ``$ref``s, defaults and ontology references.

    Args:
        writer: PFBWriter instance
        gen3_dict_data: Dict of Gen3 schema nodes
        data_path: Path to data directory
        program: Program name
        project: Project name
    """
    # Imported lazily: gen3dict imports ..cli, which loads this module as a
    # plugin entry point, so a module-level import would be circular.
    from .gen3dict import _get_ontology_references, _parse_dictionary

    # Each node's LinkML identifier slot, recorded by linkml2gen3_dict as a
    # single-slot uniqueKey.  Used as the source for PFB entity IDs.
    id_fields = _identifier_fields(gen3_dict_data)
    # Slots whose abstract range expanded into a Gen3 link subgroup.
    poly_slots = _polymorphic_slots(gen3_dict_data)

    d = _build_data_dictionary(gen3_dict_data)

    click.secho("Parsing dictionary...", fg="cyan", err=True)
    node_schemas, ontology_references, links = _parse_dictionary(d)
    metadata = _get_ontology_references(ontology_references, links)

    click.secho("Setting schema...", fg="cyan", err=True)
    writer.set_schema(node_schemas)
    writer.set_metadata(metadata)

    # Detect data format and process accordingly
    json_files = glob.glob(os.path.join(data_path, "*.json"))
    tsv_files = glob.glob(os.path.join(data_path, "*.tsv"))

    if json_files:
        click.secho("Loading JSON data...", fg="cyan", err=True)
        writer.write(
            _from_linkml_json(
                metadata, data_path, program, project, id_fields, poly_slots
            )
        )
    elif tsv_files:
        click.secho("Loading TSV data...", fg="cyan", err=True)
        writer.write(
            _from_linkml_tsv(
                metadata, writer.schema, data_path, program, project, id_fields
            )
        )
    else:
        click.secho("No JSON or TSV data files found.", fg="yellow", err=True)
        writer.write()


def _polymorphic_slots(gen3_dict_data):
    """Map ``node -> {slot name: {candidate target node, ...}}`` for subgroups.

    A slot whose LinkML range is an abstract class compiles to a Gen3 link
    subgroup with one member per concrete descendant, each named
    ``<slot>_<target>``.  Data authored against the LinkML schema refers to the
    relationship by the *slot* name, so the importer needs to know which targets
    that slot could mean.
    """
    groups = {}
    for node_id, node in gen3_dict_data.items():
        for link in node.get("links") or []:
            if "subgroup" not in link or not link.get("name"):
                continue
            targets = {member["target_type"] for member in iter_link_members(link)}
            if targets:
                groups.setdefault(node_id, {})[link["name"]] = targets
    return groups


def _node_for_file(file_base, available_nodes):
    """Resolve a data file's base name to a node name in *available_nodes*, or None.

    Tries a direct match, then CamelCase -> snake_case, then case-insensitively.
    *available_nodes* may be any container of node names.
    """
    if file_base in available_nodes:
        return file_base

    snake = to_snake_case(file_base)
    if snake in available_nodes:
        return snake

    for available in available_nodes:
        if available.lower() == file_base.lower():
            return available
    return None


def _index_entity_ids(data_path, available_nodes, id_fields, wanted):
    """First pass over the data: map each entity ID to the node(s) declaring it.

    Only nodes in *wanted* are indexed -- the possible targets of polymorphic
    slots.  PFB imposes no global uniqueness on entity IDs (the exporters key by
    ``(node name, id)``), so an ID may legitimately repeat across node types;
    resolution only has to be unambiguous among one slot's candidate targets.
    """
    index = {}
    for data_file in sorted(glob.glob(os.path.join(data_path, "*.json"))):
        file_base = os.path.basename(data_file).replace(".json", "").strip()
        node_name = _node_for_file(file_base, available_nodes)
        if node_name is None or node_name not in wanted:
            continue

        with open(data_file, "r") as handle:
            records = json.load(handle)
        if isinstance(records, dict):
            records = [records]

        for record in records:
            entity_id = _entity_id(record, node_name, id_fields)
            if entity_id is not None:
                index.setdefault(entity_id, set()).add(node_name)
    return index


def _from_linkml_json(
    metadata, path, program, project, id_fields=None, poly_slots=None
):
    """Load JSON data for LinkML-based PFB.

    Args:
        metadata: PFB metadata with nodes definition
        path: Path to data directory
        program: Program name
        project: Project name
        id_fields: Optional map of node name -> identifier slot name
        poly_slots: Optional map of node -> {slot: candidate target nodes}

    Yields:
        AVRO records
    """
    id_fields = id_fields or {}
    poly_slots = poly_slots or {}
    link_dests = {
        node["name"]: {link["name"]: link["dst"] for link in node["links"]}
        for node in metadata["nodes"]
    }

    # Build a mapping from snake_case filenames to node names in metadata
    # This handles both snake_case and CamelCase filenames
    available_nodes = {node["name"]: node["name"] for node in metadata["nodes"]}

    # Resolving a polymorphic reference needs to know which node declares the
    # referenced ID, so index those nodes' IDs before emitting any records.
    poly_targets = {
        target
        for slots in poly_slots.values()
        for targets in slots.values()
        for target in targets
    }
    id_index = (
        _index_entity_ids(path, available_nodes, id_fields, poly_targets)
        if poly_targets
        else {}
    )

    order = glob.glob(os.path.join(path, "*.json"))
    total = len(order)

    for i, o in enumerate(order):
        file_base = os.path.basename(o).replace(".json", "").strip()
        node_name = _node_for_file(file_base, available_nodes)

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
                node_name,
                json_record,
                program,
                project,
                link_dests,
                id_fields,
                poly_slots.get(node_name),
                id_index,
            )
            yield record


# Tried, in order, after a schema-declared identifier: the conventional Gen3
# identity fields, then Gen3's own `id`.  One chain is used for both naming an
# entity and referring to one, so anything usable as an ID is also resolvable.
_ID_FALLBACKS = ("submitter_id", "code", "dbgap_accession_number", "id")


def _first_present(mapping, fields):
    """Return the first non-null value among *fields* in *mapping*."""
    for field in fields:
        if field and mapping.get(field) is not None:
            return mapping[field]
    return None


def _entity_id(record, node_name, id_fields):
    """Return the PFB entity ID for *record*.

    The LinkML identifier slot is the declared source; the fallbacks keep
    Gen3-shaped data working when a schema declares no identifier.
    """
    identifier = (id_fields or {}).get(node_name)
    return _first_present(
        record, ([identifier] if identifier else []) + list(_ID_FALLBACKS)
    )


def _reference_id(value, candidates, id_fields):
    """Extract the referenced entity's ID from a link value.

    LinkML serialises a non-inlined reference as the target's identifier
    string, which is returned as-is.  An inlined link is a small object
    identifying the target, e.g. ``{"submitter_id": "..."}``; *candidates* are
    the node names the reference could point at, and their identifier slots are
    preferred so references resolve to the same IDs :func:`_entity_id` produces.
    """
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return None
    identifiers = [(id_fields or {}).get(name) for name in sorted(candidates)]
    return _first_present(value, identifiers + list(_ID_FALLBACKS))


def _split_references(value, candidates, id_fields):
    """Split a link slot's value into individual references.

    A multivalued slot holds a list of references, or -- when inlined with an
    identified range -- a dict keyed by identifier.  That dict form is told
    apart from a single inlined object by carrying no identifier field of its
    own, with every value an object (or null); its keys are the referenced IDs.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if (
        isinstance(value, dict)
        and value
        and _reference_id(value, candidates, id_fields) is None
        and all(v is None or isinstance(v, dict) for v in value.values())
    ):
        return list(value)
    return [value]


def _resolve_polymorphic(node_name, slot_name, value, candidates, id_fields, id_index):
    """Resolve a reference through a polymorphic slot to ``(dst_id, dst_name)``.

    The reference names only an ID, so the target node is recovered by looking
    that ID up among the slot's candidate targets.  Entity IDs need not be
    globally unique in a PFB, so this only has to be unambiguous within one
    slot's candidates -- and both an unresolvable and an ambiguous reference are
    reported rather than silently dropped.
    """
    reference_id = _reference_id(value, candidates, id_fields)
    if reference_id is None:
        logger.warning(
            "%s.%s references an object with no recognisable identifier: %r",
            node_name,
            slot_name,
            value,
        )
        return None, None

    matches = sorted((id_index.get(reference_id) or set()) & set(candidates))
    if not matches:
        logger.warning(
            "%s.%s references %r, which matches no record in any of its target "
            "types (%s) - relation dropped",
            node_name,
            slot_name,
            reference_id,
            ", ".join(sorted(candidates)),
        )
        return None, None
    if len(matches) > 1:
        logger.warning(
            "%s.%s references %r, which is ambiguous across %s - relation dropped",
            node_name,
            slot_name,
            reference_id,
            ", ".join(matches),
        )
        return None, None

    return reference_id, matches[0]


def _convert_json_record(
    node_name,
    json_record,
    program,
    project,
    link_dests,
    id_fields=None,
    poly_slots=None,
    id_index=None,
):
    """Convert a JSON record to an AVRO record.

    Args:
        node_name: Name of the node type
        json_record: JSON record data
        program: Program name
        project: Project name
        link_dests: Dict mapping node names to link destinations
        id_fields: Optional map of node name -> identifier slot name
        poly_slots: Optional map of slot name -> candidate target node names
        id_index: Optional map of entity ID -> node names declaring it

    Returns:
        AVRO record
    """
    poly_slots = poly_slots or {}
    id_index = id_index or {}
    relations = []
    node_id = _entity_id(json_record, node_name, id_fields)

    vals = json_record.copy()

    # Determine the set of known link field names for this node so we can
    # strip them from `vals` regardless of their exact content.
    node_link_names = set((link_dests.get(node_name) or {}).keys())

    to_delete = []
    for item, value in json_record.items():
        if item not in node_link_names and item not in poly_slots:
            continue
        # A link slot is never a property value, so it is always stripped --
        # including a reference that identifies nothing (e.g. a
        # {"code": "test"} project ref) or that was already reported.
        to_delete.append(item)

        dst_name = (link_dests.get(node_name) or {}).get(item)
        candidates = [dst_name] if dst_name else poly_slots[item]
        for reference in _split_references(value, candidates, id_fields):
            if dst_name:
                dst_id = _reference_id(reference, [dst_name], id_fields)
                target = dst_name
            else:
                # A polymorphic slot: the data names the relationship by its
                # LinkML slot name, while the compiler split it into one link
                # per concrete target, so recover the target from the ID.
                dst_id, target = _resolve_polymorphic(
                    node_name, item, reference, candidates, id_fields, id_index
                )
            if dst_id is not None:
                relations.append({"dst_id": dst_id, "dst_name": target})

    for item in to_delete:
        vals.pop(item, None)

    vals["project_id"] = "{}-{}".format(program, project)
    vals["created_datetime"] = None
    vals["updated_datetime"] = None

    return avro_record(node_id, node_name, vals, relations)


def _normalize_column(col_name, valid_fields):
    """Map a TSV column header onto a schema field name, or None.

    Tries an exact match, then TitleCase/PascalCase -> snake_case, then a
    case-insensitive match.  ``<link>.<field>`` columns are passed through
    unchanged so :func:`_convert_tsv_record` can turn them into relations.
    """
    if "." in col_name:
        return col_name
    if col_name in valid_fields:
        return col_name
    snake = to_snake_case(col_name)
    if snake in valid_fields:
        return snake
    for valid_field in valid_fields:
        if valid_field.lower() == col_name.lower():
            return valid_field
    return None


def _from_linkml_tsv(metadata, schema, path, program, project, id_fields=None):
    """Load TSV data for LinkML-based PFB.

    Args:
        metadata: PFB metadata with nodes definition
        schema: PFB schema
        path: Path to data directory
        program: Program name
        project: Project name
        id_fields: Optional map of node name -> identifier slot name

    Yields:
        AVRO records
    """
    link_dests = {
        node["name"]: {link["name"]: link["dst"] for link in node["links"]}
        for node in metadata["nodes"]
    }
    available_nodes = {node["name"] for node in metadata["nodes"]}
    fields_by_node = {node["name"]: node["fields"] for node in schema}

    order = sorted(glob.glob(os.path.join(path, "*.tsv")))
    total = len(order)

    for i, data_file in enumerate(order):
        file_base = os.path.basename(data_file).replace(".tsv", "").strip()
        node_name = _node_for_file(file_base, available_nodes)

        click.secho("{}/{}: ".format(i + 1, total), fg="blue", nl=False, err=True)
        if node_name is None or node_name not in fields_by_node:
            click.secho(
                "SKIPPED (not in schema): {}".format(file_base), fg="yellow", err=True
            )
            continue
        click.secho(node_name, fg="white", err=True)

        # Per-file, not per-row: the schema does not change between rows.
        valid_fields = {field["name"] for field in fields_by_node[node_name]}
        field_types = {
            name: get_type_from_schema(schema, node_name, name) for name in valid_fields
        }

        with open(data_file) as handle:
            for tsv_record in csv.DictReader(handle, delimiter="\t"):
                record = {}
                for col_name, col_value in tsv_record.items():
                    name = _normalize_column(col_name, valid_fields)
                    if name is None:
                        continue
                    record[name] = (
                        col_value
                        if "." in name
                        else convert_types(col_value, field_types.get(name))
                    )

                yield _convert_tsv_record(
                    node_name, record, program, project, link_dests, id_fields
                )


def _convert_tsv_record(
    node_name, tsv_record, program, project, link_dests, id_fields=None
):
    """Convert a TSV record to an AVRO record.

    A TSV cell is always text, so a relation cannot be written as a nested
    object the way the JSON path allows.  The convention -- matching what
    ``pfb to tsv`` emits -- is a ``<link>.<field>`` column, e.g.
    ``participants.submitter_id``.  The link name is resolved through
    ``link_dests`` so ``dst_name`` is the target *node*, consistent with the
    JSON path.

    Args:
        node_name: Name of the node type
        tsv_record: TSV record data
        program: Program name
        project: Project name
        link_dests: Dict mapping node names to link destinations
        id_fields: Optional map of node name -> identifier slot name

    Returns:
        AVRO record
    """
    relations = []
    node_id = _entity_id(tsv_record, node_name, id_fields)
    node_links = link_dests.get(node_name) or {}

    # A link may arrive as several columns -- `pfb to tsv` writes both
    # `participants.id` and `participants.submitter_id` -- so collect them per
    # link and emit a single relation, preferring the target's identifier.
    by_link = {}
    vals = tsv_record.copy()
    for column, value in tsv_record.items():
        if "." not in column:
            continue
        link_name, field = column.split(".", 1)
        if link_name not in node_links:
            continue
        vals.pop(column, None)
        if value not in (None, "", "null"):
            by_link.setdefault(link_name, {})[field] = value

    for link_name, fields in by_link.items():
        dst_name = node_links[link_name]
        identifier = (id_fields or {}).get(dst_name)
        for field in ([identifier] if identifier else []) + ["submitter_id", "id"]:
            if field in fields:
                relations.append({"dst_id": fields[field], "dst_name": dst_name})
                break

    vals["project_id"] = "{}-{}".format(program, project)
    vals["created_datetime"] = None
    vals["updated_datetime"] = None

    return avro_record(node_id, node_name, vals, relations)
