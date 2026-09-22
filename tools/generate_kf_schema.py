#!/usr/bin/env python3
"""Generate a LinkML schema from the Kids First (KF) Gen3 data dictionary (kf.json).

The generated schema follows the style conventions of bdchm.yaml:
  - An abstract ``Entity`` base class carries the shared ``submitter_id`` identifier.
  - A ``DataFileMixin`` captures the seven file-metadata properties shared by all
    data-file nodes, so they are not copy-pasted across 15+ classes.
  - A ``WorkflowMixin`` captures the four workflow-run properties shared by all
    analysis-workflow nodes.
  - Concrete classes use ``is_a: Entity`` (and optionally ``mixins:``) for proper
    LinkML inheritance rather than repeating base attributes inline.
  - Descriptions use block-scalar style (``>-``) instead of quoted inline strings.
  - Optional attributes carry an explicit ``required: false``.
  - Enum deduplication: permissible-value sets shared across multiple nodes are
    defined once under a canonical name; duplicate ``*Enum`` definitions are
    replaced by references to the canonical enum.

Usage:
    python tools/generate_kf_schema.py
Writes:  tests/schema/kf_schema.yaml
"""

import json
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def to_camel_case(snake: str) -> str:
    """snake_case → CamelCase."""
    return "".join(part.title() for part in snake.split("_"))


def linkml_type(json_type) -> str:
    """Best-effort mapping of a JSON Schema type to a LinkML primitive."""
    if isinstance(json_type, list):
        for t in json_type:
            if t != "null":
                json_type = t
                break
        else:
            return "string"
    return {
        "string": "string",
        "integer": "integer",
        "number": "float",
        "boolean": "boolean",
        "array": "string",
    }.get(json_type, "string")


def clean_description(raw: str) -> str:
    """Normalise a description: collapse interior whitespace/newlines."""
    return " ".join(raw.split()).strip()


def block_scalar(text: str, indent: int) -> str:
    """Render *text* as a YAML ``>-`` block scalar at *indent* spaces."""
    prefix = " " * (indent + 2)
    words = text.split()
    lines = []
    current: list[str] = []
    width = 0
    for word in words:
        if width + len(word) + (1 if current else 0) > 78:
            lines.append(prefix + " ".join(current))
            current = [word]
            width = len(word)
        else:
            width += (1 + len(word)) if current else len(word)
            current.append(word)
    if current:
        lines.append(prefix + " ".join(current))
    return ">-\n" + "\n".join(lines)


def yaml_scalar(v) -> str:
    if v is True:
        return "true"
    if v is False:
        return "false"
    if v is None:
        return "null"
    return str(v)


def yaml_str_value(s: str) -> str:
    """Return a safe single-line YAML string value (quoted if necessary).

    Quotes strings that contain special YAML characters OR that YAML 1.1
    would silently coerce to a non-string type (booleans, null, etc.).
    """
    # YAML 1.1 reserved words that must be quoted to remain strings
    _YAML_RESERVED = {
        "true",
        "false",
        "yes",
        "no",
        "on",
        "off",
        "null",
        "~",
        "True",
        "False",
        "Yes",
        "No",
        "On",
        "Off",
        "Null",
        "TRUE",
        "FALSE",
        "YES",
        "NO",
        "ON",
        "OFF",
        "NULL",
    }
    needs_quote = s in _YAML_RESERVED or any(
        c in s
        for c in (
            ":",
            "#",
            "{",
            "}",
            "[",
            "]",
            ",",
            "&",
            "*",
            "?",
            "|",
            "-",
            "<",
            ">",
            "=",
            "!",
            "%",
            "@",
            "`",
            '"',
            "'",
        )
    )
    if needs_quote or not s or s[0] in (" ", "\t") or s[-1] in (" ", "\t"):
        escaped = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return s


# ---------------------------------------------------------------------------
# Well-known property expansions from _definitions.yaml
# ---------------------------------------------------------------------------

# Properties shared by ALL data-file nodes (from $ref_file)
DATA_FILE_PROPS: dict[str, dict] = {
    "file_name": {"range": "string", "description": "The name of the file."},
    "file_size": {"range": "integer", "description": "The size of the file in bytes."},
    "md5sum": {"range": "string", "description": "The MD5 hash of the file."},
    "file_format": {"range": "string", "description": "The format of the file."},
    "file_url": {"range": "string", "description": "The URL of the file."},
    "object_id": {
        "range": "string",
        "description": "The GUID of the object in the index service.",
    },
    "state_comment": {
        "range": "string",
        "description": "Optional comment about the state of the file.",
    },
}

# Properties shared by ALL workflow nodes (from $ref_wf)
WORKFLOW_PROPS: dict[str, dict] = {
    "workflow_link": {
        "range": "string",
        "description": "Link to the workflow used for the analysis.",
    },
    "workflow_version": {
        "range": "string",
        "description": "Version of the workflow used.",
    },
    "workflow_start_datetime": {
        "range": "datetime",
        "description": "Start date/time of the workflow run.",
    },
    "workflow_end_datetime": {
        "range": "datetime",
        "description": "End date/time of the workflow run.",
    },
}

# System properties managed by Gen3 – not exposed in LinkML
SYSTEM_PROPS = {
    "id",
    "project_id",
    "state",
    "created_datetime",
    "updated_datetime",
    "file_state",
    "error_type",
    "type",
    "submitter_id",
}

# ---------------------------------------------------------------------------
# Semantic overrides for mixed-property deduplication groups.
# Key: frozenset of permissible values → preferred canonical enum name.
# ---------------------------------------------------------------------------
SEMANTIC_OVERRIDE: dict[frozenset, str] = {
    # Yes/No/Unknown used across unrelated properties on different nodes
    frozenset({"Yes", "No", "Unknown", "Not Reported"}): "YesNoUnknownEnum",
}


# ---------------------------------------------------------------------------
# Pass 1 – collect raw enums
# ---------------------------------------------------------------------------


def collect_raw_enums(kf_data: dict) -> dict:
    """Return a mapping (node_id, prop_name) → {name, values, description}.

    Iterates every node property that has an ``enum`` key in *kf_data*.
    """
    raw: dict[tuple, dict] = {}
    skip = {"type"} | SYSTEM_PROPS

    for yaml_key, node_def in kf_data.items():
        if yaml_key.startswith("_"):
            continue
        node_id = node_def.get("id", "")
        if not node_id:
            continue

        for prop_name, prop_def in node_def.get("properties", {}).items():
            if prop_name in skip or prop_name.startswith("$"):
                continue
            if not isinstance(prop_def, dict) or "enum" not in prop_def:
                continue
            values = [str(v) for v in prop_def["enum"] if v is not None]
            if not values:
                continue
            enum_name = f"{to_camel_case(node_id)}{to_camel_case(prop_name)}Enum"
            desc = clean_description(prop_def.get("description", "") or "")
            raw[(node_id, prop_name)] = {
                "name": enum_name,
                "values": values,  # preserve original order
                "description": desc,
            }
    return raw


# ---------------------------------------------------------------------------
# Pass 2 – build deduplication map
# ---------------------------------------------------------------------------


def build_dedup_map(raw_enums: dict) -> tuple[dict, dict]:
    """Group enums by permissible-value set; return canonical defs and alias map.

    Returns:
        canonical_enums:  enum_name → {description, values}
        name_map:         original_enum_name → canonical_enum_name
    """
    # frozenset(values) → list of (node_id, prop_name, enum_name, description, values_ordered)
    sig_to_entries: dict[frozenset, list] = defaultdict(list)
    for (node_id, prop_name), info in raw_enums.items():
        sig = frozenset(info["values"])
        sig_to_entries[sig].append(
            (node_id, prop_name, info["name"], info["description"], info["values"])
        )

    canonical_enums: dict[str, dict] = {}
    name_map: dict[str, str] = {}

    for sig, entries in sig_to_entries.items():
        if len(entries) == 1:
            _, _, ename, desc, vals = entries[0]
            canonical_enums[ename] = {"description": desc, "values": vals}
            name_map[ename] = ename
        else:
            # Semantic override for well-known mixed-property groups
            if sig in SEMANTIC_OVERRIDE:
                canonical_name = SEMANTIC_OVERRIDE[sig]
                desc = next((e[3] for e in entries if e[3]), "")
                vals = entries[0][4]
            else:
                # Pick shortest enum name; ties broken alphabetically
                sorted_entries = sorted(entries, key=lambda e: (len(e[2]), e[2]))
                _, _, canonical_name, desc, vals = sorted_entries[0]
                if not desc:
                    desc = next((e[3] for e in sorted_entries if e[3]), "")

            canonical_enums[canonical_name] = {"description": desc, "values": vals}
            for _, _, ename, _, _ in entries:
                name_map[ename] = canonical_name

    return canonical_enums, name_map


# ---------------------------------------------------------------------------
# Pass 3 – build class attribute dicts
# ---------------------------------------------------------------------------


def collect_links(node_def: dict) -> list:
    """Flatten links / subgroup entries into a flat list of link dicts."""
    links = []
    for link_def in node_def.get("links", []):
        if "subgroup" in link_def:
            links.extend(link_def["subgroup"])
        elif "name" in link_def:
            links.append(link_def)
    return links


def process_properties(
    node_id: str,
    node_def: dict,
    link_names: set,
    mixin_props: set,
    name_map: dict,
) -> dict:
    """Return an ``attributes`` dict for a concrete LinkML class.

    Parameters
    ----------
    node_id:    Gen3 node id (snake_case).
    node_def:   Full node definition from kf.json.
    link_names: Names of link properties (handled as slots with range=Class).
    mixin_props: Property names already covered by a mixin (excluded here).
    name_map:   Mapping from original enum name → canonical enum name.
    """
    attrs: dict = {}
    skip = {"type"} | link_names | SYSTEM_PROPS | mixin_props

    for prop_name, prop_def in node_def.get("properties", {}).items():
        if prop_name.startswith("$") or prop_name in skip:
            continue
        if not isinstance(prop_def, dict):
            continue
        if list(prop_def.keys()) == ["$ref"]:
            continue

        raw_desc = prop_def.get("description", "") or ""
        description = clean_description(raw_desc)

        # --- enum ---
        if "enum" in prop_def:
            values = [str(v) for v in prop_def["enum"] if v is not None]
            if values:
                original_name = (
                    f"{to_camel_case(node_id)}{to_camel_case(prop_name)}Enum"
                )
                canonical_name = name_map.get(original_name, original_name)
                attr: dict = {"range": canonical_name, "required": False}
                if description:
                    attr["description"] = description
                attrs[prop_name] = attr
            continue

        # --- typed property ---
        typ = prop_def.get("type")
        if typ is None:
            continue

        is_array = False
        if isinstance(typ, list):
            non_null = [t for t in typ if t != "null"]
            typ = non_null[0] if non_null else "string"
        elif isinstance(typ, dict) and typ.get("type") == "array":
            is_array = True
            items = typ.get("items", {})
            typ = items.get("type", "string") if isinstance(items, dict) else "string"

        if typ == "array":
            is_array = True
            typ = "string"

        ltype = linkml_type(typ)
        attr = {"range": ltype, "required": False}
        if description:
            attr["description"] = description
        if is_array:
            attr["multivalued"] = True
        attrs[prop_name] = attr

    return attrs


# ---------------------------------------------------------------------------
# YAML emitter helpers
# ---------------------------------------------------------------------------


def emit_attribute(lines: list, name: str, adef: dict, indent: int = 6) -> None:
    pad = " " * indent
    lines.append(f"{pad}{name}:")
    lines.append(f"{pad}  range: {adef.get('range', 'string')}")
    if adef.get("identifier"):
        lines.append(f"{pad}  identifier: true")
    req = adef.get("required", None)
    if req is True:
        lines.append(f"{pad}  required: true")
    elif req is False:
        lines.append(f"{pad}  required: false")
    if adef.get("multivalued"):
        lines.append(f"{pad}  multivalued: true")
    desc = adef.get("description", "")
    if desc:
        rendered = block_scalar(desc, indent + 2)
        lines.append(f"{pad}  description: {rendered}")


def emit_class(lines: list, cname: str, cdef: dict) -> None:
    lines.append(f"  {cname}:")
    if cdef.get("abstract"):
        lines.append("    abstract: true")
    if cdef.get("mixin"):
        lines.append("    mixin: true")
    if "is_a" in cdef:
        lines.append(f"    is_a: {cdef['is_a']}")
    if cdef.get("mixins"):
        lines.append("    mixins:")
        for m in cdef["mixins"]:
            lines.append(f"      - {m}")
    desc = clean_description(cdef.get("description", ""))
    if desc:
        rendered = block_scalar(desc, 4)
        lines.append(f"    description: {rendered}")
    if cdef.get("annotations"):
        lines.append("    annotations:")
        for k, v in cdef["annotations"].items():
            lines.append(f"      {k}: {yaml_scalar(v)}")
    if cdef.get("slots"):
        lines.append("    slots:")
        for s in cdef["slots"]:
            lines.append(f"      - {s}")
    if cdef.get("attributes"):
        lines.append("    attributes:")
        for aname, adef in cdef["attributes"].items():
            emit_attribute(lines, aname, adef, indent=6)
    lines.append("")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def generate(kf_path: Path, out_path: Path) -> None:
    with kf_path.open() as fh:
        kf_data = json.load(fh)

    # ------------------------------------------------------------------
    # Pass 1 + 2: collect and deduplicate enums
    # ------------------------------------------------------------------
    raw_enums = collect_raw_enums(kf_data)
    canonical_enums, name_map = build_dedup_map(raw_enums)

    # ------------------------------------------------------------------
    # Determine per-node mixin membership
    # ------------------------------------------------------------------
    node_metadata: dict[str, dict] = {}
    for yaml_key, node_def in kf_data.items():
        if yaml_key.startswith("_"):
            continue
        node_id = node_def.get("id", "")
        if not node_id:
            continue
        props = node_def.get("properties", {})
        node_metadata[node_id] = {
            "has_file_ref": "$ref_file" in props,
            "has_wf_ref": "$ref_wf" in props,
        }

    file_mixin_props = set(DATA_FILE_PROPS.keys())
    wf_mixin_props = set(WORKFLOW_PROPS.keys())

    # ------------------------------------------------------------------
    # Pass 3: build class definitions
    # ------------------------------------------------------------------
    classes: dict[str, dict] = {}

    classes["Entity"] = {
        "abstract": True,
        "description": "Base class for all Kids First data entities.",
        "slots": ["submitter_id"],
    }
    classes["DataFileMixin"] = {
        "mixin": True,
        "description": (
            "Common file-metadata properties shared by all data-file nodes. "
            "Derived from _definitions.yaml#/data_file_properties in the Gen3 data dictionary."
        ),
        "attributes": {
            name: {**defn, "required": False} for name, defn in DATA_FILE_PROPS.items()
        },
    }
    classes["WorkflowMixin"] = {
        "mixin": True,
        "description": (
            "Common properties shared by all analysis-workflow nodes. "
            "Derived from _definitions.yaml#/workflow_properties in the Gen3 data dictionary."
        ),
        "attributes": {
            name: {**defn, "required": False} for name, defn in WORKFLOW_PROPS.items()
        },
    }

    for yaml_key, node_def in kf_data.items():
        if yaml_key.startswith("_"):
            continue
        node_id = node_def.get("id", "")
        if not node_id:
            continue

        class_name = to_camel_case(node_id)
        category = node_def.get("category", "clinical")
        description = clean_description(node_def.get("description") or "")
        submittable = node_def.get("submittable", True)
        meta = node_metadata[node_id]

        mixins: list[str] = []
        mixin_props: set[str] = set()
        if meta["has_file_ref"]:
            mixins.append("DataFileMixin")
            mixin_props |= file_mixin_props
        if meta["has_wf_ref"]:
            mixins.append("WorkflowMixin")
            mixin_props |= wf_mixin_props

        raw_links = collect_links(node_def)
        link_names = {lk["name"] for lk in raw_links}

        link_attrs: dict[str, dict] = {}
        for lk in raw_links:
            target_cc = to_camel_case(lk["target_type"])
            mult = lk.get("multiplicity", "many_to_one")
            required = bool(lk.get("required", False))
            adef: dict = {"range": target_cc, "required": required}
            if mult in ("many_to_many", "one_to_many"):
                adef["multivalued"] = True
            link_attrs[lk["name"]] = adef

        prop_attrs = process_properties(
            node_id, node_def, link_names, mixin_props, name_map
        )

        cdef: dict = {
            "is_a": "Entity",
            "description": description,
            "annotations": {
                "gen3_category": category,
                "gen3_submittable": submittable,
            },
        }
        if mixins:
            cdef["mixins"] = mixins
        attributes = {**link_attrs, **prop_attrs}
        if attributes:
            cdef["attributes"] = attributes

        classes[class_name] = cdef

    # ------------------------------------------------------------------
    # Emit YAML
    # ------------------------------------------------------------------
    lines: list[str] = [
        "id: https://kidsirst.nci.nih.gov/schema/kf",
        "name: kf",
        "title: Kids First Data Model",
        "description: >-",
        "  LinkML representation of the Kids First (KF) Gen3 data dictionary.",
        "  Generated from kf.json; use tools/generate_kf_schema.py to regenerate.",
        "license: https://creativecommons.org/publicdomain/zero/1.0/",
        "see_also:",
        "  - https://kidsfirstdrc.org/",
        "",
        "prefixes:",
        "  linkml: https://w3id.org/linkml/",
        "  kf: https://kidsirst.nci.nih.gov/schema/kf/",
        "",
        "default_prefix: kf",
        "default_range: string",
        "",
        "imports:",
        "  - linkml:types",
        "",
        "classes:",
    ]
    for cname, cdef in classes.items():
        emit_class(lines, cname, cdef)

    lines += [
        "slots:",
        "  submitter_id:",
        "    identifier: true",
        "    required: true",
        "    range: string",
        "    description: >-",
        "      A unique submitter-assigned identifier for the entity within the scope of a",
        "      project.  Used as the primary business key for cross-references between nodes.",
        "",
        "enums:",
    ]

    for ename, edef in sorted(canonical_enums.items()):
        lines.append(f"  {ename}:")
        if edef["description"]:
            rendered = block_scalar(edef["description"], 4)
            lines.append(f"    description: {rendered}")
        lines.append("    permissible_values:")
        for v in edef["values"]:
            safe_v = yaml_str_value(str(v))
            lines.append(f"      {safe_v}: {{}}")
        lines.append("")

    out_path.write_text("\n".join(lines) + "\n")

    # Stats
    file_count = sum(1 for m in node_metadata.values() if m["has_file_ref"])
    wf_count = sum(1 for m in node_metadata.values() if m["has_wf_ref"])
    concrete = len(classes) - 3  # subtract Entity + 2 mixins
    deduped = len(raw_enums) - len(canonical_enums)
    dup_mixin = file_count * len(DATA_FILE_PROPS) + wf_count * len(WORKFLOW_PROPS)

    print(f"Wrote {out_path}")
    print(
        f"  {concrete} concrete classes  "
        f"({file_count} use DataFileMixin, {wf_count} use WorkflowMixin)"
    )
    print(
        f"  {len(canonical_enums)} enums  ({len(raw_enums)} raw → {deduped} deduplicated away)"
    )
    print(f"  ~{dup_mixin} attribute lines eliminated by mixins")


if __name__ == "__main__":
    repo = Path(__file__).parent.parent
    generate(
        kf_path=repo / "tests" / "schema" / "kf.json",
        out_path=repo / "tests" / "schema" / "kf_schema.yaml",
    )
