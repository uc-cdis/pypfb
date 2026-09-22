"""Utilities for converting LinkML schemas to Gen3 Data Dictionary format."""

import logging
import re
import inflect
from linkml_runtime import SchemaView

logger = logging.getLogger(__name__)

_inflect_engine = inflect.engine()

# LinkML type -> (Gen3 JSON Schema type, format).  Mirrors the
# "LinkML-to-Gen3 mapping rules" table in the LinkML in PFBs design doc.
TYPE_MAP = {
    "string": ("string", None),
    "integer": ("integer", None),
    "float": ("number", None),
    "double": ("number", None),
    "decimal": ("number", None),
    "boolean": ("boolean", None),
    "date": ("string", "date"),
    "datetime": ("string", "date-time"),
    "time": ("string", "time"),
    "uri": ("string", "uri"),
    "uriorcurie": ("string", "uri"),
}

# Fallback keyed on TypeDefinition.base, for LinkML builtins that are not named
# in the design doc's table (ncname, curie, objectidentifier, jsonpointer, ...).
# Reached only when a `typeof` chain bottoms out at a type TYPE_MAP lacks.
BASE_TYPE_MAP = {
    "str": ("string", None),
    "int": ("integer", None),
    "float": ("number", None),
    "Decimal": ("number", None),
    "Bool": ("boolean", None),
    "XSDDate": ("string", "date"),
    "XSDDateTime": ("string", "date-time"),
    "XSDTime": ("string", "time"),
    "URI": ("string", "uri"),
    "URIorCURIE": ("string", "uri"),
    "Curie": ("string", None),
    "NCName": ("string", None),
    "NodeIdentifier": ("string", None),
    "ElementIdentifier": ("string", None),
}

# Slot name patterns to Gen3 link labels
LINK_LABEL_MAP = {
    "associated_participant": "associated_with",
    "associated_person": "associated_with",
    "associated_visit": "associated_with",
    "member_of_research_study": "member_of",
    "member_of": "member_of",
    "part_of": "part_of",
    "derived_from": "derived_from",
    "source_participant": "derived_from",
    "source_specimen": "derived_from",
    "focus_specimen": "describes",
    "has_questionnaire_item": "describes",
    "originating_site": "performed_at",
    "consents": "performed_under",
}


# Gen3 treats all three file-ish categories alike: data_file, index_file and
# metadata_file nodes each carry _definitions.yaml#/data_file_properties
# (confirmed against every *_file node in tests/schema/kf.json).
FILE_CATEGORIES = ("data_file", "index_file", "metadata_file")

# `program` and `project` are Gen3's two hierarchy roots.  They carry their own
# identity (`dbgap_accession_number` / `code`) and declare `id`/`type` directly
# rather than inheriting _definitions.yaml#/ubiquitous_properties, so they must
# not have it injected (verified against tests/schema/kf.json, where these are
# the only two nodes lacking a ubiquitous_properties $ref).
ROOT_NODES = ("program", "project")


def to_snake_case(name: str) -> str:
    """Convert CamelCase to snake_case.

    Gen3 uses lowercase snake_case for node IDs, e.g ResearchStudy -> research_study
    """
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def pluralize(word: str) -> str:
    """Pluralize words using the inflect library.

    Used for `backref` in the `links` section
    """
    result = _inflect_engine.plural_noun(word)
    return result


def infer_link_label(slot_name: str) -> str:
    """Infer the relationship label from slot name patterns.

    See LINK_LABEL_MAP
    """
    if slot_name in LINK_LABEL_MAP:
        return LINK_LABEL_MAP[slot_name]

    # Pattern-based attempt if no mapping
    if "associated_" in slot_name or slot_name.endswith("_of"):
        return "associated_with"
    if slot_name.startswith("member_of"):
        return "member_of"
    if slot_name.startswith("part_of") or slot_name.endswith("_part_of"):
        return "part_of"
    if "derived" in slot_name or "source" in slot_name:
        return "derived_from"
    if slot_name.startswith("focus_") or slot_name.startswith("describes_"):
        return "describes"

    # fall back to "related_to" as a default
    return "related_to"


def generate_backref(source_class: str) -> str:
    """Generate a backref name for a link.

    Pluralized snake_case of the source class name
    e.g., Demography -> demographies, Specimen -> specimens
    """
    return pluralize(to_snake_case(source_class))


def get_annotation(class_def, key: str, default=None):
    """Return a string annotation value from a LinkML ClassDefinition."""
    ann = (class_def.annotations or {}).get(key)
    if ann is None or ann.value is None:
        return default
    return str(ann.value)


def guess_category(class_name: str) -> str:
    """Guess the Gen3 category for a class based on its name.

    Every node in data dictionary needs the category field
    BDCHM LinkML schema doesn't have this so infer it for now

    Example Gen3 categories:
    - administrative
    - biospecimen
    - clinical
    - data_file
    - index_file
    - notation
    - analysis
    """
    class_lower = class_name.lower()

    # Administrative
    if any(
        kw in class_lower
        for kw in ["study", "organization", "consent", "questionnaire"]
    ):
        return "administrative"

    # Data file
    # Note: ImagingStudy contains "study" which matches administrative first, which is what we want
    if any(kw in class_lower for kw in ["file", "document", "imaging"]):
        return "data_file"

    # Biospecimen
    if any(
        kw in class_lower
        for kw in ["specimen", "sample", "aliquot", "analyte", "biologic"]
    ):
        return "biospecimen"

    # Fallback to Clinical as default for now
    return "clinical"


def is_downloadable(class_name: str) -> bool:
    """Check if a node should be downloadable.

    Every node in data dictionary needs the downloadable field.
    For now assume that what we decide is a category: data_file is downloadable.
    """
    class_lower = class_name.lower()
    return any(kw in class_lower for kw in ["file", "document", "imaging"])


def resolve_type_range(range_name: str, sv):
    """Return ``(json_type, json_format)`` for a LinkML *type* range, or ``None``.

    Custom types declared with ``typeof`` are resolved to the nearest ancestor
    that TYPE_MAP knows, so a schema declaring::

        types:
          StudyURL:
            typeof: uriorcurie
            pattern: "^https://"

    maps ``StudyURL`` exactly like ``uriorcurie``.  ``SchemaView.induced_type()``
    does the work of resolving the ``typeof`` hierarchy; we walk it because a
    chain may be several levels deep before reaching a mappable type.

    Returns ``None`` when *range_name* is not a type at all (e.g. it names a
    class or enum), so callers can fall through to their own handling.
    """
    seen: set[str] = set()
    name = range_name
    tdef = None
    while name and name not in seen:
        if name in TYPE_MAP:
            return TYPE_MAP[name]
        seen.add(name)
        try:
            tdef = sv.induced_type(name)
        except Exception:
            return None
        if tdef is None:
            return None
        name = tdef.typeof

    # typeof chain exhausted without a TYPE_MAP hit - fall back to the
    # underlying Python/XSD base that linkml-runtime resolved.
    if tdef is not None and tdef.base in BASE_TYPE_MAP:
        return BASE_TYPE_MAP[tdef.base]
    return None


def slot_max_cardinality(slot) -> str:
    """Return ``"one"`` or ``"many"``: the most targets a single source may hold.

    ``exact_cardinality``/``maximum_cardinality`` refine ``multivalued`` when
    present, per the cardinality table in CLAUDE_MULTIPLICITY_GUIDANCE.md.
    """
    if not slot.multivalued:
        return "one"
    for bound in (slot.exact_cardinality, slot.maximum_cardinality):
        if bound is not None:
            return "one" if bound == 1 else "many"
    return "many"


def _has_single_slot_unique_key(class_def, slot_name: str) -> bool:
    """True if ``class_def`` declares a unique key consisting of just ``slot_name``.

    A uniqueness constraint on a relationship slot means no two instances of the
    source class may reference the same target, i.e. the inverse side of the
    relationship has a maximum cardinality of one.
    """
    for unique_key in (class_def.unique_keys or {}).values():
        if list(unique_key.unique_key_slots or []) == [slot_name]:
            return True
    return False


def inverse_max_cardinality(slot, sv, source_class: str) -> str:
    """Return ``"one"`` or ``"many"`` for the inverse side of a relationship.

    LinkML only requires one side of a relationship between two classes to be
    described, whereas Gen3 describes both ends, so the inverse cardinality is
    resolved from whichever signal the schema provides, most explicit first:

    1. an explicit ``inverse`` declaration, in either direction
    2. a reciprocal slot on the target class ranging back on the source class
    3. a single-slot ``unique_keys`` entry over this slot on the source class,
       which constrains each target to at most one source
    4. otherwise assume ``"many"``, which is Gen3's most common shape

    Only (1) and (2) actually describe both ends; (3) infers the inverse bound
    from a uniqueness constraint and (4) is a documented default.  No inspection
    of the source slot alone can distinguish these cases -- two slots identical
    in every attribute can legitimately have different inverse cardinalities.
    """
    target_class = slot.range

    # 1. An explicit inverse declaration wins.  Check the slot itself first,
    #    since class `attributes:` do not always appear in sv.all_slots().
    inverse_name = slot.inverse
    if not inverse_name:
        # SchemaView.inverse() raises if the name is not a known element, which
        # happens for class `attributes:` that never appear in all_slots().
        try:
            inverse_name = sv.inverse(slot.name)
        except Exception:
            inverse_name = None
    if inverse_name:
        inverse_slot = sv.induced_slot(inverse_name, target_class)
        if inverse_slot is not None:
            return slot_max_cardinality(inverse_slot)

    # 2. A reciprocal slot on the target class describes the other end.
    #    Self-referential relationships are skipped: the slot would match itself.
    if target_class != source_class and sv.get_class(target_class) is not None:
        for candidate in sv.class_induced_slots(target_class):
            if candidate.range == source_class:
                return slot_max_cardinality(candidate)

    # 3. Uniqueness on the source side bounds the inverse at one.
    source_def = sv.get_class(source_class)
    if source_def is not None and _has_single_slot_unique_key(source_def, slot.name):
        return "one"

    # 4. Documented default.
    return "many"


def make_link(slot, source_class, target_class, multiplicity, name, required):
    """Build a single Gen3 link definition for a class-valued slot.

    The link name comes from the source slot name (so schemas generated from
    Gen3 dictionaries, where the link name is explicit -- e.g.
    "submitted_aligned_reads_files" -- round-trip correctly), the target_type
    from the slot's range, and the required status from the slot.
    """
    link = {
        "name": name,
        "backref": generate_backref(source_class),
        "label": infer_link_label(slot.name),
        "target_type": to_snake_case(target_class),
        "multiplicity": multiplicity,
        "required": required,
    }
    if slot.description:
        link["description"] = slot.description
    return link


def iter_link_members(link):
    """Yield the concrete link definitions in *link*, flattening any subgroups.

    Gen3 represents a choice of link targets as ``{"subgroup": [...]}`` with no
    name of its own, so callers that need per-target names (to emit the matching
    node properties) must walk into it.
    """
    if "subgroup" in link:
        for member in link["subgroup"]:
            yield from iter_link_members(member)
    else:
        yield link


def build_property(slot, sv, source_class: str, unmapped):
    """Build a Gen3 property definition or link definition from a LinkML slot.

    LinkML attributes/slots can be simple properties (string, number, etc.)
    or references to other classes (relationships).

    Gen3 schema would then have simple properties as properties, and relationships as links.

    Returns:
        tuple: (property_definition, link_definition)
            - one of property_definition and link_definition will be None
            - if both are None, then it is an abstract class, return None, None
    """
    # figure out basic type
    rng = slot.range or "string"

    primitive = resolve_type_range(rng, sv) if not sv.get_class(rng) else None
    if primitive is not None:
        jtype, fmt = primitive
        spec = {"type": jtype}
        if fmt:
            spec["format"] = fmt
    elif sv.get_class(rng):
        # This is a relationship to another class.
        target_class_def = sv.get_class(rng)

        # An inlined class-valued slot holds its target by value rather than by
        # reference.  When the target is identified we normalize it the same way
        # as any reference - a separate Gen3 node plus a link - which is what a
        # graph-oriented intermediary needs.  An inlined target with no
        # identifier is an anonymous embedded structure that cannot be given a
        # stable PFB entity ID, so record it instead of silently mis-modelling it.
        if (slot.inlined or slot.inlined_as_list) and not any(
            s.identifier for s in sv.class_induced_slots(rng)
        ):
            unmapped.append(
                f"slot {slot.name} inlines unidentified class {rng} - "
                "anonymous embedded structures are not supported"
            )
        # Gen3 multiplicity reads "<sources per target>_to_<targets per source>",
        # i.e. "{inverse}_to_{source}" (see CLAUDE_MULTIPLICITY_GUIDANCE.md).
        multiplicity = "{}_to_{}".format(
            inverse_max_cardinality(slot, sv, source_class),
            slot_max_cardinality(slot),
        )

        if target_class_def.abstract:
            # Polymorphic relationship: an abstract range stands for a link to
            # any of its concrete descendants, so expand it into explicit Gen3
            # link targets rather than dropping the relationship.  More than one
            # target is expressed as a Gen3 link subgroup, per "Abstract class"
            # and "Polymorphic relationship" in the LinkML in PFBs design doc.
            targets = [
                d
                for d in sv.class_descendants(rng, reflexive=False)
                if not (sv.get_class(d).abstract or sv.get_class(d).mixin)
            ]
            if not targets:
                unmapped.append(
                    f"slot {slot.name} references abstract class {rng} with no "
                    "concrete descendants - link skipped"
                )
                return None, None

            if len(targets) == 1:
                return None, make_link(
                    slot,
                    source_class,
                    targets[0],
                    multiplicity,
                    name=slot.name,
                    required=bool(slot.required),
                )

            subgroup = [
                make_link(
                    slot,
                    source_class,
                    target,
                    multiplicity,
                    # Qualify by slot name: a class may have two polymorphic
                    # slots on the same abstract range (e.g. BDCHM's
                    # MeasurementObservation.focus and .associated_artifact),
                    # and Gen3 link names must be unique within a node.
                    name=f"{slot.name}_{to_snake_case(target)}",
                    # Gen3 marks the group required, not its members.
                    required=False,
                )
                for target in targets
            ]
            return None, {
                # The originating LinkML slot name.  Gen3 ignores `name` on a
                # subgroup (dictionaryutils recurses into `subgroup` and reads
                # names off the members), but the importer needs it: data
                # authored against the LinkML schema refers to this relationship
                # by the slot name, not by the per-target names below.
                "name": slot.name,
                # A single-valued polymorphic slot holds exactly one target
                # type, so the choice is exclusive; a multivalued one may mix.
                "exclusive": not bool(slot.multivalued),
                "required": bool(slot.required),
                "subgroup": subgroup,
            }

        return None, make_link(
            slot,
            source_class,
            rng,
            multiplicity,
            name=slot.name,
            required=bool(slot.required),
        )

    elif sv.get_enum(rng):
        # For enums, do not include type
        # see test_enums_dont_have_type in dictionaryutils
        enum_values = list(sv.get_enum(rng).permissible_values.keys())

        # Empty enums are invalid in Gen3, convert to strings
        if not enum_values:
            unmapped.append(
                f"slot {slot.name} has empty enum {rng} - converted to string"
            )
            spec = {"type": "string"}
        else:
            spec = {"enum": enum_values}
    else:
        unmapped.append(f"slot {slot.name} with range {rng}")
        spec = {"type": "string"}

    if slot.description:
        spec["description"] = slot.description
    if slot.multivalued:
        spec = {"type": "array", "items": spec}
    return spec, None


def linkml2gen3_dict(schema_path: str) -> dict:
    """Convert LinkML schema to Gen3 data dictionary format (in-memory).

    Returns a dictionary structure compatible with dictionaryutils that can be
    passed to Gen3 DataDictionary.

    Args:
        schema_path: Path to LinkML YAML schema file

    Returns:
        dict: Gen3 data dictionary structure with nodes/properties/links
    """
    sv = SchemaView(str(schema_path))
    # Constructs the conversion could not represent faithfully in Gen3.
    unmapped: list[str] = []
    # Constructs intentionally omitted (abstract/mixin classes are not nodes).
    by_design: list[str] = []

    # Build the Gen3 dictionary structure
    gen3_dict = {}

    for cn, c in sv.all_classes().items():
        if c.abstract or c.mixin:  # Skip abstract and mixin-only classes
            # Expected, not a shortcoming: abstract and mixin classes are never
            # emitted as nodes.  Their slots reach concrete descendants through
            # class_induced_slots(), and slots ranging on them are expanded into
            # links to those descendants.
            by_design.append(f"class {cn} is abstract or mixin")
            continue

        # init list of links
        node_links: list[dict] = []

        # Determine if this is a data file node for $ref selection.
        # Honour an explicit `gen3_category` annotation when present so that
        # schemas generated from Gen3 dictionaries keep their original
        # category values (e.g. "analysis", "metadata_file").
        ann_category = get_annotation(c, "gen3_category")
        category = ann_category if ann_category else guess_category(cn)
        # Whether the node inherits data_file_properties (all *_file categories)
        # is a different question from whether it needs the extra Gen3 data_file
        # scaffolding below (core_metadata_collections link, required data fields),
        # which only applies to the "data_file" category proper.
        has_file_properties = category in FILE_CATEGORIES
        is_file_node = category == "data_file"

        ann_submittable = get_annotation(c, "gen3_submittable")
        if ann_submittable is not None:
            submittable = str(ann_submittable).lower() not in ("false", "0", "no")
        else:
            submittable = True

        properties = {"type": {"enum": [to_snake_case(cn)]}}
        if to_snake_case(cn) not in ROOT_NODES:
            properties["$ref_ubiq"] = "_definitions.yaml#/ubiquitous_properties"
        if has_file_properties:
            properties["$ref_file"] = "_definitions.yaml#/data_file_properties"

        node = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "id": to_snake_case(cn),  # Gen3 uses snake_case for node IDs
            "title": cn,
            "type": "object",
            "category": category,
            "program": "*",
            "project": "*",
            "submittable": submittable,
            "downloadable": is_downloadable(cn),
            "systemProperties": [
                "id",
                "project_id",
                "state",
                "created_datetime",
                "updated_datetime",
            ],
            "validators": None,
            # Every node carries the ubiquitous system properties (id, type,
            # submitter_id, project_id, state, created/updated_datetime); file
            # nodes additionally pull in the data-file properties.  dictionaryutils
            # resolves *any* key beginning with "$ref", so distinct suffixes let a
            # node reference more than one definition - the same trick Gen3
            # dictionaries use once serialised to JSON, where duplicate "$ref" keys
            # are impossible (see "$ref_ubiq"/"$ref_file" in tests/schema/kf.json).
            "properties": properties,
            "required": ["type", "submitter_id"],
        }

        # class_induced_slots() returns the fully resolved SlotDefinitions for
        # the class, already incorporating inheritance, mixins, attributes,
        # slot_usage refinements and applicable defaults - so pypfb never walks
        # the inheritance tree itself.
        identifier_slot = None
        for slot in sv.class_induced_slots(c.name):
            if slot.identifier:
                identifier_slot = slot.name
            pdef, link = build_property(slot, sv, cn, unmapped)
            if pdef is not None:
                node["properties"][slot.name] = pdef
            if link is not None:
                node_links.append(link)
            if slot.required:
                # Skip if both are None (unmappable abstract class)
                if pdef is None and link is None:
                    continue
                if link is not None and "subgroup" in link:
                    # Gen3 marks the subgroup itself required and leaves its
                    # members out of the node's `required` list, since only one
                    # of an exclusive choice can be populated.
                    continue
                # For links the property name is the link name, not the slot name
                prop_name = link["name"] if link else slot.name
                if prop_name not in node["required"]:
                    node["required"].append(prop_name)

        # core_metadata_collection is a Gen3 Schema construct, not a LinkML one, so
        # it is deliberately NOT synthesized here.  If a LinkML schema declares a
        # CoreMetadataCollection class and a slot referencing it, the normal
        # class-reference path in build_property() emits the link like any other,
        # exactly as the schema defines it.  Callers who want Gen3's
        # core_metadata_collection scaffolding should use a Gen3 dictionary.
        if is_file_node:
            # add data_type, data_format, data_category, object_id for file nodes
            # see test_required_data_fields in dictionaryutils
            required_file_fields = {
                "data_type": {
                    "type": "string",
                    "description": "The type of data contained in the file.",
                },
                "data_format": {
                    "type": "string",
                    "description": "The format of the data file.",
                },
                "data_category": {
                    "type": "string",
                    "description": "Broad categorization of the contents of the data file.",
                },
                "object_id": {
                    "type": "string",
                    "description": "The GUID of the object in the index service.",
                },
            }
            for field, defn in required_file_fields.items():
                if field not in node["properties"]:
                    node["properties"][field] = defn

            for field in ["data_type", "data_format", "data_category"]:
                if field not in node["required"]:
                    node["required"].append(field)

        # Ensure no orphan nodes
        # see test_simulate_data in dictionaryutils
        has_required_link = any(link.get("required", False) for link in node_links)

        if not node_links:
            # if no links add a required link to project
            # Probably indicates top level class like ResearchStudy, Person, etc that don't have parent relationships in the LinkML
            node_links.append(
                {
                    "name": "projects",
                    "backref": pluralize(to_snake_case(cn)),
                    "label": "member_of",
                    "target_type": "project",
                    "multiplicity": "many_to_one",
                    "required": True,
                }
            )
            node["properties"]["projects"] = {
                "$ref": "_definitions.yaml#/to_one_project",
                "description": "Link to the project.",
            }
            if "projects" not in node["required"]:
                node["required"].append("projects")
        elif not has_required_link:
            # If class has links but none are required, make the first link required for now
            # TODO: evaluate what to do in these cases
            node_links[0]["required"] = True
            if "subgroup" not in node_links[0]:
                link_name = node_links[0]["name"]
                if link_name not in node["required"]:
                    node["required"].append(link_name)

        node["links"] = node_links

        # Add link properties to node properties
        # Gen3 validation requires links to be both in the links and properties
        # with a reference multiplicity e.g. $ref: "_definitions.yaml#/to_many"`
        # see test_required_fields_in_links in dictionaryutils
        for link in node_links:
            for member in iter_link_members(link):
                if member["name"] in node["properties"]:
                    continue

                if member["multiplicity"] in ("many_to_one", "one_to_one"):
                    ref = "_definitions.yaml#/to_one"
                else:
                    ref = "_definitions.yaml#/to_many"

                node["properties"][member["name"]] = {
                    "$ref": ref,
                    "description": member.get(
                        "description", f"Link to {member['target_type']}."
                    ),
                }

        # uniqueKeys required.  A LinkML identifier slot uniquely identifies an
        # instance of its class, so it becomes a unique key on the Gen3 node in
        # addition to Gen3's own system keys (see "Identifier slot" in the
        # LinkML in PFBs design doc).  It is also what linkml.py uses as the
        # source for each PFB entity ID.
        unique_keys = [["id"], ["project_id", "submitter_id"]]
        if identifier_slot and [identifier_slot] not in unique_keys:
            unique_keys.insert(0, [identifier_slot])
        node["uniqueKeys"] = unique_keys

        gen3_dict[to_snake_case(cn)] = node

    # Surface constructs that could not be represented faithfully: silently
    # degrading parts of a schema leaves no trace that the resulting PFB is an
    # incomplete view of the source model.
    if unmapped:
        logger.warning(
            "%d LinkML construct(s) were not mapped faithfully to Gen3:\n  %s",
            len(unmapped),
            "\n  ".join(unmapped),
        )
    if by_design:
        logger.debug(
            "%d LinkML construct(s) intentionally omitted:\n  %s",
            len(by_design),
            "\n  ".join(by_design),
        )

    return gen3_dict
