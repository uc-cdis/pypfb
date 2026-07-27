"""Utilities for converting LinkML schemas to Gen3 Data Dictionary format."""

from pathlib import Path
import re
import inflect
from linkml_runtime import SchemaView

_inflect_engine = inflect.engine()

# LinkML to JSON Schema type mapping
TYPE_MAP = {
    "string": ("string", None),
    "integer": ("integer", None),
    "float": ("number", None),
    "decimal": ("number", "float"),
    "boolean": ("boolean", None),
    "datetime": ("string", "date-time"),
    "date": ("string", "date"),
    "uriorcurie": ("string", "uri"),
    "uri": ("string", "uri"),
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


def generate_backref(source_class: str, slot_name: str, target_class: str) -> str:
    """Generate a backref name for a link.

    Pluralized snake_case of the source class name
    e.g., Demography -> demographies, Specimen -> specimens
    """
    source_lower = to_snake_case(source_class)
    return pluralize(source_lower)


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
    link = None

    if rng in TYPE_MAP:
        jtype, fmt = TYPE_MAP[rng]
        spec = {"type": jtype}
        if fmt:
            spec["format"] = fmt
    elif sv.get_class(rng):
        # This is a relationship to another class
        target_class_def = sv.get_class(rng)

        # Skip links to abstract classes
        if target_class_def.abstract:
            unmapped.append(
                f"slot {slot.name} references abstract class {rng} - link skipped"
            )
            # Return no property, no link
            return None, None

        target_class = rng
        multiplicity = "many_to_many" if slot.multivalued else "many_to_one"

        link = {
            "name": pluralize(to_snake_case(target_class)),
            "backref": generate_backref(source_class, slot.name, target_class),
            "label": infer_link_label(slot.name),
            "target_type": to_snake_case(target_class),
            "multiplicity": multiplicity,
            "required": bool(slot.required),
        }

        # Return None for property, return the link
        if slot.description:
            link["description"] = slot.description

        return None, link

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
    unmapped: list[str] = []

    # Build the Gen3 dictionary structure
    gen3_dict = {}

    for cn, c in sv.all_classes().items():
        if c.abstract:  # Skip abstract classes
            unmapped.append(f"class {cn} is abstract")
            continue

        # init list of links
        node_links: list[dict] = []

        # Determine if this is a data file node for $ref selection
        category = guess_category(cn)
        is_file_node = category == "data_file"

        node = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "id": to_snake_case(cn),  # Gen3 uses snake_case for node IDs
            "title": cn,
            "type": "object",
            "category": category,
            "program": "*",
            "project": "*",
            "submittable": True,
            "downloadable": is_downloadable(cn),
            "systemProperties": [
                "id",
                "project_id",
                "state",
                "created_datetime",
                "updated_datetime",
            ],
            "validators": None,
            "properties": {
                "$ref": (
                    "_definitions.yaml#/data_file_properties"
                    if is_file_node
                    else "_definitions.yaml#/ubiquitous_properties"
                ),
                "type": {"enum": [to_snake_case(cn)]},
            },
            "required": ["type", "submitter_id"],
        }

        for s in sv.class_slots(c.name):
            slot = sv.induced_slot(s, c.name)
            pdef, link = build_property(slot, sv, cn, unmapped)
            if pdef is not None:
                node["properties"][slot.name] = pdef
            if link is not None:
                node_links.append(link)
            if slot.required:
                # Skip if both are None (abstract class)
                if pdef is None and link is None:
                    continue
                # For links the property name is the link name, not the slot name
                prop_name = link["name"] if link else slot.name
                if prop_name not in node["required"]:
                    node["required"].append(prop_name)

        # file nodes must link to core_metadata_collection, and have some 'required' fields
        if is_file_node:
            node_links.append(
                {
                    "name": "core_metadata_collections",
                    "backref": to_snake_case(cn) + "_files",
                    "label": "data_from",
                    "target_type": "core_metadata_collection",
                    "multiplicity": "many_to_one",
                    "required": True,
                }
            )
            node["properties"]["core_metadata_collections"] = {
                "$ref": "_definitions.yaml#/to_one",
                "description": "Link to the core_metadata_collection.",
            }

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
                "description": f"Link to the project.",
            }
            if "projects" not in node["required"]:
                node["required"].append("projects")
        elif not has_required_link:
            # If class has links but none are required, make the first link required for now
            # TODO: evaluate what to do in these cases
            node_links[0]["required"] = True
            link_name = node_links[0]["name"]
            if link_name not in node["required"]:
                node["required"].append(link_name)

        node["links"] = node_links

        # Add link properties to node properties
        # Gen3 validation requires links to be both in the links and properties
        # with a reference multiplicity e.g. $ref: "_definitions.yaml#/to_many"`
        # see test_required_fields_in_links in dictionaryutils
        for link in node_links:
            if link["name"] in node["properties"]:
                continue

            if link["multiplicity"] in ("many_to_one", "one_to_one"):
                ref = "_definitions.yaml#/to_one"
            else:
                ref = "_definitions.yaml#/to_many"

            node["properties"][link["name"]] = {
                "$ref": ref,
                "description": link.get(
                    "description", f"Link to {link['target_type']}."
                ),
            }

        # uniqueKeys required
        node["uniqueKeys"] = [["id"], ["project_id", "submitter_id"]]

        gen3_dict[to_snake_case(cn)] = node

    return gen3_dict
