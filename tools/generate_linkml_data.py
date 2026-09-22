#!/usr/bin/env python3
"""Generate deterministic synthetic data for a LinkML schema.

Why this exists
---------------
LinkML ships no instance-data generator -- every generator in
``linkml.generators`` transforms a *schema* into another schema, code, or
documentation.  Gen3's ``data-simulator`` does generate graph data from a Gen3
data dictionary, but it fetches the dictionary from an S3 URL and offers no
random seed, so it cannot be pointed at a local LinkML schema and cannot produce
byte-identical output between runs.

What this generates against
---------------------------
The **raw LinkML schema**, read through ``SchemaView`` -- deliberately *not*
pypfb's compiled Gen3 dictionary.  Generating from the compiled dictionary would
be circular: the data would inherit whatever the compiler decided, so it could
never expose a compiler bug.  Reading the LinkML directly means the data is an
independent input, and feeding it to ``pfb from linkml`` genuinely exercises the
LinkML-to-Gen3 conversion.

Everything therefore comes from LinkML concepts only:

* concrete classes (not abstract, not mixin)      -> one data file each
* ``SchemaView.class_induced_slots()``            -> the effective fields, with
  inheritance, mixins, attributes and slot_usage already resolved
* ``SchemaView.induced_type()``                   -> a custom type's base type
* ``slot.identifier``                             -> the record's identity
* ``slot.range``                                  -> primitive, enum, or class
* ``multivalued`` / ``required`` / cardinality    -> shape of each value
* ``SchemaView.is_inlined()``                     -> whether a class-valued slot
  holds a reference (the target's identifier, as a string) or the target
  object itself -- as a list, or as a dict keyed by identifier

Determinism
-----------
Every value is drawn from a ``random.Random`` seeded with a BLAKE2b hash of
``(seed, class, slot, instance index)``.  Python's built-in ``hash()`` is salted
per process and is deliberately not used.  Because each value's seed is derived
from its own coordinates rather than from a single shared stream, output is
stable under reordering: adding a class, or changing ``--count``, does not shift
the values generated for unrelated slots.

Usage
-----
    python tools/generate_linkml_data.py -s <schema.yaml> -o <dir> [--count N]

    # feed the result straight into pypfb
    pfb from -o out.avro linkml -s <schema.yaml> --program DEV --project test <dir>
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sys
from pathlib import Path

from linkml_runtime import SchemaView

# Reused from pypfb, but neither is part of the LinkML-to-Gen3 conversion:
# `to_snake_case` is a string helper (so generated filenames match the node names
# the importer derives from them), and BASE_TYPE_MAP is a table over LinkML's own
# builtin type bases.  The conversion itself -- linkml2gen3_dict, build_property --
# is deliberately not used; see "What this generates against" above.
from pfb.importers.linkml_utils import BASE_TYPE_MAP, to_snake_case

# Written by the importer rather than by the data: see
# linkml.py::_convert_json_record.
IMPORTER_OWNED_FIELDS = frozenset(
    {"project_id", "created_datetime", "updated_datetime"}
)

# LinkML TypeDefinition.base -> a value-generator kind.  Derived from
# linkml_utils.BASE_TYPE_MAP rather than hand-maintained here: both tables are
# keyed on LinkML's own builtin bases, and keeping two copies in sync has
# already proved error-prone.  A `(json_type, format)` pair collapses to a
# single kind because the format, when present, is the more specific of the two
# ("string"/"date" -> date, "string"/None -> string, "number"/None -> number).
BASE_KINDS = {
    base: fmt or json_type for base, (json_type, fmt) in BASE_TYPE_MAP.items()
}

# Nonsense-but-pronounceable filler, so generated strings are visibly synthetic
# and stable without pulling in a dependency such as Faker.
_ONSETS = (
    "b",
    "br",
    "c",
    "cl",
    "d",
    "dr",
    "f",
    "fl",
    "g",
    "gr",
    "h",
    "j",
    "k",
    "l",
    "m",
    "n",
    "p",
    "pl",
    "qu",
    "r",
    "s",
    "sh",
    "sk",
    "sl",
    "sp",
    "st",
    "t",
    "tr",
    "v",
    "w",
    "z",
)
_NUCLEI = ("a", "e", "i", "o", "u", "ai", "ea", "ee", "ie", "oa", "oo", "ou")
_CODAS = (
    "",
    "b",
    "ck",
    "d",
    "ft",
    "g",
    "l",
    "lt",
    "m",
    "n",
    "nd",
    "ng",
    "nt",
    "p",
    "r",
    "rt",
    "s",
    "sh",
    "st",
    "t",
    "x",
)


def stable_rng(seed: int, *parts: object) -> random.Random:
    """Return a ``random.Random`` seeded from *seed* and *parts*.

    A stable hash is required: ``hash()`` on a ``str`` is salted per process, so
    using it would make output differ between runs.
    """
    key = "\x1f".join([str(seed)] + [str(part) for part in parts]).encode("utf-8")
    digest = hashlib.blake2b(key, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))


def _syllable(rng: random.Random) -> str:
    return rng.choice(_ONSETS) + rng.choice(_NUCLEI) + rng.choice(_CODAS)


def _word(rng: random.Random, syllables: int = 2) -> str:
    return "".join(_syllable(rng) for _ in range(syllables))


def _phrase(rng: random.Random, words: int = 2) -> str:
    return "_".join(_word(rng, rng.randint(1, 3)) for _ in range(words))


def concrete_classes(sv: SchemaView) -> list[str]:
    """Class names that may appear in data: neither abstract nor mixin."""
    return [
        name
        for name, cls in sv.all_classes().items()
        if not (cls.abstract or cls.mixin)
    ]


def identifier_slot(sv: SchemaView, class_name: str) -> str | None:
    """Return the name of *class_name*'s identifier slot, if it declares one."""
    for slot in sv.class_induced_slots(class_name):
        if slot.identifier:
            return slot.name
    return None


def type_kind(sv: SchemaView, range_name: str) -> str | None:
    """Return a value-generator kind for a LinkML *type* range, else ``None``.

    ``None`` means *range_name* is not a type at all -- it names a class or an
    enum -- so the caller handles it.
    """
    try:
        type_def = sv.induced_type(range_name)
    except Exception:
        return None
    if type_def is None:
        return None
    if type_def.base in BASE_KINDS:
        return BASE_KINDS[type_def.base]
    # An unrecognised base is still a scalar; a string is always safe.
    return "string"


def scalar_value(kind: str, rng: random.Random):
    """Generate one value of the given kind."""
    if kind == "integer":
        return rng.randint(1, 20000)
    if kind == "number":
        return round(rng.uniform(0.5, 500.0), 3)
    if kind == "boolean":
        return rng.choice([True, False])
    if kind == "date":
        return (
            f"{rng.randint(1950, 2024)}-{rng.randint(1, 12):02d}-"
            f"{rng.randint(1, 28):02d}"
        )
    if kind == "date-time":
        return (
            f"{rng.randint(1950, 2024)}-{rng.randint(1, 12):02d}-"
            f"{rng.randint(1, 28):02d}T{rng.randint(0, 23):02d}:"
            f"{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}Z"
        )
    if kind == "time":
        return (
            f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:"
            f"{rng.randint(0, 59):02d}"
        )
    if kind == "uri":
        return f"https://example.org/{_word(rng)}/{_word(rng)}"
    return _phrase(rng)


def value_count(slot, rng: random.Random) -> int:
    """How many values to emit for *slot*, honouring its cardinality."""
    if not slot.multivalued:
        return 1
    if slot.exact_cardinality is not None:
        return max(1, slot.exact_cardinality)
    upper = slot.maximum_cardinality if slot.maximum_cardinality is not None else 2
    lower = slot.minimum_cardinality if slot.minimum_cardinality is not None else 1
    lower = max(1, lower)
    return rng.randint(lower, max(lower, upper))


def link_targets(sv: SchemaView, range_name: str, concrete: set[str]) -> list[str]:
    """Concrete classes a slot ranging on *range_name* may point at.

    A concrete range is its own only target.  An abstract range stands for any of
    its concrete descendants, matching how the compiler expands it.
    """
    target = sv.get_class(range_name)
    if target is None:
        return []
    if not (target.abstract or target.mixin):
        return [range_name]
    return [
        name
        for name in sv.class_descendants(range_name, reflexive=False)
        if name in concrete
    ]


def serialise_references(sv: SchemaView, slot, picks, id_slots, built):
    """Serialise the ``(target class, target id)`` *picks* for *slot*.

    This follows LinkML's own rules for a class-valued slot:

    * not inlined          -> the identifier string (a list if multivalued)
    * inlined, single      -> the target object
    * inlined, multivalued -> a list of objects if ``inlined_as_list`` or the
      range has no identifier, otherwise a dict keyed by identifier whose
      values are the objects without that key

    An inlined object is a copy of the target's generated record, so it carries
    the target's required slots.  A target whose records are not built yet --
    a self-reference, or a cycle -- falls back to an object holding only its
    identifier.
    """
    if not sv.is_inlined(slot):
        refs = [target_id for _, target_id in picks]
        return refs if slot.multivalued else refs[0]

    objects = []
    for target, target_id in picks:
        id_slot = id_slots.get(target) or "submitter_id"
        obj = built.get(target, {}).get(target_id) or {id_slot: target_id}
        objects.append((id_slot, target_id, dict(obj)))

    if not slot.multivalued:
        return objects[0][2]
    if slot.inlined_as_list or sv.get_identifier_slot(slot.range) is None:
        return [obj for _, _, obj in objects]
    return {
        target_id: {k: v for k, v in obj.items() if k != id_slot}
        for id_slot, target_id, obj in objects
    }


def class_order(sv: SchemaView, concrete: list[str]) -> list[str]:
    """Order classes so inlined targets are generated before their sources.

    Only inlined slots constrain the order: an inlined value is a copy of the
    target's record, which must exist first.  A plain reference needs only the
    target's ID, and every ID is known before any record is built, so following
    those edges too would just let an optional reference -- typically through a
    polymorphic slot, whose abstract range reaches many classes -- place a class
    ahead of the target of one of its *required* references.

    Depth-first with cycle tolerance: LinkML permits cycles and the compiler makes
    no effort to break them, so a class already being visited is simply skipped.
    """
    concrete_set = set(concrete)
    order: list[str] = []
    done: set[str] = set()
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in done or name in visiting:
            return
        visiting.add(name)
        for slot in sv.class_induced_slots(name):
            if slot.range and sv.is_inlined(slot):
                for target in link_targets(sv, slot.range, concrete_set):
                    visit(target)
        visiting.discard(name)
        done.add(name)
        order.append(name)

    for name in sorted(concrete):
        visit(name)
    return order


def generate_records(sv: SchemaView, count: int, seed: int, required_only=False):
    """Build ``{node_id: [record, ...]}`` for every concrete class."""
    concrete = concrete_classes(sv)
    concrete_set = set(concrete)
    id_slots = {name: identifier_slot(sv, name) for name in concrete}
    # IDs are fixed by class and index, so all of them are known up front and
    # any record can reference any other, whatever order they are built in.
    ids: dict[str, list[str]] = {
        name: [f"{to_snake_case(name)}_{index:04d}" for index in range(count)]
        for name in concrete
    }
    # class name -> {entity id: record}, for copying into inlined slots.
    built: dict[str, dict[str, dict]] = {}
    records: dict[str, list[dict]] = {}

    for class_name in class_order(sv, concrete):
        node_id = to_snake_case(class_name)
        id_slot = id_slots.get(class_name) or "submitter_id"
        entity_ids = ids[class_name]
        rows = []

        for index, entity_id in enumerate(entity_ids):
            record = {id_slot: entity_id}

            for slot in sv.class_induced_slots(class_name):
                name = slot.name
                if name == id_slot or name in IMPORTER_OWNED_FIELDS:
                    continue
                if required_only and not slot.required:
                    continue

                rng = stable_rng(seed, class_name, name, index)
                range_name = slot.range or "string"
                enum_def = sv.get_enum(range_name)

                if sv.get_class(range_name) is not None:
                    # Class-valued slot: references to other records, under
                    # the LinkML slot name.  A polymorphic slot may mix
                    # concrete targets, so draw from all of their instances.
                    candidates = [
                        (target, target_id)
                        for target in sorted(link_targets(sv, range_name, concrete_set))
                        for target_id in ids[target]
                    ]
                    # A record does not reference itself -- unless the slot is
                    # required and it is the only instance there is.
                    pool = [
                        pick for pick in candidates if pick != (class_name, entity_id)
                    ] or (candidates if slot.required else [])
                    if not pool:
                        # No concrete target, or an optional self-reference
                        # with no other instance: leave the slot unset.
                        continue
                    # Distinct targets: a record lists each link once.
                    picks = rng.sample(pool, min(value_count(slot, rng), len(pool)))
                    record[name] = serialise_references(
                        sv, slot, picks, id_slots, built
                    )
                elif enum_def is not None:
                    values = list(enum_def.permissible_values or {})
                    if values:

                        def draw(r, _values=values):
                            return r.choice(_values)

                    else:
                        # An empty enum carries no permissible values, so the
                        # compiler degrades it to a plain string.  Do the same --
                        # and still honour `multivalued`, since the degraded
                        # property is still wrapped in an array.
                        def draw(r):
                            return _phrase(r)

                    if slot.multivalued:
                        record[name] = [
                            draw(rng) for _ in range(value_count(slot, rng))
                        ]
                    else:
                        record[name] = draw(rng)
                else:
                    kind = type_kind(sv, range_name) or "string"
                    if slot.multivalued:
                        record[name] = [
                            scalar_value(kind, rng)
                            for _ in range(value_count(slot, rng))
                        ]
                    else:
                        record[name] = scalar_value(kind, rng)

            rows.append(record)

        records[node_id] = rows
        built[class_name] = dict(zip(entity_ids, rows))

    return records


def reference_fields(sv: SchemaView) -> dict[str, set[str]]:
    """Map each concrete class's node name to its class-valued slot names."""
    return {
        to_snake_case(name): {
            slot.name
            for slot in sv.class_induced_slots(name)
            if slot.range and sv.get_class(slot.range) is not None
        }
        for name in concrete_classes(sv)
    }


def write_json(records: dict, out_dir: Path) -> list[Path]:
    """Write one ``<node>.json`` per class, each a JSON list of objects."""
    written = []
    for node_id, rows in sorted(records.items()):
        path = out_dir / f"{node_id}.json"
        with path.open("w") as handle:
            json.dump(rows, handle, indent=2, sort_keys=True)
            handle.write("\n")
        written.append(path)
    return written


def write_tsv(
    records: dict, out_dir: Path, references: dict[str, set[str]]
) -> list[Path]:
    """Write one ``<node>.tsv`` per class.

    Reference and multivalued fields are omitted: the importer's TSV path reads
    every cell as a string and only builds a relation from a ``<link>.<field>``
    column, so a bare reference column would silently produce unlinked records.
    *references* names each node's reference fields, which -- now written as
    plain ID strings -- cannot be told apart from scalars by value alone.
    """
    written = []
    for node_id, rows in sorted(records.items()):
        skip = references.get(node_id, set())
        flat = [
            {
                k: v
                for k, v in row.items()
                if k not in skip and not isinstance(v, (dict, list))
            }
            for row in rows
        ]
        columns = sorted({key for row in flat for key in row})
        path = out_dir / f"{node_id}.tsv"
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, delimiter="\t")
            writer.writeheader()
            writer.writerows(flat)
        written.append(path)
    return written


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic data for a LinkML schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "example:\n"
            "  python tools/generate_linkml_data.py -s tests/schema/kf_schema.yaml "
            "-o /tmp/kf-data --count 3\n"
            "  pfb from -o out.avro linkml -s tests/schema/kf_schema.yaml "
            "--program DEV --project test /tmp/kf-data\n"
        ),
    )
    parser.add_argument("-s", "--schema", required=True, help="LinkML schema (YAML)")
    parser.add_argument("-o", "--out", required=True, help="output directory")
    parser.add_argument(
        "-n", "--count", type=int, default=1, help="records per class (default: 1)"
    )
    parser.add_argument("--seed", type=int, default=0, help="random seed (default: 0)")
    parser.add_argument(
        "--format", choices=("json", "tsv"), default="json", help="output format"
    )
    parser.add_argument(
        "--required-only",
        action="store_true",
        help="emit only required slots",
    )
    args = parser.parse_args(argv)

    if args.count < 1:
        parser.error("--count must be at least 1")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    sv = SchemaView(args.schema)
    records = generate_records(
        sv, count=args.count, seed=args.seed, required_only=args.required_only
    )

    if args.format == "json":
        written = write_json(records, out_dir)
    else:
        written = write_tsv(records, out_dir, reference_fields(sv))

    total = sum(len(rows) for rows in records.values())
    print(
        f"Wrote {total} record(s) across {len(written)} {args.format} file(s) "
        f"to {out_dir}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
