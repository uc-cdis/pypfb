# Portable Format for Biomedical Data

- [Portable Format for Biomedical Data](#portable-format-for-biomedical-data)
  - [What is PFB?](#what-is-pfb)
  - [Schema](#schema)
    - [Types](#types)
      - [Enum](#enum)
        - [Future implementation:](#future-implementation)
  - [Example](#example)

## What is PFB?

**PFB** is the serialization file format designed to store bio-medical data, as well as metadata. The format is built on top [**Avro**][1] to make it fast, extensible and interoperable between different systems.

## Schema

**PFB** file is consist of list of **Entities**, where **Entity** is a container type (it stores the actual data) in *object* field. The *id*, *name* and *relations* fields used for linking each entity to other entities in the PFB file. The *relations* consists of a destination *id* and *name*: so each entity is linked to some other entity (*note*, there is no backward links). The *id* and *relations* is only relevant for clinical data, so for **Metadata** entity it's `null`.

The only entity which is required in the PFB file is **Metadata**: it describes the data model linking and stores ontology references to make interoperability with other systems easier. For each node and fields multiple links to ontologies can exists. The ontology is user defined and it's possible to perform a schema evolution on the PFB file to harmonize all of the data to a single version of a given ontology.

The Avro IDL for **Metadata**:

```
record Metadata {
    array<Node> nodes;
    map<string> misc;
}
```

With `Node` and `Property` defined as:

```
record Node {
    string name;
    string ontology_reference;
    map<string> values;
    array<Link> links;
    array<Property> properties;
}

record Property {
    string name;
    string ontology_reference;
    map<string> values;
}
```

In both `Node` and `Property` the `name` is the name of the field in the data model, `ontology_reference` stores the name for ontology reference (any ontology, e.g. [NCIt][2], [caDSR][3]) and `values` stores any additional field regarding ontology: URL, ontology codes, synonyms etc. Same for `Property`.

In addition, inside *links* for `Node` it's important to specify the linkage for the nodes: destination and multiplicity.

The `Link` and `Multiplicity` enum is presented below:
```
record Link {
    Multiplicity multiplicity;
    string dst;
}

enum Multiplicity {
    ONE_TO_ONE,
    ONE_TO_MANY,
    MANY_TO_ONE,
    MANY_TO_MANY
}
```

### Types

#### Enum

Because Avro can't store anything except alphanumeric and `_` symbols, all enums are encoded in such a way, that all other symbols is being encoded with codepoint wrapped in single underscores. For example `bpm > 60` will be encoded in: `bpm_32__62__32_60`, so space ` ` is encoded as `_32_` and greater sign `>` into `_62_`. Same for Unicode characters: `ä` - `_228_`, `ü` - `_252_`. The Avro schema also doesn't allow for the first character to be a number. So we encode the first character in the way if the character happens to be a number.

## Example

The schema on the image below shows the simplest model with only **Demographic** data.

[![schema][4]][4]

The Avro IDL for this schema is located in [`sample.avdl`][16].

  [1]: https://avro.apache.org/docs/current/spec.html
  [2]: https://ncit.nci.nih.gov/ncitbrowser/pages/
  [3]: https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#/search
  [4]: ./schema.svg
  [16]: ./sample.avdl
