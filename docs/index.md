---

layout: home
title: Portable Format for Biomedical Data
---

- [Overview](#overview)
- [Introduction](#introduction)
  - [Vanilla Avro vs PFB](#vanilla-avro-vs-pfb)
- [PFB Principles](#principles)
  - [Schema](#schema)
    - [Node](#node)
    - [Property](#property)
    - [Link](#link)
    - [Types](#types)
- [Examples](#example)


# Overview

## Contact Information

Contact: Michael Lukowski (University of Chicago)

Contact Email: <mlukowski@uchicago.edu>

## License information 

License: Apache-2.0

License URL: https://github.com/uc-cdis/pypfb/blob/master/LICENSE

## Requirements

Fields in the specification of PFB may be used with a term **MUST**, **REQUIRED**, or **SHALL**. These correspond to the following in [RFC2119](https://www.ietf.org/rfc/rfc2119.txt) 

<br>

___ 

<br>

# Introduction

**PFB** is a serialization file format designed to store bio-medical data and metadata. The format is built on top [**Avro**][1] to make it fast, extensible and interoperable between different systems.

### What is an Avro File?

[AVRO](https://avro.apache.org/) is a data serialization system that takes a schema and then serializes the data as it is added to the file. This ensures that the data in the file that you are using will conform to the schema that the file was created from. Using this for PFB means that when someone has a PFB file they have the entire schema for data they will be working with. That means that only a single file is required to fully describe an entire biomedical data commons.

### What is a PFB File?

A PFB file is special kind of Avro file, suitable for capturing and reconstructing biomedical relational data.

A PFB file is an Avro file with a particular Avro schema that represents a relational database. We call this schema the [PFB Schema](#schema).

The data in a PFB file contains a list of JSON objects called PFB Entity objects. There are 2 types of PFB Entities. One (Metadata) captures information about the relational database and the other (Table Row) captures a row of data from a particular table in the database.

The data records in a PFB file are produced by transforming the original data from a relational database into PFB Entity objects. Each PFB Entity object conforms to its Avro schema.

### Vanilla Avro vs PFB

Let's say a client receives an Avro file. It reads in the Avro data. Now a client has the Avro schema and all of the data that conforms to that schema in a big JSON blob. It can do what it wants. Maybe it wants to construct some data input forms. It has everything it needs to do this since the schema has all of the entities, attributes, and types for those attributes defined.

Now what happens if the client wants to reconstruct a relational database from the data? How does it know what tables to create, and what the relationships are between those tables? Which relationships are required vs not? This is one of the problems PFB addresses.

<br>

 ___

<br>

# Principles

## Schema

The **Schema** is the translated JSON or YAML graph data model of the data commons. This translation **MUST** conform to the AVRO schema [specification](https://avro.apache.org/docs/1.11.1/specification/)

**PFB** file is consist of list of **Entities**, where **Entity** is a container type (it stores the actual data) in *object* field. The *id*, *name* and *relations* fields used for linking each entity to other entities in the PFB file. The *relations* consists of a destination *id* and *name*: so each entity is linked to some other entity (*note*, there is no backward links). The *id* and *relations* is only relevant for clinical data, so for **Metadata** entity it's `null`.

The only entity which is **MUST** be included in the PFB file is **Metadata**: this describes the data model linking and stores ontology references to make interoperability with other systems easier. For each node and fields multiple links to ontologies can exists. The ontology is user defined and it's possible to perform a schema evolution on the PFB file to harmonize all of the data to a single version of a given ontology.

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

Below are tables of the properties of Node and Property. All properties are **REQUIRED** unless stated otherwise.

### Node

<table>
    <tr>
        <th>Node Field</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    <tr>
        <th>Name</th>
        <th>string</th>
        <th>Name of the node (e.g. Demographic)</th>
    </tr>
    <tr>
        <th>Ontology Reference</th>
        <th>string</th>
        <th>Reference to external ontology. This field is allowed to be empty</th>
    </tr>
    <tr>
        <th>Values</th>
        <th>Map / Dictionary < string:string > </th>
        <th>Information about external ontology. Can be empty if no external ontology is referenced</th>
    </tr>
    <tr>
        <th>Links</th>
        <th>Array < Link ></th>
        <th>Links from node to node. A single node can link to multiple nodes</th>
    </tr>
    <tr>
        <th>Properties</th>
        <th>Array < Property ></th>
        <th>Information about the properties of the nodes. Optional if no additional information about terms inside Node is required</th>
    </tr>
</table>

### Property

<table>
    <tr>
        <th>Property Field</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    <tr>
        <th>Name</th>
        <th>string</th>
        <th>Name of the Property (e.g. Age)</th>
    </tr>
    <tr>
        <th>Ontology Reference</th>
        <th>string</th>
        <th>Reference to external ontology for the defined Property. This field is allowed to be empty</th>
    </tr>
    <tr>
        <th>Values</th>
        <th>Map / Dictionary < string:string > </th>
        <th>Information about external ontology (e.g. ontology source, cde, version). Can be empty if no external ontology is referenced</th>
    </tr>
</table>


In addition, inside *links* for `Node` it's important to specify the linkage for the nodes: destination and multiplicity.

The `Link` and `Multiplicity` enum is presented below:
```
record Link {
    Multiplicity multiplicity;
    string dst;
    string name;
}

enum Multiplicity {
    ONE_TO_ONE,
    ONE_TO_MANY,
    MANY_TO_ONE,
    MANY_TO_MANY
}
```

Below is a table for `Link`, all fields are **REQUIRED** unless specified.

### Link

<table>
    <tr>
        <th>Link Field</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    <tr>
        <th>Multiplicity</th>
        <th>enum Multiplicity</th>
        <th>Multipliction of link for node (e.g one to one)</th>
    </tr>
    <tr>
        <th>DST</th>
        <th>string</th>
        <th>Destination that links current node to next node.</th>
    </tr>
    <tr>
        <th>Name</th>
        <th>string</th>
        <th>Human readable destination of node this field is optional</th>
    </tr>
</table>

### Types

#### Enum

Because Avro can't store anything except alphanumeric and `_` symbols (<https://avro.apache.org/docs/1.9.1/spec.html#Enums>) , all enums are encoded in such a way, that all other symbols is being encoded with codepoint wrapped in single underscores. For example `bpm > 60` will be encoded in: `bpm_32__62__32_60`, so space ` ` is encoded as `_32_` and greater sign `>` into `_62_`. Same for Unicode characters: `ä` - `_228_`, `ü` - `_252_`. The Avro schema also doesn't allow for the first character to be a number. So we encode the first character in the way if the character happens to be a number.

<br>

___

<br>

# Example

Putting everything together we are able to see a complete reference to the Demographic node from a valid PFB representing the NHLBI BioData Catalyst powered by Gen3

```
{
    "name": "demographic",
    "ontology_reference": "",
    "values": {},
    "links": [{
        "multiplicity": "ONE_TO_ONE",
        "dst": "subject",
        "name": "subjects"
    }],
    "properties": [{
        "name": "annotated_sex",
        "ontology_reference": "Person Gender Text Type",
        "values": {
            "source": "caDSR",
            "cde_id": "2200604",
            "cde_version": "3.0",
            "term_url": "https://cdebrowser.nci.nih.gov/CDEBrowser/search?elementDetails=9&FirstTimer=0&PageId=ElementDetailsGroup&publicId=2200604&version=3.0"
        }
    }, {
        "name": "race",
        "ontology_reference": "Race Category Text",
        "values": {
            "source": "caDSR",
            "cde_id": "2192199",
            "cde_version": "1.0",
            "term_url": "https://cdebrowser.nci.nih.gov/CDEBrowser/search?elementDetails=9&FirstTimer=0&PageId=ElementDetailsGroup&publicId=2192199&version=1.0"
        }
    }, {
        "name": "ethnicity",
        "ontology_reference": "Ethnic Group Category Text",
        "values": {
            "source": "caDSR",
            "cde_id": "2192217",
            "cde_version": "2.0",
            "term_url": "https://cdebrowser.nci.nih.gov/CDEBrowser/search?elementDetails=9&FirstTimer=0&PageId=ElementDetailsGroup&publicId=2192217&version=2.0"
        }
    }, {
        "name": "weight",
        "ontology_reference": "Patient Weight Measurement",
        "values": {
            "source": "caDSR",
            "cde_id": "651",
            "cde_version": "4.0",
            "term_url": "https://cdebrowser.nci.nih.gov/CDEBrowser/search?elementDetails=9&FirstTimer=0&PageId=ElementDetailsGroup&publicId=651&version=4.0"
        }
    }, {
        "name": "bmi_baseline",
        "ontology_reference": "Body Mass Index (BMI)",
        "values": {
            "source": "caDSR",
            "cde_id": "4973892",
            "cde_version": "1.0",
            "term_url": "https://cdebrowser.nci.nih.gov/CDEBrowser/search?elementDetails=9&FirstTimer=0&PageId=ElementDetailsGroup&publicId=4973892&version=1.0"
        }
    }, {
        "name": "height_baseline",
        "ontology_reference": "Patient Height Measurement",
        "values": {
            "source": "caDSR",
            "cde_id": "649",
            "cde_version": "4.1",
            "term_url": "https://cdebrowser.nci.nih.gov/CDEBrowser/search?elementDetails=9&FirstTimer=0&PageId=ElementDetailsGroup&publicId=649&version=4.1"
        }
    }]
}

```

The schema on the image below shows the simplest model with only **Demographic** data.

[![schema][4]][4]

The Avro IDL for this schema is located in [`sample.avdl`][16].

  [1]: https://avro.apache.org/docs/current/spec.html
  [2]: https://ncit.nci.nih.gov/ncitbrowser/pages/
  [3]: https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#/search
  [4]: ./schema.svg
  [16]: ./sample.avdl


