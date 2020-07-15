# :construction: PFB Python SDK :construction:

Python SDK to create, explore and modify PFB (Portable Format for Biomedical Data) files.


## PyPFB Overview

PyPFB is a python sdk to create, explore, and modify PFB (Portable Format for Bioinformatics) files.

These files start from a Gen3 data dictionary. These can be made from either json hosted on a cloud storage service, like s3, or from a local directory. See PyPFB From Schema for an example.

Once we have a PFB file created from a schema we can start to add data to the file. This is done using JSON files from local directory. We create them in the style of our data-simulator https://github.com/uc-cdis/data-simulator/ Once we have them we can use PFB From JSON to import the structured json into our Serialized PFB file.

At this point we have a PFB file with married schema and serialized data. Now we have a few options for modifying these PFB files. These are good options for breaking changes within the dictionary. This allows a commons operator to export the entire structured database, make modifications to fix the breaking changes, and then re import the file back to the commons. 

Changes that are already supported by this SDK are renames (enum and nodes) and adds of records data.


## PFB Schema

[![metadata][1]][1]

## Installation

* From PyPI:

```bash
pip install pypfb[gen3]
```

(The optional `gen3` dependencies add the ability to convert a Gen3 data dictionary into
a PFB file.)

* From source code:

```bash
poetry install
```


## Usage

### Main

    Usage: pfb [OPTIONS] COMMAND [ARGS]...

      PFB: Portable Format for Biomedical Data.

    Commands:
      add     Add records into a PFB file.
      from    Generate PFB from other data formats.
      make    Make a blank record for add.
      rename  Rename different parts of schema.
      show    Show different parts of a PFB file.
      to      Convert PFB into other data formats.

### Show different parts of PFB

    Usage: pfb show [OPTIONS] COMMAND [ARGS]...

      Show records of the PFB file.

      Specify a sub-command to show other information.

    Options:
      -i, --input FILENAME  The PFB file.  [default: <stdin>]
      -n, --limit INTEGER   How many records to show, ignored for sub-commands.
                            [default: no limit]

    Commands:
      metadata  Show the metadata of the PFB file.
      nodes     Show all the node names in the PFB file.
      schema    Show the schema of the PFB file.

    Examples:
      schema:
        pfb show -i data.avro schema
      nodes:
        pfb show -i data.avro nodes
      metadata:
        pfb show -i data.avro metadata
      records:
        pfb show -i data.avro -n 5

### Convert Gen3 data dictionary into PFB schema

    Usage: pfb from [PARENT OPTIONS] dict DICTIONARY

      Convert Gen3 data DICTIONARY into a PFB file.

      If DICTIONARY is a HTTP URL, it will be downloaded and parsed as JSON; or
      it will be treated as a local path to a directory containing YAML files.

    Parent Options:
      -o, --output FILENAME  The output PFB file.  [default: <stdout>]

    Examples:
      URL:
        pfb from -o thing.avro dict https://s3.amazonaws.com/dictionary-artifacts/gtexdictionary/3.2.2/schema.json
      Directory:
        pfb from -o gdc.avro dict /path/to/dictionary/schemas/

### Convert JSON for corresponding datadictionary to PFB

    Usage: pfb from [PARENT OPTIONS] json [OPTIONS] [PATH]

      Convert JSON files under PATH into a PFB file.

    Parent Options:
      -o, --output FILENAME  The output PFB file.  [default: <stdout>]

    Options:
      -s, --schema FILENAME  The PFB file to load the schema from.  [required]
      --program TEXT         Name of the program.  [required]
      --project TEXT         Name of the project.  [required]

    Example:
      pfb from -o data.avro json -s schema.avro --program DEV --project test /path/to/data/json/

### Make new blank record

    Usage: pfb make [OPTIONS] NAME

      Make a blank record according to given NODE schema in the PFB file.

    Options:
      -i, --input PFB  Read schema from this PFB file.  [default: <stdin>]

    Example:
      pfb make -i test.avro demographic > empty_demographic.json

### Add new record to PFB

    Usage: pfb add [OPTIONS] PFB

      Add records from a minified JSON file to the PFB file.

    Options:
      -i, --input JSON  The JSON file to add.  [default: <stdin>]

    Example:
      pfb add -i new_record.json pfb.avro 

### Rename different parts of PFB (schema evolution)

    Usage: pfb rename [OPTIONS] COMMAND [ARGS]...

      Rename different parts of schema.

    Options:
      -i, --input FILENAME   Source PFB file.  [default: <stdin>]
      -o, --output FILENAME  Destination PFB file.  [default: <stdout>]

    Commands:
      enum  Rename enum.
      node  Rename node.
      type  Rename type (not implemented).

    Examples:
      enum:
        pfb rename -i data.avro -o data_enum.avro enum demographic_ethnicity old_enum new_enum
      node:
        pfb rename -i data.avro -o data_update.avro node demographic information

### Rename node

    Usage: pfb rename [PARENT OPTIONS] node [OPTIONS] OLD NEW

      Rename node from OLD to NEW.

### Rename enum

    Usage: pfb rename [PARENT OPTIONS] enum [OPTIONS] FIELD OLD NEW

      Rename enum of FIELD from OLD to NEW.

### Convert PFB into Neptune (bulk load format for Gremlin)

    Usage: pfb to [PARENT OPTIONS] gremlin [OPTIONS] [OUTPUT]

      Convert PFB into CSV files under OUTPUT for Neptune bulk load (Gremlin).

      The default OUTPUT is ./gremlin/.

    Options:
      --gzip / --no-gzip  Whether gzip the output.  [default: yes]

    Example:
      pfb to -i data.avro gremlin

### Example of minimal PFB
    In the examples/minimal-pfb directory we have an example of a minimal pfb that only contains submitted unaligned read data

    First create the PFB with schema from the json dictionary
    pfb from -o minimal_schema.avro dict minimal_file.json

    Then we put the data into the PFB
    pfb from -o minimal_data.avro json -s minimal_schema.avro --program DEV --project test sample_file_json/

    We can view the data of the PFB
    pfb show -i minimal_data.pfb

    We can also view the schema of the PFB
    pfb show -i minimal_data.pfb schema


## Examples

    pfb from dict http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json > ./tests/schema/kf.avro
    
    pfb from json ./tests/data -s ./tests/schema/kf.avro --program DEV --project test > tests/pfb-data/test.avro

    cat tests/pfb-data/test.avro | pfb rename node slide slide_test > tests/pfb-data/rename_test.avro
    
    cat tests/pfb-data/test.avro | pfb rename enum state validated validated_test > tests/pfb-data/rename_test.avro
    
    cat tests/pfb-data/test.avro | pfb show -n 1 | jq

    cat tests/pfb-data/test.avro | pfb show --schema | jq

    cat tests/pfb-data/test.avro | pfb to gremlin ./output/


  [1]: ./doc/schema.svg
