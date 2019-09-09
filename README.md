# :construction: PFB Python SDK :construction:

Python SDK to create, explore and modify PFB (Portable Format for Biomedical Data) files.

## Installation

* From PyPI:

```bash
pip install pypfb[gen3]
```

(The optional `gen3` dependencies add the ability to convert a Gen3 data dictionary into
a PFB file.)

* From source code:

```bash
pipenv install
```

(Also add `--dev` for development.)


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

### Convert Gen3 data dictionary into PFB schema

    Usage: pfb from [PARENT OPTIONS] dict DICTIONARY

      Convert Gen3 data DICTIONARY into a PFB file.

      If DICTIONARY is a HTTP URL, it will be downloaded and parsed as JSON; or
      it will be treated as a local path to a directory containing YAML files.

    Parent Options:
      -o, --output FILENAME  The output PFB file.  [default: <stdout>]

### Convert JSON for corresponding datadictionary to PFB

    Usage: pfb from [PARENT OPTIONS] json [OPTIONS] [PATH]

      Convert JSON files under PATH into a PFB file.

    Parent Options:
      -o, --output FILENAME  The output PFB file.  [default: <stdout>]

    Options:
      -s, --schema FILENAME  The PFB file to load the schema from.  [required]
      --program TEXT         Name of the program.  [required]
      --project TEXT         Name of the project.  [required]

### Make new blank record

    Usage: pfb make [OPTIONS] NAME

      Make a blank record according to given NODE schema in the PFB file.

    Options:
      -i, --input PFB  Read schema from this PFB file.  [default: <stdin>]

### Add new record to PFB

    Usage: pfb add [OPTIONS] PFB

      Add records from a minified JSON file to the PFB file.

    Options:
      -i, --input JSON  The JSON file to add.  [default: <stdin>]

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


## Examples

    pfb from dict http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json > ./tests/schema/kf.avro
    
    pfb from json ./tests/data -s ./tests/schema/kf.avro --program DEV --project test > tests/pfb-data/test.avro

    cat tests/pfb-data/test.avro | pfb rename node slide slide_test > tests/pfb-data/rename_test.avro
    
    cat tests/pfb-data/test.avro | pfb rename enum state validated validated_test > tests/pfb-data/rename_test.avro
    
    cat tests/pfb-data/test.avro | pfb show -n 1 | jq

    cat tests/pfb-data/test.avro | pfb show --schema | jq

    cat tests/pfb-data/test.avro | pfb to gremlin ./output/
