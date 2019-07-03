# :construction: PFB Python SDK :construction:

Python SDK to create, explore and modify PFB (Portable Format for Biomedical Data) files.

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
pipenv install
```

(Also add `--dev` for development.)


## Usage

### Main

    Usage: pfb [OPTIONS] COMMAND [ARGS]...

      PFB: Portable Format for Biomedical Data.

    Commands:
      add     Add a record from a minified JSON file to the PFB file.
      from    Generate PFB from other data formats.
      make    Make blank record from the PFB file.
      rename  Rename different parts of schema.
      show    Show schema or records of the PFB file.

### Show different parts of PFB

    Usage: pfb show [OPTIONS] PFB

      Show schema or records of the PFB file.

    Options:
      -s, --schema     Show PFB file schema.
      --limit INTEGER  How many entries to show, -1 for all; ignored for "schema".

### Convert Gen3 data dictionary into PFB schema

    Usage: pfb from dict [OPTIONS] URL

      Convert Gen3 data dictionary at URL into PFB file.

    Options:
      -o, --output FILENAME  Output PFB file.  [required]

### Convert JSON for corresponding datadictionary to PFB

    Usage: pfb from json [OPTIONS] PATH

      Convert JSON files under PATH into a PFB file.

    Options:
      -s, --schema FILENAME  The PFB file to load the schema from.  [required]
      -o, --output FILENAME  The result PFB file.  [required]
      --program TEXT         Name of the program.  [required]
      --project TEXT         Name of the project.  [required]

### Make new blank record

    Usage: pfb make [OPTIONS] PFB

      Make blank record from the PFB file.

    Options:
      -n, --node TEXT  Node to create.  [required]

### Add new record to PFB

    Usage: pfb add [OPTIONS] JSON PFB

      Add a record from a minified JSON file to the PFB file.

### Rename different parts of PFB (schema evolution)

    Usage: pfb rename [OPTIONS] COMMAND [ARGS]...

      Rename different parts of schema.

    Options:
      -i, --input FILENAME   Source PFB file.  [required]
      -o, --output FILENAME  Destination PFB file.  [required]

    Commands:
      enum  Rename enum.
      node  Rename node.
      type  Rename type (not implemented).

### Rename node

    Usage: pfb rename [PARENT OPTIONS] node [OPTIONS]

      Rename node.

    Options:
      --from TEXT  [required]
      --to TEXT    [required]

### Rename enum

    Usage: pfb rename [PARENT OPTIONS] enum [OPTIONS]

      Rename enum.

    Options:
      --field TEXT  [required]
      --from TEXT   [required]
      --to TEXT     [required]


## Examples

    pfb from dict http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json -o ./tests/schema/kf.avro
    
    pfb from json ./tests/data -s ./tests/schema/kf.avro -o tests/pfb-data/test.avro --program DEV --project test

    pfb rename -i tests/pfb-data/test.avro -o tests/pfb-data/rename_test.avro node --name_from slide --name_to slide_test
    
    pfb rename -i tests/pfb-data/test.avro -o tests/pfb-data/rename_test.avro enum --field_name state --val_from validated --val_to validated_test
    
    pfb show -s --limit -1 tests/pfb-data/test.avro 


  [1]: ./doc/metadata.svg
