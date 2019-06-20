# :construction: pypfb :construction:

Python SDK to create, explore and modify PFB files.

## PFB Schema

[![metadata][1]][1]

## Usage

### Main

    usage: pypfb [-h] {show,dict2pfb,json2pfb,make,rename} ...
    
    PFB tool
    
    positional arguments:
      {show,dict2pfb,json2pfb,make,rename}
        show                Show schema or records of the PFB file
        dict2pfb            Convert datadictionary into PFB file with schema
        json2pfb            Convert JSON files correspond to datadictionary into
                            PFB file
        make                Make blank record
        rename              Rename different parts of schema
    
    optional arguments:
      -h, --help            show this help message and exit

### Show different parts of PFB

    usage: pypfb show [-h] [-s] [--limit LIMIT] input
    
    positional arguments:
      input          Path to PFB file
    
    optional arguments:
      -h, --help     show this help message and exit
      -s, --schema   Show PFB file schema
      --limit LIMIT  How many entries to show, -1 for all; ignored for "schema"

### Convert datadictionary into PFB schema

    usage: pypfb dict2pfb [-h] -d DICTIONARY -o OUTPUT
    
    optional arguments:
      -h, --help            show this help message and exit
      -d DICTIONARY, --dictionary DICTIONARY
                            Link to dictionary URL
      -o OUTPUT, --output OUTPUT
                            Output PFB file

### Convert JSON for corresponding datadictionary to PFB

    usage: pypfb json2pfb [-h] -s SCHEMA -o OUTPUT --program PROGRAM
                                --project PROJECT
                                dir
    
    positional arguments:
      dir                   Path to directory with input JSON files
    
    optional arguments:
      -h, --help            show this help message and exit
      -s SCHEMA, --schema PFB SCHEMA
                            Filename for schema PFB file
      -o OUTPUT, --output OUTPUT
                            Filename for resulting PFB file
      --program PROGRAM     Name of the program
      --project PROJECT     Name of the project

### Make new blank record

    usage: pypfb make [-h] [-n NODE] input
    
    positional arguments:
      input                 Path to PFB file
    
    optional arguments:
      -h, --help            show this help message and exit
      -n NODE, --node NODE  Node to create

### Add new record to PFB

    usage: pypfb add [-h] PFB_file JSON_file
    
    Add a record to PFB file from a minified JSON file
    
    positional arguments:
      PFB_file    pfb file to add record to. Default = test.pfb
      JSON_file   JSON file to add into the pfb file. Default = test.json
    
    optional arguments:
      -h, --help  show this help message and exit

### Rename different parts of PFB (schema evolution)

    usage: pypfb rename [-h] {node,type,enum} ...
    
    positional arguments:
      {node,type,enum}
        node            Rename node
        type            Rename type (not implemented)
        enum            Rename enum (not implemented)
    
    optional arguments:
      -h, --help        show this help message and exit

### Rename node

    usage: pypfb rename node [-h] -i INPUT -o OUTPUT --name_from NAME_FROM
                                   --name_to NAME_TO
    
    optional arguments:
      -h, --help            show this help message and exit
      -i INPUT, --input INPUT
      -o OUTPUT, --output OUTPUT
      --name_from NAME_FROM
      --name_to NAME_TO

## Examples

    python pypfb dict2pfb -d http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json -o kf.avro
    dict2pfb.py -h
    
    python -m pypfb json2pfb ./tests/data -s ./tests/schema/kf.avro -o tests/pfb-data/test.avro --program DEV --project test

    python -m pypfb rename node --name_from slide --name_to slide_test -i tests/pfb-data/test.avro -o tests/pfb-data/rename_test.avro
    
    python pypfb/__main__.py show -s --limit -1 tests/pfb-data/test.avro 


  [1]: ./doc/metadata.svg
