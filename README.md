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
      -d DICTIONARY, --dictionary DICTIONARY
                            Link to dictionary URL
      -o OUTPUT, --output OUTPUT
                            Output PFB file

## Examples

    dict2pfb.py -d http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json
    dict2pfb.py -d http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json -o kf.avro
    dict2pfb.py --dictionary http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json --output kf.avro
    dict2pfb.py -h

  [1]: ./doc/metadata.svg
