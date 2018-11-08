# python-pfb-sdk
Python scripts used to modify PFB files
## PFB Editing Scripts

### Add Field
	usage: addField.py [-h] PFB_file parentField field fieldType fieldDefault

	Add a new field to PFB schema

	positional arguments:
	  PFB_file      pfb file to add record to. Default = test.pfb
	  parentField   parent of field wanting to add to schema
	  field         field to add to the schema
	  fieldType     type for new field
	  fieldDefault  default value for new field

	optional arguments:
	  -h, --help    show this help message and exit

### Remove Field
	usage: removeField.py [-h] PFB_file parentField field

	Remove a field from PFB schema and output to new file "rm.pfb"

	positional arguments:
	  PFB_file     pfb file to remove field from. Default = test.pfb
	  parentField  parent of field wanting to remove from schema
	  field        field to remove to the schema

	optional arguments:
	  -h, --help   show this help message and exit

### Add Record
	usage: addRecord.py [-h] PFB_file JSON_file

	Add a record to PFB file from a minified JSON file

	positional arguments:
	  PFB_file    pfb file to add record to. Default = test.pfb
	  JSON_file   JSON file to add into the pfb file. Default = test.json

	optional arguments:
	  -h, --help  show this help message and exit

### Reader
	usage: reader.py [-h] PFB_file

	Read a PFB file in json format

	positional arguments:
	  PFB_file    pfb file to add record to. Default = test.pfb

	optional arguments:
	  -h, --help  show this help message and exit

### Make Blank Record
	usage: makeBlankRecord.py [-h] PFB_file node

	Create a blank record from a pfb schema

	positional arguments:
	  PFB_file    pfb file to read schema from
	  node        Node to add record to

	optional arguments:
	  -h, --help  show this help message and exit

