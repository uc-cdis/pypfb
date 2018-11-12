# python-pfb-sdk
Python scripts used to modify PFB files
## PFB Editing Scripts

### Add Field
Adds a new field to the specified node for the specified pfb file as of right now this is only limited to string and int with enum supported later
This writes to a new file new.pfb

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

Example usage

	command line usage:
		python reader.py test.pfb
	python usage:
		import reader
		reader.read("test.pfb")

### Remove Field
removes a field from pfb file to a new file (rm.pfb)

	usage: removeField.py [-h] PFB_file parentField field

	Remove a field from PFB schema and output to new file "rm.pfb"

	positional arguments:
	  PFB_file     pfb file to remove field from. Default = test.pfb
	  parentField  parent of field wanting to remove from schema
	  field        field to remove to the schema

	optional arguments:
	  -h, --help   show this help message and exit

Example usage
	
	command line usage:
		python removeField.py test.pfb demographic gender
	python usage:
		import removeField
		removeField.remove("test.pfb", "simple_germline_variation", "data_category")


### Add Record
Adds new record(s) to the pfb file from a json file

	usage: addRecord.py [-h] PFB_file JSON_file

	Add a record to PFB file from a minified JSON file

	positional arguments:
	  PFB_file    pfb file to add record to. Default = test.pfb
	  JSON_file   JSON file to add into the pfb file. Default = test.json

	optional arguments:
	  -h, --help  show this help message and exit

Exapmle Usage

	command line usage:
		python addRecord.py test.pfb test.json
	python usage
		import addRecord
		addRecord.add("test.pfb", "test.json")

### Reader
Used to read the records from pfb file records]

	usage: reader.py [-h] PFB_file

	Read a PFB file in json format

	positional arguments:
	  PFB_file    pfb file to add record to. Default = test.pfb

	optional arguments:
	  -h, --help  show this help message and exit

Example Usage

	command line usage:
		python reader.py test.pfb
	python usage:
		import reader
		reader.read("test.pfb")

### Make Blank Record
Make a blank json record for specific pfb schema to blank.json files

	usage: makeBlankRecord.py [-h] PFB_file node

	Create a blank record from a pfb schema

	positional arguments:
	  PFB_file    pfb file to read schema from
	  node        Node to add record to

	optional arguments:
	  -h, --help  show this help message and exit

Example Usage

	command line usage:
		python makeBlankRecord.py test.pfb demographic
	python usage:
		import makeBlankRecord
		makeBlankRecord.makeRecord("test.pfb", "simple_germline_variation")

