import argparse
import logging.config
import os

import yaml

from avro_utils.avro_schema import AvroSchema
from utils.dictionary import init_dictionary

default_level = logging.INFO
config_path = 'config.yml'

if os.path.exists(config_path):
    with open(config_path, 'rt') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
else:
    logging.basicConfig(level=default_level)

log = logging.getLogger()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert Dictionary to PFB')
    parser.add_argument('-d', '--dictionary', required=True, type=str, help='Link to dictionary URL')
    parser.add_argument('-o', '--output', required=True, type=argparse.FileType('wb'), help='Output PFB file')
    args = parser.parse_args()

    dictionary, _ = init_dictionary(args.dictionary)
    schema = dictionary.schema

    log.info('Using dictionary: {}'.format(args.dictionary))

    avro_schema = AvroSchema.from_dictionary(schema)
    avro_schema.write(args.output)
