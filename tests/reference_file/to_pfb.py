import itertools
import os
import sys
from functools import partial, reduce
import csv
import json

import requests
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index

from pfb.reader import PFBReader
from pfb.writer import PFBWriter
from tests.reference_file.test_ingestion import from_json
from typing import List, Dict, Any

LEFT = 0
RIGHT = 1


def tsv_to_json(tsv_file_path) -> List[Dict[Any, Any]]:
    data = []
    with open(tsv_file_path, 'r', newline='', encoding='utf-8') as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter='\t')
        for row in reader:
            data.append(row)
    return data


def test_tsv_ingestion():
    outcome = tsv_to_json("tsv/dest-bucket-manifest.tsv")
    print(outcome)


def create_original_file_node(indexd_data):
    object_id = indexd_data["did"]
    drs_uri = f"drs://{object_id}"

    original_file = {
        "submitter_id": indexd_data["submitter_id"],
        "type": "original_file",
        "ga4gh_drs_uri": drs_uri,
    }
    pfb_data = {
        "program": indexd_data["program"],
        "project": indexd_data["project"],
        "original_file": original_file
    }
    return pfb_data


def ingest_json_files_into_pfb(program, project, original_file_nodes):
    try:
        # pfb from -o thing.avro dict https://s3.amazonaws.com/dictionary-artifacts/gtexdictionary/3.2.2/schema.json
        # todo: figure out where to get original_file schema from
        # right now we get it from that manifest file in github iirc
        # url https://raw.githubusercontent.com/uc-cdis/cdis-manifest/main/README.md
        path = "avro/raw_pfbs/" + project + ".avro"
        with PFBReader("avro/schema/example_schema.avro") as s_reader:
            data_from_json = []
            for original_file_node in original_file_nodes:
                node_info = {
                    "program": program,
                    "project": project,
                    "original_file": original_file_node
                }
                data_from_json.append(from_json(s_reader.metadata, node_info))
            with PFBWriter(path) as d_writer:
                d_writer.copy_schema(s_reader)
                d_writer.write(data_from_json)
            with PFBReader(path) as d_reader:
                for r in itertools.islice(d_reader, None):
                    json.dump(r, sys.stdout)
                    sys.stdout.write("\n")
    except Exception as e:
        print("Failed! -> ", e)
        raise


def map_from(key, dictionaries):
    def map_from_key_in_dictionary(dictionary):
        value_at_key = dictionary[key]
        return value_at_key, dictionary

    return dict(map(map_from_key_in_dictionary, dictionaries))


def map_values(mutator, dictionary):
    def map_value(kv_pair):
        return kv_pair[LEFT], mutator(kv_pair[RIGHT])

    return dict(map(map_value, dictionary.items()))


def derive_acl_to_program_project_from_destination_manifest(manifest_location):
    dest_data = tsv_to_json(manifest_location)
    acl_to_dest_data = map_from("acl", dest_data)

    def get_program_project_from_dest_entry(dest_entry):
        program_project_pair = dest_entry["dataset_identifier"].split('-', 1)
        return {"program": program_project_pair[LEFT],
                "project": program_project_pair[RIGHT]}

    acl_to_program_project = map_values(get_program_project_from_dest_entry, acl_to_dest_data)
    return acl_to_program_project


def get_and_save_indexd_records_to_file(original_file_guids, index):
    cred_path = os.environ.get("PP_CREDS")
    auth = Gen3Auth(refresh_file=cred_path)
    index = Gen3Index(auth_provider=auth)

    def chunk(lst, size):
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    chunked_guids = chunk(original_file_guids, 2500)

    def get_and_save_chunked_records():
        def get_chunked_records(indexd_chunks, guid_chunk):
            indexd_data_chunk = index.get_records(guid_chunk)
            indexd_chunks += indexd_data_chunk
            return indexd_chunks

        indexd_data = reduce(get_chunked_records, chunked_guids, [])
        with open("json/indexd/ref_file_indexd_records.json", 'w') as file:
            json.dump(indexd_data, file, indent=4)


def read_json(file_location):
    with open(file_location, 'r') as file:
        file_contents = json.load(file)
    return file_contents


def read_tsv(file_location):
    with open(file_location, 'r') as file:
        reader = list(csv.DictReader(file, delimiter='\t'))
    return reader


def map_guid_to_release_data():
    release_files = tsv_to_json("tsv/release_manifest_release-27.tsv")
    fields_with_accession_number = list(filter(lambda entry: bool(entry.get("study_accession_with_consent")),
                                               release_files))
    guid_to_release_data = map_from("guid", fields_with_accession_number)
    return guid_to_release_data


def derive_guid_to_program_project(guid_to_release_data):
    acl_to_program_project = derive_acl_to_program_project_from_destination_manifest("tsv/dest-bucket-manifest.tsv")
    guid_to_acl = map_values(lambda field: field["study_with_consent"], guid_to_release_data)
    guid_to_program_project = map_values(lambda acl: acl_to_program_project[acl], guid_to_acl)
    return guid_to_program_project


def get_indexd_data_and_add_program_project(guid_to_program_project):
    indexd_data = read_json("json/indexd/ref_file_indexd_records.json")

    def add_program_and_project(indexd_entry):
        program_project = guid_to_program_project.get(indexd_entry["did"])
        assert program_project is not None
        indexd_entry["program"] = program_project["program"]
        indexd_entry["project"] = program_project["project"]
        return indexd_entry

    return list(map(add_program_and_project, indexd_data))


def try_next(iterator_instance):
    try:
        next_element = next(iterator_instance)
        return next_element
    except StopIteration:
        return None


def mint_new_submitter_id(existing_ids, bucket_url):
    path_components = bucket_url[5:].split("/")
    reversed_components = iter(path_components[::-1])

    def generate_submitter_id(previous_submitter_id, id_components):
        next_element = try_next(id_components)
        if next_element is None:
            return None
        else:
            return previous_submitter_id + "_" + next_element

    new_submitter_id = try_next(reversed_components)
    id_already_used = new_submitter_id in existing_ids
    exhausted_possible_submitter_ids = new_submitter_id is None
    while id_already_used and not exhausted_possible_submitter_ids:
        new_submitter_id = generate_submitter_id(new_submitter_id, reversed_components)
        id_already_used = new_submitter_id in existing_ids
        exhausted_possible_submitter_ids = new_submitter_id is None
    assert new_submitter_id is not None  # come up with a definitive way to create unique submitter IDs
    existing_ids.add(new_submitter_id)
    return new_submitter_id


def generate_submitter_ids(guid_to_bucket_urls):
    existing_ids = set()
    guid_to_submitter_ids = map_values(partial(mint_new_submitter_id, existing_ids), guid_to_bucket_urls)
    return guid_to_submitter_ids


def add_submitter_ids(indexd_contexts):
    def map_guid_to_bucket_path(mapping, indexd_context):
        mapping[indexd_context["did"]] = indexd_context["bucket_path"]
        return mapping
    guid_to_bucket_urls = reduce(map_guid_to_bucket_path, indexd_contexts, {})
    guid_to_submitter_ids = generate_submitter_ids(guid_to_bucket_urls)
    add_submitter_id = lambda d: insert(d, ("submitter_id", guid_to_submitter_ids[d["did"]]))
    original_file_context = list(map(add_submitter_id, indexd_contexts))
    return original_file_context


def upsert(identifier, graph, value):
    identifier_exists = identifier in graph
    if identifier_exists:
        graph[identifier].append(value)
    else:
        graph[identifier] = [value]
    return graph


def insert(dictionary, pair):
    dictionary.update([pair])
    return dictionary


def organize_by_program(program_to_original_file_context, original_file_context):
    program = original_file_context["program"]
    program_exists = program in program_to_original_file_context
    program_context = {"project": original_file_context["project"],
                       "original_file": original_file_context["original_file"]}
    if program_exists:
        program_to_original_file_context[program].append(program_context)
    else:
        program_to_original_file_context[program] = [program_context]
    return program_to_original_file_context


def collect_to_project(program_with_original_files_under_program):
    def build_project_contexts(project_to_original_file_context, project_context):
        project = project_context["project"]
        project_already_added = project in project_to_original_file_context
        if project_already_added:
            project_to_original_file_context[project].append(project_context["original_file"])
        else:
            project_to_original_file_context[project] = [project_context["original_file"]]
        return project_to_original_file_context

    original_files_under_program = program_with_original_files_under_program[1]
    project_to_original_file_nodes = reduce(build_project_contexts, original_files_under_program, {})
    return program_with_original_files_under_program[0], project_to_original_file_nodes


def add_bucket_path(source, desired_subset):
    source_map = {
        # from indexd 'urls' seem to(!) take the form [google_url, amazon_url, ...]
        # each entry represents the length of 'urls', and assumes(!) the order is the same between records
        "google": 1,
        "amazon": 2
    }
    subset_length = source_map[desired_subset]
    subset_location = subset_length - 1
    indexd_subset_by_cloud_source = list(filter(lambda context: len(context["urls"]) == subset_length, source))
    insert_bucket_path = lambda context: insert(context, ("bucket_path", context["urls"][subset_location]))
    node_context_with_bucket_path = list(map(insert_bucket_path, indexd_subset_by_cloud_source))
    return node_context_with_bucket_path


def test_full_ingestion_process():
    def get_manifest_schema_for_bdc():
        url = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/gen3.biodatacatalyst.nhlbi.nih.gov/manifest.json'
        manifest_result = requests.get(url)
        assert manifest_result.status_code == 200
        schema_location = manifest_result.json()["global"]["dictionary_url"]
        schema_result = requests.get(schema_location)
        return schema_result.json()
    schema = get_manifest_schema_for_bdc()
    #todo: pass this schema in, instead of the one that is saved locally

    guid_to_release_data = map_guid_to_release_data()
    guid_to_program_project = derive_guid_to_program_project(guid_to_release_data)
    indexd_data_with_program_and_project = get_indexd_data_and_add_program_project(guid_to_program_project)
    node_context_in_google = add_bucket_path(indexd_data_with_program_and_project, "google")
    node_context_in_amazon = add_bucket_path(indexd_data_with_program_and_project, "amazon")
    node_context_without_submitter_id = node_context_in_google + node_context_in_amazon
    full_context_for_nodes = add_submitter_ids(node_context_without_submitter_id)
    original_file_nodes = list(map(create_original_file_node, full_context_for_nodes))
    program_to_original_file_nodes = reduce(organize_by_program, original_file_nodes, {})
    program_to_project_context = dict(map(collect_to_project, program_to_original_file_nodes.items()))


    for program, project_context in program_to_project_context.items():
        for project, original_files in project_context.items():
            ingest_json_files_into_pfb(program, project, original_files)
    print("done!")
