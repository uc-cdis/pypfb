import itertools
import os
import random
import shutil
import string
import sys
from functools import partial, reduce

import requests
import csv
import json
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index

from pfb.reader import PFBReader
from pfb.writer import PFBWriter
from tests.reference_file.test_ingestion import from_json_v2


def tsv_to_json(tsv_file_path):
    data = []
    with open(tsv_file_path, 'r', newline='', encoding='utf-8') as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter='\t')
        for row in reader:
            data.append(row)
    return data


def test_tsv_ingestion():
    outcome = tsv_to_json("tsv/dest-bucket-manifest.tsv")
    print(outcome)


from datetime import datetime, timezone


def generate_random_string():
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(12))
    return random_string


def create_ref_file_node(indexd_data):
    reference_file = {
        "data_category": "Clinical Data",
        "data_format": "XML",
        "data_type": "Other",
        "file_name": indexd_data["file_name"],
        "file_size": indexd_data["size"],
        "md5sum": indexd_data["hashes"]["md5"],
        "submitter_id": generate_random_string(),
        "type": "reference_file"

        # Add object id!
        # Gag4
        # bucket_path

    }
    pfb_data = {
        "program": indexd_data["program"],
        "project": indexd_data["project"],
        "reference_file": reference_file
    }
    return pfb_data


def clear_directory(directory_path):
    # Check if the directory exists
    if not os.path.exists(directory_path):
        print(f"The directory {directory_path} does not exist.")
        return

    # Iterate over all the files and directories in the specified directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        try:
            # Check if it is a file and remove it
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
            # Check if it is a directory and remove it
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')


def write_dicts_to_json_files(directory, index_with_node_dict):
    """
    Writes each dictionary in the list to a separate JSON file.

    :param dict_list: List of dictionaries to write to files.
    :param directory: The directory where the files will be saved.
    """

    file_path = os.path.join(directory, f"entry_{index_with_node_dict[0]+1}.json")
    with open(file_path, 'w') as json_file:
        json.dump(index_with_node_dict[1], json_file, indent=4)


def for_each(iterable, run_side_effect):
    for item in iterable:
        run_side_effect(item)


def ingest_json_files_into_pfb(ref_file_nodes):
    try:
        # todo: figure out where to get ref_file schema from
        # right now we get it from that manifest file in github iirc
        with PFBReader("avro/minimal_schema.avro") as s_reader:
            data_from_json = []
            for node_info in ref_file_nodes:
                data_from_json.append(from_json_v2(s_reader.metadata, ("reference_file", node_info)))
            with PFBWriter("minimal_data.avro") as d_writer:
                d_writer.copy_schema(s_reader)
                for json_data in data_from_json:
                    d_writer.write([json_data])
            with PFBReader("minimal_data.avro") as d_reader:
                for r in itertools.islice(d_reader, None):
                    json.dump(r, sys.stdout)
                    sys.stdout.write("\n")

    except Exception as e:
        print("Failed! -> ", e)
        raise


def add_program_and_project(dest):
    def get_program_and_project(ref_file_info):
        dataset = dest.get(ref_file_info["study_with_consent"], None)
        assert dataset is not None
        program_project = dataset["dataset_identifier"].split('-', 1)
        ref_file_info["program"] = program_project[0]
        ref_file_info["project"] = program_project[1]
        return ref_file_info
    return get_program_and_project


def add_program_and_project_to_indexd_closure(guid_to_updated_nodes):
    def add_program_and_project_to_indexd(indexd_data):
        dataset = guid_to_updated_nodes.get(indexd_data["did"], None)
        assert dataset is not None
        indexd_data["program"] = dataset["program"]
        indexd_data["project"] = dataset["project"]
        return indexd_data
    return add_program_and_project_to_indexd



def insert_or_append(graph, key, value):
    # Get the list for the key, creating it if necessary
    graph.setdefault(key, []).append(value)
    return graph


def group_by(group_identifier, accessor=lambda gid, e: e[gid], inserter=insert_or_append):
    def add_to_graph(graph, data_entry):
        group_id = accessor(group_identifier, data_entry)
        graph = inserter(graph, group_id, data_entry)
        return graph
    return add_to_graph


def test_ref_to_json():
    cred_path = os.environ.get("PP_CREDS")
    auth = Gen3Auth(refresh_file=cred_path)
    index = Gen3Index(auth_provider=auth)


    dest_data = tsv_to_json("tsv/dest-bucket-manifest.tsv")
    ref_file_data = tsv_to_json("tsv/release_manifest_release-27.tsv")
    filter_by_field = lambda field_name, dictionary: list(filter(lambda d: d.get(field_name), dictionary))
    map_dict_to_field = lambda field_name, dictionary: dict(map(lambda d: (d[field_name], d), dictionary))
    workable_ref_fields = map_dict_to_field("study_with_consent", filter_by_field("study_with_consent", ref_file_data))
    acl_to_dest_data = dict(map(lambda d: (d["acl"], d), dest_data))
    study_set = set(acl_to_dest_data.keys())
    nodes_with_program_and_project = list(map(add_program_and_project(acl_to_dest_data), workable_ref_fields.values()))
    guid_to_updated_nodes = dict(map(lambda n: (n["guid"], n), nodes_with_program_and_project))
    def check_lake(ss):
        def value_in_lake(n):
            return n["study_with_consent"] in ss
        return value_in_lake

    fields_to_make_nodes_for = list(filter(check_lake(study_set), list(nodes_with_program_and_project)))
    guids_for_fields = list(map(lambda d: d["guid"], fields_to_make_nodes_for))
    # acl_set = set(fields_to_make_nodes_for)
    reference_file_data_from_indexd = index.get_records(guids_for_fields)
    indexd_data_with_program_and_project = list(map(add_program_and_project_to_indexd_closure(guid_to_updated_nodes),
                                                    reference_file_data_from_indexd))

    ref_file_data_by_project = reduce(group_by("project"), indexd_data_with_program_and_project, {})
    ref_file_data_by_project_program = reduce(group_by("program",
                                                       lambda gid, e: e[1][0][gid]),
                                              list(ref_file_data_by_project.items()), {})
    # todo: why am i getting only record per file?
    final_answer_so_far = dict(map(lambda p: (p[0], dict(p[1])), ref_file_data_by_project_program.items()))

    x = create_reference_file_node()

    # for acl in acl_set:
        # params = {"study_with_consent": acl}
        # a = index.get_with_params(params)
        # reference_file_data_from_indexd.append(a)

    pfb_data_list = list(map(create_ref_file_node, indexd_data_with_program_and_project))
    output_directory_for_ref_file_json_files = "json/output_ref_files/"
    # if not os.path.exists(output_directory_for_ref_file_json_files):
    #     try:
    #         os.makedirs(output_directory_for_ref_file_json_files)
    #     except Exception as e:
    #         print(e)
    # if len(os.listdir(output_directory_for_ref_file_json_files)) > 0:
    #     clear_directory(output_directory_for_ref_file_json_files)
    # for_each(list(enumerate(output)), partial(write_dicts_to_json_files, output_directory_for_ref_file_json_files))
    ingest_json_files_into_pfb(pfb_data_list)
    print("done!")


    # url = "https://preprod.gen3.biodatacatalyst.nhlbi.nih.gov/index/index/"
    # params = {"page": 0}  # Set the number of records to fetch per request
    # data_array = []
    # outcome1 = index.get_all_records()
    # tsv_acls = map(lambda row: row["acl"], dest_data)
    # params = {"acl": [next(iter(dest_data))["acl"]]}
    # outcome2 = index.get_with_params(params)
    # outcome = create_ref_file_node(outcome2)
    # print(outcome1, outcome2)
    # try:
    #     while params["page"] < 1:
    #         response = requests.get(url, params=params)
    #         if response.status_code == 200:
    #             data = response.json()
    #             if len(data["records"]) == 0:
    #                 break
    #             else:
    #                 data_array.extend(data["records"])
    #                 params["page"] = params.get("page") + 1
    #         else:
    #             print("Failed to retrieve data. Status code:", response.status_code)
    #             break
    # except requests.exceptions.RequestException as e:
    #     print("Error fetching data:", e)
    # print("Total records fetched:", len(data_array))

    # get data from indexd
    # compare against the tsv
