import itertools
import os
import random
import shutil
import string
import sys
from functools import partial, reduce
from typing import List

import requests
import csv
import json
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index

from pfb.reader import PFBReader
from pfb.writer import PFBWriter
from tests.reference_file.test_ingestion import from_json_v2
from ref_file_helper import create_reference_file_node
from typing import List, Dict, Any


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


def ingest_json_files_into_pfb(ref_file_node):
    try:
        # todo: figure out where to get ref_file schema from
        # right now we get it from that manifest file in github iirc
        with PFBReader("avro/minimal_schema.avro") as s_reader:
            data_from_json = []
            for node_info in ref_file_node:
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


def add_or_insert(phs_to_file_data, file):
    field_value = file["study_with_consent"]
    field_exists = field_value in phs_to_file_data
    if field_exists:
        phs_to_file_data[field_value].append(file)
    else:
        phs_to_file_data[field_value] = [file]
    return phs_to_file_data


def test_ref_to_json():
    cred_path = os.environ.get("PP_CREDS")
    auth = Gen3Auth(refresh_file=cred_path)
    index = Gen3Index(auth_provider=auth)

    # there should be more than one guid per study
    dest_data = tsv_to_json("tsv/dest-bucket-manifest.tsv")
    acl_to_dest_data = dict(map(lambda d: (d["acl"], d), dest_data))
    acl_to_program_project = dict(map(lambda p: (p[0], p[1]["dataset_identifier"].split('-', 1)), acl_to_dest_data.items()))

    release_files = tsv_to_json("tsv/release_manifest_release-27.tsv")
    def remove_empty(d):
        v = d.get("study_with_consent")
        return bool(v)
    fields_with_study_with_consent = list(filter(remove_empty, release_files))

    guid_to_release_data = dict(map(lambda r: (r["guid"], r), fields_with_study_with_consent))
    guid_to_acl = dict(map(lambda p: (p[0], p[1]["study_with_consent"]), guid_to_release_data.items()))
    guid_to_program_project = dict(map(lambda p: (p[0], acl_to_program_project[p[1]]), guid_to_acl.items()))

    reference_file_guids = list(guid_to_program_project.keys())

    def chunk(lst, size):
        return [lst[i:i + size] for i in range(0, len(lst), size)]
    chunked_guids = chunk(reference_file_guids, 2500)
    def get_chunked_records(indexd_chunks, guid_chunk):
        indexd_data_chunk = index.get_records(guid_chunk)
        indexd_chunks += indexd_data_chunk
        return indexd_chunks
    indexd_data = reduce(get_chunked_records, chunked_guids[:2], [])
    def add_program_and_project_v2(indexd_entry):
        program_project = guid_to_program_project.get(indexd_entry["did"])
        assert program_project is not None
        indexd_entry["program"] = program_project[0]
        indexd_entry["project"] = program_project[1]
        return indexd_entry
    indexd_data_with_program_and_project = list(map(add_program_and_project_v2, indexd_data))

    submitter_ids = []
    # for index_dict in indexd_data_with_program_and_project:
        # print("blah")
        # todo: generate submitter id
        # x = create_reference_file_node(index_dict["project"], index_dict["acl"][1])
    reference_file_context = list(map(create_ref_file_node, indexd_data_with_program_and_project))

    def sort_by_program(program_to_ref_file_context, ref_context):
        program = ref_context["program"]
        program_exists = program in program_to_ref_file_context
        program_context = {"project": ref_context["project"], "reference_file": ref_context["reference_file"]}
        if program_exists:
            program_to_ref_file_context[program].append(program_context)
        else:
            program_to_ref_file_context[program] = [program_context]
        return program_to_ref_file_context
    program_to_reference_file_context = reduce(sort_by_program, reference_file_context, {})

    def collect_to_project(program_with_reference_files_under_program):
        def build_project_contexts(project_to_ref_file_context, project_context):
            project = project_context["project"]
            project_already_added = project in project_to_ref_file_context
            if project_already_added:
                project_to_ref_file_context[project].append(project_context["reference_file"])
            else:
                project_to_ref_file_context[project] = [project_context["reference_file"]]
            return project_to_ref_file_context
        reference_files_under_program = program_with_reference_files_under_program[1]
        project_to_reference_file_context = reduce(build_project_contexts, reference_files_under_program, {})
        return program_with_reference_files_under_program[0], project_to_reference_file_context

    program_to_project_context = dict(map(collect_to_project, program_to_reference_file_context.items()))

    def sort_by_program():
        print("blah")

    output_directory_for_ref_file_json_files = "json/output_ref_files/"
    # if not os.path.exists(output_directory_for_ref_file_json_files):
    #     try:
    #         os.makedirs(output_directory_for_ref_file_json_files)
    #     except Exception as e:
    #         print(e)
    # if len(os.listdir(output_directory_for_ref_file_json_files)) > 0:
    #     clear_directory(output_directory_for_ref_file_json_files)
    # for_each(list(enumerate(output)), partial(write_dicts_to_json_files, output_directory_for_ref_file_json_files))

    # for program, project_context in pfb_data_list.items()
    ingest_json_files_into_pfb(pfb_data_list)
    print("done!")




    # ref_file_data_by_project = reduce(group_by("project"), indexd_data_with_program_and_project, {})
    # ref_file_data_by_project_program = reduce(group_by("program",
    #                                                    lambda gid, e: e[1][0][gid]),
    #                                           list(ref_file_data_by_project.items()), {})
    # program_to_project_context = dict(map(lambda p: (p[0], dict(p[1])), ref_file_data_by_project_program.items()))


    # fields_organized_by_study = reduce(add_or_insert, fields_with_study_with_consent, {})

    # def add_program_and_project(study_id_to_study_context, study_pair):
    #     study_id = study_pair[0]
    #     destination_info = acl_to_dest_data.get(study_id)
    #     assert destination_info is not None
    #     program_project = destination_info["dataset_identifier"].split('-', 1)
    #     study_context = {"program": program_project[0], "project": program_project[1], "study": study_pair[1]}
    #     study_id_to_study_context[study_id] = study_context
    #     return study_id_to_study_context
    #
    # study_id_to_study_context = reduce(add_program_and_project, fields_organized_by_study.items(), {})
    # def map_to_project(project_to_study_id, study_tuple):
    #     project = study_tuple[1]["project"]
    #     project_already_added = project in project_to_study_id
    #     assert not project_already_added
    #     project_to_study_id[project] = study_tuple[1]
    #     return project_to_study_id
    # project_to_study_context = reduce(map_to_project, study_id_to_study_context.items(), {})
    # def map_to_program(program_to_project_context, project_tuple):
    #     program = project_tuple[1]["program"]
    #     program_already_added = program in program_to_project_context
    #     if program_already_added:
    #         program_to_project_context[program][project_tuple[0]] = project_tuple[1]
    #     else:
    #         program_to_project_context[program] = {project_tuple[0]: project_tuple[1]}
    #     return program_to_project_context
    # program_to_project_context = reduce(map_to_program, project_to_study_context.items(), {})
    #
    # def inner_get(inner_list, study):
    #     inner_list.append(study["guid"])
    #     return inner_list
    # def outer_get(outer_list, project_study_pair):
    #     inner_list = reduce(inner_get, project_study_pair[1]["study"], [])
    #     outer_list += inner_list
    #     return outer_list
    # guids_for_fields = reduce(outer_get, program_to_project_context["topmed"].items(), [])

    # for acl in acl_set:
        # params = {"study_with_consent": acl}
        # a = index.get_with_params(params)
        # reference_file_data_from_indexd.append(a)

    # studies_organized_by_project = reduce(lambda p: "blah", fields_organized_by_study.items(), {})

    # phsid_to_file_data = dict(map(lambda d: (d["study_with_consent"], d), fields_with_study_with_consent))
    # study_set = set(acl_to_dest_data.keys())
    # files_with_program_and_project = list(map(add_program_and_project(acl_to_dest_data),
    #                                           phsid_to_file_data.values()))
    # files_sorted_by_project = reduce(group_by("project"), files_with_program_and_project, {})
    # projects_sorted_by_program = reduce(group_by("program", lambda gid, e: e[1][0][gid]),
    #                                     list(files_sorted_by_project.items()), {})
    # def check_lake(ss):
    #     def value_in_lake(n):
    #         return n["study_with_consent"] in ss
    #     return value_in_lake

    # fields_to_make_nodes_for = list(filter(check_lake(study_set), list(nodes_with_program_and_project)))
    # guids_for_fields = list(map(lambda d: d["guid"], fields_to_make_nodes_for))
    # acl_set = set(fields_to_make_nodes_for)

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
