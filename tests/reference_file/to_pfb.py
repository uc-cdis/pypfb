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


def create_reference_file_node(self, ppc, dbgap_ascnum, nhlbi_manifest, reference_file_node=None):
    '''
    This function creates a reference file node from the given project, dbgap accession number, NHLBI manifest,
    and optional reference file node.
    Arguments:
    self: Instance of the class
    ppc: Program, Project and Consent text string delimited by a "-" and "_" respectively. (ex. BioLINCC-MESA_HMB)
    dbgap_ascnum: dbGaP accession number (e.g., phs001234.v5.p1)
    nhlbi_manifest: Path to the NHLBI manifest file or a pandas DataFrame
    reference_file_node: Optional path to the reference file node or a pandas DataFrame (default: None_
    Returns:
    submission_df: Pandas DataFrame containing the reference file node data

    TODO: Some columns DNE for every project and should be dropped or not called if they DNE to avoid errors.
        For example, 'callset' is usually unique to topmed studies, but is fully integrated into this function.
        The function should be changed to dynamically check whether a column exists in 'reference_file_node'
        and append it with the existing data. Otherwise, only the required fields should be affixed.
    '''
    # If a reference file node is not provided, warn the user that default values will be used
    if reference_file_node is None:
        print("""NOTE: The 'reference_file_node' argument is not defined. The existing 'reference_file' node values from Gen3 will not map based on matching md5sum values in the NHLBI manifest. To match these values, include the 'reference_file_node' function argument.
        The function will apply these default values:
        1. 'callset' will be dropped as a column.
        2. 'file_type' = 'Other'
        3. 'data_category' = 'Clinical Data'
        4. 'data_type' = 'Other'""")
    # Ensure the NHLBI manifest and reference file node are dataframes
    nhlbi_manifest = self.ensure_dataframe(nhlbi_manifest)
    reference_file_node = self.ensure_dataframe(reference_file_node)
    # Check for errors in reading the files and return None if there are any
    if isinstance(nhlbi_manifest, str):
        print("Error reading nhlbi_manifest")
        return None
    if isinstance(reference_file_node, str):
        print("Error reading reference_file_node")
        return None
    # Split the dbGaP accession number into its constituent parts
    dbgap_phs, dbgap_version, dbgap_participant_set, dbgap_consent = dbgap_ascnum.split(sep = ".")
    # Split the ppc into program and proj_cons
    program, proj_cons = self.prog_projcons_partsplit(ppc)
    # Initialize a dataframe for the submission data
    submission_df = pd.DataFrame(columns=['type', 'submitter_id', 'projects.code', 'object_id', 'ga4gh_drs_uri', 'file_name', 'md5sum', 'file_size', 'bucket_path', 'callset', 'file_type', 'data_category', 'data_format', 'file_format', 'data_type', 'file_md5sum', 'study_version'])
    # Assign values to the submission dataframe from the NHLBI manifest
    submission_df['md5sum'] = nhlbi_manifest['md5sum'].astype(str)
    submission_df['type'] = 'reference_file'
    submission_df['projects.code'] = proj_cons
    submission_df['object_id'] = nhlbi_manifest['guid'].astype(str)
    submission_df['ga4gh_drs_uri'] = nhlbi_manifest['ga4gh_drs_uri'].astype(str)
    submission_df['file_name'] = nhlbi_manifest['file_name'].astype(str)
    submission_df['file_size'] = nhlbi_manifest['s3_file_size'].astype(str)
    submission_df['bucket_path'] = nhlbi_manifest['s3_path'].astype(str)

    def map_and_fillna(submission_df, reference_file_node, mapping_column, fill_with_column, fillna_value):
        '''
        Helper function to map values from the reference file node to the submission dataframe based on a given column,
        and fill NaN values with a given value.
        '''
        # Convert the mapping column in both dataframes to string
        submission_df[mapping_column] = submission_df[mapping_column].astype(str)
        reference_file_node[mapping_column] = reference_file_node[mapping_column].astype(str)
        # Set the mapping column as the index for easier access
        submission_df.set_index(mapping_column, inplace=True)
        reference_file_node.set_index(mapping_column, inplace=True)
        # Update the submission dataframe with values from the reference file node
        submission_df.update(reference_file_node[fill_with_column])
        # Fill NaN values in the fill_with_column with the fillna_value
        submission_df[fill_with_column] = submission_df[fill_with_column].fillna(fillna_value)
        # Reset the index of both dataframes
        submission_df.reset_index(inplace=True)
        reference_file_node.reset_index(inplace=True)
    # Store the original column order of the submission dataframe
    original_columns_order = submission_df.columns.tolist()
    # If a reference file node is not provided, assign default values to the submission dataframe
    if reference_file_node is None:
        submission_df['callset'] = ''
        submission_df['file_type'] = 'Other'
        submission_df['data_category'] = 'Clinical Data'
        submission_df['data_type'] = 'Other'
    # Otherwise, map and fill values from the reference file node
    else:
        map_and_fillna(submission_df, reference_file_node, 'md5sum', 'callset', 'Freeze 9b')
        map_and_fillna(submission_df, reference_file_node, 'md5sum', 'file_type', 'Other')
        map_and_fillna(submission_df, reference_file_node, 'md5sum', 'data_category', 'Clinical Data')
        map_and_fillna(submission_df, reference_file_node, 'md5sum', 'data_type', 'Other')
    # Reindex the submission dataframe to match the original column order
    submission_df = submission_df.reindex(columns=original_columns_order)
    def get_extension(filename):
        '''
        Helper function to get the file extension from a filename.
        If the filename has a compressed extension (e.g., .gz), the function recursively calls itself
        until it gets the actual file extension.
        '''
        # Split the filename into parts using '.' as the delimiter
        parts = filename.split('.')
        # If the filename does not have an extension, return 'TXT' as the default extension
        if len(parts) < 2:
            return 'TXT'
        else:
            ext = parts[-1]
            # Check if the extension is a compressed extension
            if ext.lower() in ['zip', 'gz', 'gzip', 'bz2', 'xz', '7z', 'rar', 'tar']:
                # If the file has only one '.', it means the extension is only a compressed extension
                if len(parts) == 2:
                    # Return the compressed extension in uppercase
                    return ext.upper()
                else:
                    # If the file has more than one '.', it means the file has an extension other than the compressed one
                    # Recursively call the function by removing the last part (compressed extension)
                    return get_extension('.'.join(parts[:-1]))
            else:
                # If the extension is not a compressed extension, return it in uppercase
                return ext.upper()
    # Apply the get_extension function to get the file format and data format
    submission_df['data_format'] = submission_df['file_name'].apply(get_extension)
    submission_df['file_format'] = submission_df['file_name'].apply(get_extension)
    submission_df['file_md5sum'] = submission_df['md5sum']
    submission_df['study_version'] = ''.join([char for char in dbgap_version if char.isdigit()])
    def generate_unique_submitter_ids(df, url_col, id_col):
        '''
        Helper function to generate unique submitter IDs based on the bucket path.
        '''
        # Initialize a set to store existing submitter IDs
        existing_ids = set()
        for i, row in df.iterrows():
            # Remove 's3://' and split the path into components
            path_components = row[url_col][5:].split('/')
            for j in range(len(path_components)):
                # Create a potential submitter ID by joining the relevant path components
                submitter_id = '_'.join(path_components[-(j+1):])
                # If this id is unique, use it and break the inner loop
                if submitter_id not in existing_ids:
                    df.loc[i, id_col] = submitter_id
                    existing_ids.add(submitter_id)
                    break
        return df
    # Generate unique submitter IDs
    generate_unique_submitter_ids(submission_df, 'bucket_path', 'submitter_id')
    # If a reference file node is not provided, drop the 'callset' column
    if reference_file_node is None:
        submission_df.drop('callset', axis=1, inplace=True)
    submission_df['ga4gh_drs_uri'] = 'drs://dg.4503:' + submission_df['object_id']
    submission_df['submitter_id'] = submission_df['submitter_id'].str.replace(' ', '_')
    return submission_df


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
    def add(g, gid, d):
        entry_as_dict = dict([d])
        g[gid] = dict([d])
        return g
    ref_file_data_by_project = reduce(group_by("project"), indexd_data_with_program_and_project, {})
    ref_file_data_by_project_program = reduce(group_by("program",
                                                       lambda gid, e: e[1][0][gid]),
                                              list(ref_file_data_by_project.items()), {})
    # todo: why am i getting only record per file?
    final_answer_so_far = dict(map(lambda p: (p[0], dict(p[1])), ref_file_data_by_project_program.items()))

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
