import os

import requests
import csv
import json
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index


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


def create_ref_file_node(indexd_data):
    # ask user services about
    # can we get away with data_type always being "Clinical Data",
    # data_category being "Other" and data_format being the file extension? what about submitter_id? (
    # data type, data category, data format, submitter id
    # Format the timestamp
    print(indexd_data)
    return {
        "data_category": "Clinical Data",  # ?
        "data_format": "XML",  # ?
        "data_type": "Unharmonized Clinical Data",  # ?
        "file_name": indexd_data["file_name"],
        "file_size": indexd_data["size"],
        "md5sum": indexd_data["hashes"]["md5"],
        "submitter_id": "no idea",  # ?
        "type": "reference_file"
    }


def test_ref_to_tsv():
    cred_path = os.environ.get("PP_CREDS")
    auth = Gen3Auth(refresh_file=cred_path)
    index = Gen3Index(auth_provider=auth)

    # 'dg.4503/e4581ff0-5a8e-43cd-a3fb-13893e0e2d61'
    dest_data = tsv_to_json("tsv/dest-bucket-manifest.tsv")
    ref_file_data = tsv_to_json("tsv/release_manifest_release-27.tsv")
    filter_by_field = lambda field_name, dictionary: list(filter(lambda d: d.get(field_name), dictionary))
    map_dict_to_field = lambda field_name, dictionary: dict(map(lambda d: (d[field_name], d), dictionary))
    workable_ref_fields = map_dict_to_field("study_with_consent", filter_by_field("study_with_consent", ref_file_data))
    study_set = set(map(lambda d: d["acl"], dest_data))

    def x(ss):
        def y(t):
            return t[0] in ss

        return y

    fields_to_make_nodes_for = dict(filter(x(study_set), list(workable_ref_fields.items())))
    guids_for_fields = list(map(lambda d: d["guid"], fields_to_make_nodes_for.values()))
    acl_set = set(fields_to_make_nodes_for.keys())
    reference_file_data_from_indexd = index.get_records(guids_for_fields)
    # for acl in acl_set:
        # params = {"study_with_consent": acl}
        # a = index.get_with_params(params)
        # reference_file_data_from_indexd.append(a)

    output = list(map(create_ref_file_node, reference_file_data_from_indexd))



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
