import json

import pandas as pd
import os

def ensure_dataframe(input_data):
    """
    Helper function to ensure the input data is a pandas DataFrame.
    If a string is given, it is treated as a file path. The function then reads the file,
    automatically detecting if it is a TSV or CSV based on the file extension.
    """
    # Check if input_data is a string (potentially a file path)
    if isinstance(input_data, str):
        try:
            # Determine the file extension and set the appropriate sep
            _, file_extension = os.path.splitext(input_data)
            if file_extension.lower() == '.tsv':
                sep = '\t'
            elif file_extension.lower() == '.csv':
                sep = ','
            else:
                print("Unsupported file type. Please provide a TSV or CSV file.")
                return None
            # Read the file with the correct determined sep
            input_data = pd.read_csv(input_data, sep=sep)
        except Exception as e:
            print(f"Error in reading file: {e}")
            return None
    # If input_data is already a DataFrame, do nothing
    elif isinstance(input_data, pd.DataFrame):
        pass
    else:
        print("Input data is neither a file path nor a DataFrame.")
        return None
    return input_data


def map_and_fillna(submission_df, reference_file_node, mapping_column, fill_with_column, fillna_value):
    """
    Helper function to map values from the reference file node to the submission dataframe based on a given column,
    and fill NaN values with a given value.
    """
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


def get_extension(filename):
    """
    Helper function to get the file extension from a filename.
    If the filename has a compressed extension (e.g., .gz), the function recursively calls itself
    until it gets the actual file extension.
    """
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


def generate_unique_submitter_ids_v2(bucket_url):
    path_components = bucket_url[5:].split("/")
    reversed_components = path_components[::-1]
    outcome = '_'.join(reversed_components)
    existing_ids = set()
    submitter_id = None
    blah = list(range(len(path_components)))[::-1]
    for component_index in blah:
        # ['ACTIV4-10-PKimlab-BioData_Catalyst-2023-to_upload-2024updates.xlsx']
        thing = path_components[component_index:]
        submitter_id = '_'.join(thing)
    return submitter_id


def generate_unique_submitter_ids(df, url_col, id_col):
    """
        Helper function to generate unique submitter IDs based on the bucket path.
        """
    # Initialize a set to store existing submitter IDs
    existing_ids = set()
    submitter_ids = []
    for i, row in df.iterrows():
        # Remove 's3://' and split the path into components
        bucket_value = row[url_col]
        path_components = bucket_value[5:].split('/')
        submitter_ids.append(i)
        for j in range(len(path_components)):
            # Create a potential submitter ID by joining the relevant path components
            reverse_index = -(j+1)
            path_component_at_reverse_index = path_components[reverse_index:]
            submitter_id = '_'.join(path_component_at_reverse_index)
            # If this id is unique, use it and break the inner loop
            if submitter_id not in existing_ids:
                df.loc[i, id_col] = submitter_id
                existing_ids.add(submitter_id)
                submitter_ids.append(submitter_id)
                break
    return df, existing_ids, submitter_ids


def prog_projcons_partsplit(ppc):
    parts = ppc.split('-', 1)
    return parts[0], parts[1]


def read_json(file_location):
    with open(file_location, 'r') as file:
        data = json.load(file)
    return data


def create_reference_file_node(ppc, dbgap_ascnum, manifest_location, node_location=None):
    # what is a guid, exactly?
    # a guid is a unique id for a file
    # what is a dbgap accession number?
    # all the full dots of the phs id?
    """
    This function creates a reference file node from the given project, dbgap accession number, NHLBI manifest,
    and optional reference file node.
    Arguments:
    self: Instance of the class
    ppc: Program, Project and Consent text string delimited by a "-" and "_" respectively. (ex. BioLINCC-MESA_HMB)
    dbgap_ascnum: dbGaP accession number (e.g., phs001234.v5.p1)
    manifest_location: Path to the NHLBI manifest file or a pandas DataFrame
    node_location: Optional path to the reference file node or a pandas DataFrame (default: None_
    Returns:
    submission_df: Pandas DataFrame containing the reference file node data

    TODO: Some columns DNE for every project and should be dropped or not called if they DNE to avoid errors.
        For example, 'callset' is usually unique to topmed studies, but is fully integrated into this function.
        The function should be changed to dynamically check whether a column exists in 'node_location'
        and append it with the existing data. Otherwise, only the required fields should be affixed.
    """
    # If a reference file node is not provided, warn the user that default values will be used
    if node_location is None:
        print("""
        NOTE: The 'node_location' argument is not defined. 
        The existing 'reference_file' node values from Gen3 will not map based on matching md5sum values in the NHLBI manifest. 
        To match these values, include the 'node_location' function argument.
        The function will apply these default values:
        1. 'callset' will be dropped as a column.
        2. 'file_type' = 'Other'
        3. 'data_category' = 'Clinical Data'
        4. 'data_type' = 'Other'
        """)
    # Ensure the NHLBI manifest and reference file node are dataframes
    nhlbi_manifest = ensure_dataframe(manifest_location)
    reference_file_node = pd.DataFrame(read_json(node_location), index=[0])
    assert nhlbi_manifest is not None
    # assert reference_file_node is not None
    # Check for errors in reading the files and return None if there are any
    if isinstance(nhlbi_manifest, str):
        print("Error reading nhlbi_manifest")
        return None
    if isinstance(reference_file_node, str):
        print("Error reading reference_file_node")
        return None
    # Split the dbGaP accession number into its constituent parts
    dbgap_phs, dbgap_version, dbgap_participant_set, dbgap_consent = dbgap_ascnum.split(sep=".")
    # Split the ppc into program and proj_cons
    program, proj_cons = prog_projcons_partsplit(ppc)
    # Initialize a dataframe for the submission data
    submission_df = pd.DataFrame(
        columns=['type', 'submitter_id', 'projects.code', 'object_id', 'ga4gh_drs_uri', 'file_name', 'md5sum',
                 'file_size', 'bucket_path', 'callset', 'file_type', 'data_category', 'data_format', 'file_format',
                 'data_type', 'file_md5sum', 'study_version'])
    # Assign values to the submission dataframe from the NHLBI manifest
    submission_df['md5sum'] = nhlbi_manifest['md5sum'].astype(str)
    submission_df['type'] = 'reference_file'
    submission_df['projects.code'] = proj_cons
    submission_df['object_id'] = nhlbi_manifest['guid'].astype(str)
    submission_df['ga4gh_drs_uri'] = nhlbi_manifest['ga4gh_drs_uri'].astype(str)
    submission_df['file_name'] = nhlbi_manifest['file_name'].astype(str)
    submission_df['file_size'] = nhlbi_manifest['s3_file_size'].astype(str)
    submission_df['bucket_path'] = nhlbi_manifest['s3_path'].astype(str)

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
        map_and_fillna(submission_df, reference_file_node, 'md5sum', 'file_type', 'Other')
        map_and_fillna(submission_df, reference_file_node, 'md5sum', 'data_category', 'Clinical Data')
        map_and_fillna(submission_df, reference_file_node, 'md5sum', 'data_type', 'Other')
    # Reindex the submission dataframe to match the original column order
    submission_df = submission_df.reindex(columns=original_columns_order)
    # Apply the get_extension function to get the file format and data format
    submission_df['data_format'] = submission_df['file_name'].apply(get_extension)
    submission_df['file_format'] = submission_df['file_name'].apply(get_extension)
    submission_df['file_md5sum'] = submission_df['md5sum']
    submission_df['study_version'] = ''.join([char for char in dbgap_version if char.isdigit()])
    # Generate unique submitter IDs
    bucket_url = 's3://nih-nhlbi-biodata-catalyst-phs002694-v4/ACTIV4a-Mechanistic-Studies/10-Kim-ACTIV4-BEACONS-COVID-19-Comprehensive-Biomarker-Analysis-for-Prediction-of-clinical-course/ACTIV4-10-PKimlab-BioData_Catalyst-2023-to_upload-2024updates.xlsx''s3://nih-nhlbi-biodata-catalyst-phs002694-v4/ACTIV4a-Mechanistic-Studies/10-Kim-ACTIV4-BEACONS-COVID-19-Comprehensive-Biomarker-Analysis-for-Prediction-of-clinical-course/ACTIV4-10-PKimlab-BioData_Catalyst-2023-to_upload-2024updates.xlsx'
    outcome = generate_unique_submitter_ids_v2(list(submission_df.iterrows())[0][1]["bucket_path"])
    outcome2 = generate_unique_submitter_ids(submission_df, 'bucket_path', 'submitter_id')
    # todo: if need to make a new ref file node, use the submission df
    # swap out guids if existing df

    # If a reference file node is not provided, drop the 'callset' column
    if reference_file_node is None:
        submission_df.drop('callset', axis=1, inplace=True)
    submission_df['ga4gh_drs_uri'] = 'drs://dg.4503:' + submission_df['object_id']
    submission_df['submitter_id'] = submission_df['submitter_id'].str.replace(' ', '_')
    return submission_df
