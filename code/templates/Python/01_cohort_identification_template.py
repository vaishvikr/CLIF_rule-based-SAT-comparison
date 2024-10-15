# Load required libraries
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
import pyarrow.parquet as pq
import time
from utils import config

# Add the parent directory of the current script to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Use the imported config
# Access configuration parameters
site_name = config['site_name']
tables_path = config['tables_path']
file_type = config['file_type']


# Print the configuration parameters
print(f"Site Name: {site_name}")
print(f"Tables Path: {tables_path}")
print(f"File Type: {file_type}")

# Your cohort identification code here
# Cohort identification script for inpatient admissions
# Objective: identify a cohort of hospitalizations from CLIF tables
# Identify patients admitted to the hospital in a given date range. 
# Export a list of `hospitalization_id` and filtered CLIF tables for the 
# identified hospitalizations.
# An example project for this cohort would be included for surveillance of 
# sepsis events based on the CDC Adult Sepsis Event criteria.

# Specify inpatient cohort parameters

## Date range
start_date = "2020-01-01"
end_date = "2021-12-31"

## Confirm that these are the correct paths
adt_filepath = f"{tables_path}/clif_adt.{file_type}"
hospitalization_filepath = f"{tables_path}/clif_hospitalization.{file_type}"
vitals_filepath = f"{tables_path}/clif_vitals.{file_type}"
labs_filepath = f"{tables_path}/clif_labs.{file_type}"
meds_filepath = f"{tables_path}/clif_medication_admin_continuous.{file_type}"
resp_support_filepath = f"{tables_path}/clif_respiratory_support.{file_type}"


def read_data(filepath, filetype):
    """
    Read data from file based on file type.
    Parameters:
        filepath (str): Path to the file.
        filetype (str): Type of the file ('csv' or 'parquet').
    Returns:
        DataFrame: DataFrame containing the data.
    """
    start_time = time.time()  # Record the start time
    file_name = os.path.basename(filepath) 
    if filetype == 'csv':
        df = pd.read_csv(filepath)
    elif filetype == 'parquet':
        table = pq.read_table(filepath)
        df = table.to_pandas()
    else:
        raise ValueError("Unsupported file type. Please provide either 'csv' or 'parquet'.")
    
    end_time = time.time()  # Record the end time
    load_time = end_time - start_time  # Calculate the loading time
    
    # Calculate the size of the loaded dataset in MB
    dataset_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    print(f"File name: {file_name}")
    print(f"Time taken to load the dataset: {load_time:.2f} seconds")
    print(f"Size of the loaded dataset: {dataset_size_mb:.2f} MB\n")
    
    return df


clif_adt = read_data(adt_filepath, file_type)
clif_hospitalization = read_data(hospitalization_filepath, file_type)
clif_vitals = read_data(vitals_filepath, file_type)
clif_labs = read_data(labs_filepath, file_type)
clif_medication_admin_continuous = read_data(meds_filepath, file_type)
clif_respiratory_support = read_data(resp_support_filepath, file_type)


# Ensure datetime format is correct
clif_hospitalization['admission_dttm'] = pd.to_datetime(clif_hospitalization['admission_dttm'])

# Step 1: Filter admissions between March 1, 2020 and March 31, 2022
admissions_filtered = clif_hospitalization[
    (clif_hospitalization['admission_dttm'] >= start_date) & 
    (clif_hospitalization['admission_dttm'] <= end_date)
]

# Filter for adults (age >= 18)
cohort = admissions_filtered[admissions_filtered['age_at_admission'] >= 18]
cohort = cohort[['hospitalization_id']].drop_duplicates()

### Apply other inclusion and exclusion criteria ....

### Export the cohort
