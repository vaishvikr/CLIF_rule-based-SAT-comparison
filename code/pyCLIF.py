import json
import os
import duckdb
import pandas as pd

def load_config():
    json_path = '../config/config.json'
    
    with open(json_path, 'r') as file:
        config = json.load(file)
    print("Loaded configuration from config.json")
    
    return config

def load_data(table,sample_size=None):
    """
    Load the patient data from a file in the specified directory.

    Returns:
        pd.DataFrame: DataFrame containing patient data.
    """
    # Determine the file path based on the directory and filetype
    file_path = helper['tables_path'] + table + '.' + helper['file_type']
    
    # Load the data based on filetype
    if os.path.exists(file_path):
        if helper['file_type'] == 'csv':
            df = duckdb.read_csv(file_path,sample_size=sample_size).df()
        elif helper['file_type'] == 'parquet':
            df = duckdb.read_parquet(file_path).df()
        else:
            raise ValueError("Unsupported filetype. Only 'csv' and 'parquet' are supported.")
        print(f"Data loaded successfully from {file_path}")
        return df
    else:
        raise FileNotFoundError(f"The file {file_path} does not exist in the specified directory.")
    
def deftime(df):
    
    # Count entries with both hours and minutes
    has_hr_min = df.notna() & (df.dt.hour.notna() & df.dt.minute.notna())
    count_with_hr_min = has_hr_min.sum()

    # Count entries without hours and minutes
    count_without_hr_min = (~has_hr_min).sum()

    # Print the results
    print(f"Count with hours and minutes: {count_with_hr_min}")
    print(f"Count without hours and minutes: {count_without_hr_min}")

def getdttm(df):
    return pd.to_datetime(df).dt.ceil('min')

helper = load_config()
print(helper)