# Description: Functions to load the source data and begin laying out the initial structure. 
# Author: Thomas Purk
# Date: 2025-07-18
# TODO Add error trapping
# TODO Add Unit Testing
# TODO Add Logging

import pandas as pd
from pandas import DataFrame

def invoke (etl_mapping: dict) -> dict:
    """ Loads and gathers the data from the defined sources as a collection of Pandas DataFrames.

        Args:
            etl_mapping (dict): A set of properties defining data sources, extraction, transformations, and destinations.
        
        Returns:
            dict: A set of Pandas DataFrames loaded with data from the data sources.

    """

    # Load the data from source into a Pandas DataFrame
    # TODO: Add Loaders for other data source types
    if etl_mapping["format"] == "file":
        df_source = load_source_from_file(etl_mapping)

    # Extract the tables from the source
    # Track extracted tables
    extract_tables = {}

    for table in etl_mapping["extract_tables"]:
        # Create a new dataframe with only the relevant columns to the class of items tracked in the table
        columns = [ x["source_column"] for x in table["columns"]]
        df = df_source[columns]

        # Add to the tracking dictionary
        extract_tables[table["name"]] = df

    return extract_tables

def load_source_from_file(etl_mapping: dict) -> DataFrame:
    """ Reads the source specified in the etl_mapping configuration into a Pandas DataFrame
    
        Args:
            etl_mapping (dict): A set of properties defining data sources, extraction, transformations, and destinations.
        
        Returns:
            DataFrame: A Pandas DataFrame containing the data from the source.
        
    """
    df = pd.DataFrame()

    # TODO: Add Loaders for other data types
    if etl_mapping["type"] == "csv":

        # Load the Source CSV into a data frame
        SOURCE_DATA_FILE = etl_mapping['source_name']

            # Load the Source CSV into a data frame
        df = pd.read_csv(SOURCE_DATA_FILE)
    
    return df