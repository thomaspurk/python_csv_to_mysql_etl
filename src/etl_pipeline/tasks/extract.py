import pandas as pd
from pandas import DataFrame

def invoke (etl_mapping: dict) -> dict:
    """
        TODO: Fill out this docstring
        TODO: Add Error Handling
        TODO: Add Loaders for other data source types
    """

    # Load the data from source into a Pandas DataFrame
    if etl_mapping["format"] == "file":
        df_source = load_source_from_file(etl_mapping)

    # Extract the tables from the source
    # Track extracted tables
    extract_tables = {}

    for table in etl_mapping["extract_tables"]:
        # Create a new dataframe with only the relevant columns to the class of items tracked in the table
        columns = [ x["source_column"] for x in table["columns"]]
        df = df_source[columns]
        
        # Remove duplicate rows
        #df = df.drop_duplicates()

        # Track
        extract_tables[table["name"]] = df
        #df.to_sql(table['target_table'], con=engine, if_exists='append', index=False)

    return extract_tables

def load_source_from_file(etl_mapping: dict) -> DataFrame:
    """
        TODO: Fill out this docstring
        TODO: Add Error Handling
        TODO: Add Loaders for other file types
    """
    df = pd.DataFrame()

    if etl_mapping["type"] == "csv":

        # Load the Source CSV into a data frame
        SOURCE_DATA_FILE = etl_mapping['source_name']

            # Load the Source CSV into a data frame
        df = pd.read_csv(SOURCE_DATA_FILE)
    
    return df