# Description: Functions to transform the extracted data.
# Author: Thomas Purk
# Date: 2025-07-18
# TODO Add error trapping
# TODO Add Unit Testing
# TODO Add Logging

import pandas as pd
from pandas import DataFrame

def invoke (etl_mapping: dict, extract_tables: dict) -> None:
    """ Executes the Transformation instructions contained in the etl_mapping configuration.

        Args:
        etl_mapping (dict): A set of properties defining data sources, extraction, transformations, and destinations.
        
        etract_tables (dict): A set of Pandas DataFrames containing the extracted and transformed data reference by the table name as a key.

    """

    # Iterate over each DataFrame in the set
    for table_name in extract_tables.keys():

        # Loop the list of tables in the ETL mapping config to find the matching configuration
        for table in etl_mapping["extract_tables"]:
            if table["name"] == table_name:
                # Table Transformations
                run_table_transformations(
                    df=extract_tables[table_name],
                    transformations=table["table_xforms"]
                )
                # Build a column re-naming dict
                column_rename = {}
                for column in table["columns"]:
                    column_rename[column["source_column"]] = column["target_column"]
                    # Columns Transformation
                    run_column_transformations(
                        df=extract_tables[table_name],
                        column_name=column["source_column"],
                        transformations=column["column_xforms"]
                    )
                
                # Complete the column rename task
                extract_tables[table_name].rename(columns=column_rename, inplace=True)

def run_table_transformations(df: DataFrame, transformations: list) -> None:
    """ Execute Columns Transformation Functions.

        Args:
            df (DataFrame): The data to transform

            transforamtions (list): The changes to make.
    """
    # Iterate over the list of tranformations
    for xform in transformations:

        # Get the function that the transformation is specifying
        xform_function = globals()[xform["function"]]

        xform_function(df)

def run_column_transformations( df: DataFrame, column_name: str, transformations: list):
    """ Execute Columns Transformation Functions.

        Args:
            df (DataFrame): The data to transform.
            column_name (str): The name of the column to update.
            transforamtions (list): The changes to make.
    """
    # Iterate over the list of tranformations
    for xform in transformations:
        # Get the function that the transformation is specifying
        xform_function = globals()[xform["function"]]
        # Get the value to value mapping that is specified
        #xform_mapping = xform.get("mapping", None) # TODO Not yet implemented
        # Execute
        xform_function(df,column_name, *xform["args"])

def to_unique(df: DataFrame):
    """ Table transformation to remove duplicate records.
    """
    df.drop_duplicates(inplace=True)

def to_int(df: DataFrame, column_name: str, *args):
    """ Column transformation to convert data into an integer
    TODO - Need to error trap for when data cannot be converted to an int.
    """

    df[column_name] = df[column_name].astype(int)

def to_text(df: DataFrame, column_name: str, *args):
    """ Column transformation to convert data into a string
    """

    df[column_name] = df[column_name].astype(str)

def to_date(df: DataFrame, column_name: str, *args):
    """ Column transformation to convert data into a data
    TODO - Need to error trap for when data cannot be converted to a datetime type.
    """

    df[column_name] = pd.to_datetime(df[column_name])

def to_decimal(df: DataFrame, column_name: str, *args):
    """ Column transformation to convert data into a decimal number. 
    TODO - Need to error trap for when data cannot be converted to a decimal number.
    """
    
    df[column_name] = df[column_name].astype(float)

def append_value(df: DataFrame, column_name: str, *args):
    """ Concatenate 2 or more str columns

        Args:
            df (DataFrame): The data to update
            column_name (str): The column to update
            args (tuple): (A list of columns, The join string i.e. ' - ')
    """

    df[column_name] = df[[column_name, args[0]]].astype(str).agg(args[1].join, axis=1)
       
def drop_column(df: DataFrame, column_name: str, *args):
    """ Remove columns from the DataFrame
    """

    df.drop(columns=args[0], inplace=True)              
