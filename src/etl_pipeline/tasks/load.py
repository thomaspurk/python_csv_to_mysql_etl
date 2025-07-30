# Description: Functions to load the transformed data into the destination. This file uses sqlalchemy to demonstrate this task.
# Author: Thomas Purk
# Date: 2025-07-18
# TODO Add error trapping
# TODO Add Unit Testing
# TODO Add Logging

import os
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine

def invoke (etl_mapping: dict, extract_tables: dict) -> None:
    """ Loads and gathers the data from the defined sources as a collection of Pandas DataFrames.

        Args:
            etl_mapping (dict): A set of properties defining data sources, extraction, transformations, and destinations.
            
            etract_tables (dict): A set of Pandas DataFrames containing the extracted and transformed data reference by the table name as a key.
    """
   
    # Iterate over each DataFrame in the set
    for table_name in extract_tables.keys():

        # Loop the list of tables in the ETL mapping config to find the matching configuration
        for config_table in etl_mapping["extract_tables"]:
            if config_table["name"] == table_name:
                
                # Get the destination connection information
                service = os.environ.get(config_table["target"]["service"])
                host = os.environ.get(config_table["target"]["host"])
                port = os.environ.get(config_table["target"]["port"])
                database = os.environ.get(config_table["target"]["database"])
                service = os.environ.get(config_table["target"]["service"])
                user = os.environ.get(config_table["target"]["user"])
                password = os.environ.get(config_table["target"]["password"])
                table = config_table["target"]["table"]

                # Create the sqlalchemy connection string
                connection_string = f"{service}://{user}:{password}@{host}/{database}"
                engine = create_engine(url=connection_string)

                # Move the data form the DataFrame to the destination
                df: DataFrame = extract_tables[table_name]
                df.to_sql(
                    name=table,  # type: ignore
                    con=engine, 
                    if_exists='append', 
                    index=False
                    )
                engine.dispose()