# Description: Migrate from source to target.
# Author: Thomas Purk
# Date: 2025-07-18


import os
import mysql.connector
import pandas as pd
from datetime import datetime
import json
from sqlalchemy import create_engine

DB_CONFIG = {
    'host':os.environ.get("DB_HOST"),
    'user':os.environ.get("DB_USER"),
    'password':os.environ.get("DB_PASSWORD"),
}

engine = create_engine(f"mysql+pymysql://{os.environ.get("DB_USER")}:{os.environ.get("DB_PASSWORD")}@{os.environ.get("DB_HOST")}/superstore")




DB_CONFIG = {
    'host':os.environ.get("DB_HOST"),
    'user':os.environ.get("DB_USER"),
    'password':os.environ.get("DB_PASSWORD"),
}

# # Pipeline function
# def run() -> None:

#     ##### PIPELINE STEP 1 - LOAD ##########
#     # Load the ETL Mapping
#     with open(MAPPING_FILE, 'r') as f:
#         etl_mapping = json.loads(f.read())
    
#     # Load the Source CSV into a data frame
#     df_source = pd.read_csv(SOURCE_DATA_FILE);

#     ##### PIPELINE STEP 2 - EXTRACT ##########
#     # Extract the source data into one or more tables
#     # The source may may need to be split into multiple tables

#     # Track extracted tables
#     extract_tables = {}

#     for table in etl_mapping["extract_tables"]:
#         # Create a new dataframe with only the relevant columns to the class of items tracked in the table
#         df = df_source[table["fields"]]
        
#         # Remove duplicate rows
#         df = df.drop_duplicates()

#         # Track
#         extract_tables[table["name"]] = df
#         #df.to_sql(table['target_table'], con=engine, if_exists='append', index=False)

#     print(f"Source Info: {df_source.info()}")