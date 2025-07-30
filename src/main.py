# Description: The main entry point for code execution. 
# Author: Thomas Purk
# Date: 2025-07-18
# TODO Add error trapping
# TODO Add Unit Testing
# TODO Add Logging

# Load the local environment configuration first
# Some local packages depend on env settings
from dotenv import load_dotenv
load_dotenv(".env.development.local")

import json
import os
import etl_pipeline.tasks.extract as extract
import etl_pipeline.tasks.transform as transform
import etl_pipeline.tasks.load as load
import utils.create_db as create_db


# Create the MySQL Database - This is the destination to load data
# Get the path to the DDL file
DDL_SCRIPT_FILE = os.environ.get("DDL_SCRIPT_FILE") or ""

DB_CONFIG = {
    'host':os.environ.get("DB_HOST"),
    'user':os.environ.get("DB_USER"),
    'password':os.environ.get("DB_PASSWORD"),
}
create_db.invoke(DB_CONFIG, DDL_SCRIPT_FILE)

# Get the path to the mapping file
MAPPING_FILE = os.environ.get("MAPPING_FILE") or ""

# Load the ETL Mapping
with open(MAPPING_FILE, 'r') as f:
    etl_mappings = json.loads(f.read())

# Extract - Get a list of DataFrames from the Data Source
for etl_mapping in etl_mappings:
    # Get a list of tables
    ##### Extract ##########
    extract_tables: dict = extract.invoke(etl_mapping)

    ##### Transform ##########
    transform.invoke(etl_mapping, extract_tables)

    ##### Load ##########
    load.invoke(etl_mapping, extract_tables)



