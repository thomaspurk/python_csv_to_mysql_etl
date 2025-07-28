# Description: The main entry point for code execution. 
# Author: Thomas Purk
# Date: 2025-07-18

# Load the local environment configuration first
# Some local packages depend on env settings
from dotenv import load_dotenv
load_dotenv(".env.development.local")

import json
import etl_pipeline.tasks.extract as extract
import etl_pipeline.tasks.transform as transform
import etl_pipeline.tasks.load as load
import utils.create_superstore_db as create_superstore_db


# Create the MySQL Database - This is the destination to load data
create_superstore_db.invoke()

# TODO Move this to the ENV File
MAPPING_FILE = 'src/etl_pipeline/mapping.json'

##### PSTEP 1 - LOAD ##########
# Load the ETL Mapping
with open(MAPPING_FILE, 'r') as f:
    etl_mappings = json.loads(f.read())

# Extract - Get a list of DataFrames from the Data Source
for etl_mapping in etl_mappings:
    # Get a list of tables
    extract_tables: dict = extract.invoke(etl_mapping)
    transform.invoke(etl_mapping, extract_tables)
    load.invoke(etl_mapping, extract_tables)



