# Description: Helper functions to create the superstore database from scratch.
# Author: Thomas Purk
# Date: 2025-07-18
# Reference: https://www.w3schools.com/python/python_mysql_getstarted.asp

import os
import mysql.connector
import utils.create_mapping_skeleton as create_mapping_skeleton

# TODO Move this to ENV file
DDL_SCRIPT_FILE = 'src/ddl/superstore_schema.sql'

DB_CONFIG = {
    'host':os.environ.get("DB_HOST"),
    'user':os.environ.get("DB_USER"),
    'password':os.environ.get("DB_PASSWORD"),
}

def invoke() -> None:
    """ TODO
    """
    print("\n====== Creating New Database =========")

    # Load the DDL script
    with open(DDL_SCRIPT_FILE, 'r') as f:
        ddl_script = f.read()

    # Connect to database with context manager and auto closing
    with mysql.connector.connect(**DB_CONFIG) as conn:
           
        # Create a cursor with contect manager and auto closing
        with conn.cursor() as cursor:
            
            # Execute the DDL (multi statement)
            for statement in ddl_script.split(";"):
                statement = statement.strip()
                cursor.execute(statement)
                # Print messages  
                print(f"\n\nExecuted statement: {cursor.statement}")
                print(f"    Rows affected: {cursor.rowcount}")

                warnings = cursor.fetchwarnings()
                if warnings:
                    for warning in warnings:
                        print(f"    Warning: {warning}")
    
    # Update the mapping skeleton to capture any schema changes.
    create_mapping_skeleton.invoke()
        



