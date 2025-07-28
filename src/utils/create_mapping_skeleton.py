# Description: Helper functions to create the superstore database from scratch.
# Author: Thomas Purk
# Date: 2025-07-18
# Reference: https://www.w3schools.com/python/python_mysql_getstarted.asp

import os
import json
import mysql.connector

DB_CONFIG = {
    'host':os.environ.get("DB_HOST"),
    'user':os.environ.get("DB_USER"),
    'password':os.environ.get("DB_PASSWORD"),
    'database':os.environ.get("DB_DATABASE")
}

def invoke() -> None:
    """ TODO
    """

    ms_dict = {
        "source_name":"",
        "format":"",
        "type":"",
        "extract_tables":[]
    }
  
    # Connect to database with context manager and auto closing
    with mysql.connector.connect(**DB_CONFIG) as conn:
           
        # Create a cursor with contect manager and auto closing
        with conn.cursor() as cursor:

            # Get All Tables
            cursor.execute("SHOW TABLES;")
            all_tables = cursor.fetchall()
            
            
            for (table_name, ) in all_tables:

                table_dict = {
                    "name": "extract_" + str(table_name),
                    "target": {
                        "service": "DB_SERVICE",
                        "host": "DB_HOST",
                        "port": "DB_PORT",
                        "database": "DB_DATABASE",
                        "user": "DB_USER",
                        "password": "DB_PASSWORD",
                        "table": ""
                    },
                    "table_xforms": [
                        {
                            "function": "to_unique"
                        }
                    ],
                    "columns":[]
                }
                # Get the columns of each table
                cursor.execute(f"DESCRIBE {table_name}")
                columns = cursor.fetchall()
                for column in columns:
                    table_dict['columns'].append({
                        "source_column": "",
                        "target_column": str(tuple(column)[0]), # ignore type
                        "column_xforms": []
                    })
                
                ms_dict["extract_tables"].append(table_dict)

    # Write to a JSON file
    with open('output/mapping_skeleton.json', 'w') as f:
        json.dump(ms_dict, f, indent=4)