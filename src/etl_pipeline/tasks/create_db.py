# Description:  Functions for creating databases
# Author: Thomas Purk
# Date: 2025-07-18
# Reference: https://www.w3schools.com/python/python_mysql_getstarted.asp
# TODO Add error trapping
# TODO Add Unit Testing
# TODO Add Logging/Messages

import json
import mysql.connector


def invoke(DB_CONFIG: dict, DDL_SCRIPT_FILE: str, output_folder: str = "output") -> None:
    """ Create the the database from the DDL file. Uses the mysql-connector-python package for demonstration. This package cannot execute multiple SQL statements at once andreturn individual cursor message. So the DDL is split into multipl statements.Alternatives, like sqlalchemy, could provide more features.

        Args:
            DB_CONFIG (dict): A set of named connection paramaters for the db server where the new database will be created.

            DDL_SCRIPT_FILE (str): The path to a ddl containing SQL Statement to create the database and schema. The name of the new database will be in this file, not hte DB_CONFIG dict.

            output_folder: (str): The folder to save metadata files.

    """

    # Load the DDL script
    with open(DDL_SCRIPT_FILE, 'r') as f:
        ddl_script = f.read()

    # Connect to database with context manager and auto closing
    with mysql.connector.connect(**DB_CONFIG) as conn:
           
        # Create a cursor with contect manager and auto closing
        with conn.cursor() as cursor:
            
            database_name = ""
            # Execute the DDL (multi statement)
            for statement in ddl_script.split(";"):
                statement = statement.strip()
                database_name = extract_database_name(statement, database_name)
                cursor.execute(statement)

                warnings = cursor.warnings
                if warnings:
                    for warning in warnings:
                        print(f"    Warning: {warning}")
    
    # Update the mapping skeleton to capture any schema changes.
    DB_CONFIG["database"] = database_name
    create_mapping_skeleton(DB_CONFIG, f"{output_folder}/mapping_skeleton.json")
        
def extract_database_name(statement: str, database_name: str) -> str:
    """ Get the database name from the "CREATE DATABASE" statement. Updates the database_name parameter if applicable.

        Args:
            statement (str): The string to investigate and extract the database name if possible.
            database_name (srt): Updates the database name, usually from '', to the real name if possible
        
        Returns:
            str: The name of the database, including an update.
    """
    
    if "create database" in statement.lower():
        lines = statement.split("\n")
        for line in lines:
            if not line.startswith("--"):
                database_name = line.lower().replace("create database", "").replace("if not exists","").strip()
    
    return database_name

def create_mapping_skeleton(DB_CONFIG: dict, output_file: str) -> None:
    """ Creates the rough outline of an ETL mapping configuration that has the destination side of the ETL filled in. Makes it easier than typing up the config from scratch.

        Args:
            DB_CONFIG (dict): Database connection parameters.
            output_file (str): Path to save a file that contains a JSON representation the the skeleton mapping.
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
    with open(output_file, 'w') as f:
        json.dump(ms_dict, f, indent=4)

