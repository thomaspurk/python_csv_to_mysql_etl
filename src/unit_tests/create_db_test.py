# Description: Unit tests for cteate_db
# Author: Thomas Purk
# Date: 2025-08-06

import pytest
import os
import mysql.connector
from src.etl_pipeline.tasks.create_db import invoke

# Load the local environment configuration first
# Some local packages depend on env settings
from dotenv import load_dotenv
load_dotenv(".env.development.local")

# ARRANGE (global)
test_db_name = "abcde_test"
create_sql = f"""
-- Drop the database if it exists
DROP DATABASE IF EXISTS {test_db_name};

CREATE DATABASE {test_db_name};

CREATE TABLE {test_db_name}.test_ab (
    a INT PRIMARY KEY,
    b VARCHAR(4)
);

CREATE TABLE {test_db_name}.test_cde (
    c VARCHAR(4),
    d VARCHAR(50),
    e INT PRIMARY KEY
);
"""

out_dir = "src/unit_tests/input_output/"

validate_sql = f"""
SELECT TABLE_NAME, COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{test_db_name}'
ORDER BY TABLE_NAME, ORDINAL_POSITION;
"""

DB_CONFIG = {
    'host':os.environ.get("DB_HOST"),
    'user':os.environ.get("DB_USER"),
    'password':os.environ.get("DB_PASSWORD"),
}

@pytest.mark.order(1) # Create the test database first
def test_invoke():
    """Should create a test database."""
    # ARRANGE
    input_db_config = DB_CONFIG
    input_script_file = out_dir + "temp.sql"
    input_output_folder = out_dir
    expected_results = "('test_ab', 'a')\n('test_ab', 'b')\n('test_cde', 'c')\n('test_cde', 'd')\n('test_cde', 'e')"

    # Create a file if it doesn't exist or overwrite it if it does.
    with open(input_script_file, "w") as file:
        file.write(create_sql)

    # ACT
    invoke(input_db_config, input_script_file, input_output_folder)

    with mysql.connector.connect(**input_db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(validate_sql)
            rows = cursor.fetchall()
            # Convert rows to a single string
            results = '\n'.join([str(row) for row in rows])

    # ASSERT
    assert results == expected_results