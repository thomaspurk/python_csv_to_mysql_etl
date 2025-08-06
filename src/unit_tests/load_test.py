# Description: Unit tests for load.py
# Author: Thomas Purk
# Date: 2025-08-06
import pytest
import pandas as pd
import os
import mysql.connector
from io import StringIO

from src.etl_pipeline.tasks import load

# Load the local environment configuration first
# Some local packages depend on env settings
from dotenv import load_dotenv
load_dotenv(".env.development.local")

# Create the test database first (see create_db_test.py)
# Then test loading the data
@pytest.mark.order(2) 
def test_invoke():
    """should insert new records into the database"""

    # ARRANGE
    mapping =[ 
        {
            "source_name": "src/unit_tests/input_output/test.csv",
            "format": "file",
            "type": "csv",
            "extract_tables": [
                {
                    "name": "extract_test_ab",
                     "target": {
                        "service": "DB_SERVICE",
                        "host": "DB_HOST",
                        "port": "DB_PORT",
                        "database": "TEST_DB_DATABASE",
                        "user": "DB_USER",
                        "password": "DB_PASSWORD",
                        "table": "test_ab"
                    },
                    "table_xforms": [
                        {
                            "function": "to_unique",
                            "args": []
                        }
                    ],
                    "columns": [
                        {
                            "source_column": "A",
                            "target_column": "a",
                            "column_xforms": [                       {
                                    "function": "to_int",
                                    "args": []
                                }]
                        },
                        {
                            "source_column": "B",
                            "target_column": "b",
                            "column_xforms": []
                        }
                    ]
                },
                {
                    "name": "extract_test_cde",
                    "target": {
                        "service": "DB_SERVICE",
                        "host": "DB_HOST",
                        "port": "DB_PORT",
                        "database": "TEST_DB_DATABASE",
                        "user": "DB_USER",
                        "password": "DB_PASSWORD",
                        "table": "test_cde"
                    },
                    "table_xforms": [
                        {
                            "function": "to_unique",
                            "args": []
                        }
                    ],
                    "columns": [
                        {
                            "source_column": "C",
                            "target_column": "c",
                            "column_xforms": []
                        },
                        {
                            "source_column": "D",
                            "target_column": "d",
                            "column_xforms": [                           
                                {
                                    "function": "to_date",
                                    "args": []
                                }]
                        },
                        {
                            "source_column": "E",
                            "target_column": "e",
                            "column_xforms": [                       {
                                    "function": "to_int",
                                    "args": []
                                }]
                        }
                    ]
                }
            ]
        }
    ]
    
    csv_ab = "A,B\n1,a\n2,b\n3,c\n"
    csv_cde = "C,D,E\nx,2025-08-01,5\ny,2025-03-01,6\nz,2025-10-01,7\n"

    test_db_name = "abcde_test"

    validate_ab_sql = f"""SELECT * FROM {test_db_name}.test_ab"""
    validate_cde_sql = f"""SELECT * FROM {test_db_name}.test_cde"""

    DB_CONFIG = {
        'host':os.environ.get("DB_HOST"),
        'user':os.environ.get("DB_USER"),
        'password':os.environ.get("DB_PASSWORD"),
    }

    input = {
        "extract_test_ab": pd.read_csv(StringIO(csv_ab)),
        "extract_test_cde": pd.read_csv(StringIO(csv_cde))
    }
    expected_results_ab = "(1, 'a')\n(2, 'b')\n(3, 'c')"
    expected_results_cde = "('x', '2025-08-01', 5)\n('y', '2025-03-01', 6)\n('z', '2025-10-01', 7)"

    # ACT
    load.invoke(mapping[0], input)
    with mysql.connector.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(validate_ab_sql)
            rows = cursor.fetchall()
            # Convert rows to a single string
            ab_results = '\n'.join([str(row) for row in rows])

            cursor.execute(validate_cde_sql)
            rows = cursor.fetchall()
            # Convert rows to a single string
            cde_results = '\n'.join([str(row) for row in rows])

    # ASSERT
    assert ab_results == expected_results_ab
    assert cde_results == expected_results_cde 