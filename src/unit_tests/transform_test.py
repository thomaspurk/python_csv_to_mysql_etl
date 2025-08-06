# Description: Unit tests for transform.py
# Author: Thomas Purk
# Date: 2025-08-06
# TODO Write a unit test fo the invoke function

import pandas as pd
from pandas import DataFrame
from io import StringIO

from src.etl_pipeline.tasks.transform import invoke, run_column_transformations, run_table_transformations, to_date, to_float, to_int, to_text, to_unique

# ARRANGE (global)
mapping =[ 
    {
        "source_name": "src/unit_tests/input_output/test.csv",
        "format": "file",
        "type": "csv",
        "extract_tables": [
            {
                "name": "extract_test_ab",
                "table_xforms": [
                    {
                        "function": "to_unique",
                        "args": []
                    }
                ],
                "columns": [
                    {
                        "source_column": "A",
                        "target_column": "value_a",
                        "column_xforms": [                       {
                                "function": "to_int",
                                "args": []
                            }]
                    },
                    {
                        "source_column": "B",
                        "target_column": "value_b",
                        "column_xforms": []
                    }
                ]
            },
            {
                "name": "extract_test_cde",
                "table_xforms": [
                    {
                        "function": "to_unique",
                        "args": []
                    }
                ],
                "columns": [
                    {
                        "source_column": "C",
                        "target_column": "value_c",
                        "column_xforms": []
                    },
                    {
                        "source_column": "D",
                        "target_column": "value_d",
                        "column_xforms": [                           
                            {
                                "function": "to_date",
                                "args": []
                            }]
                    },
                    {
                        "source_column": "E",
                        "target_column": "value_e",
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

csv_full = "A,B,C,D,E\n1,a,x,2025-08-01,5\n2,b,yb,2025-03-01,6\n3,c,z,2025-10-01,7\n1,a,x,2025-08-01,5\n2,b,yb,2025-03-01,6\n3,c,z,2025-10-01,7\n"
csv_full_unique = "A,B,C,D,E\n1,a,x,2025-08-01,5\n2,b,yb,2025-03-01,6\n3,c,z,2025-10-01,7\n"
csv_ab = "A,B\n1,a\n2,b\n3,c\n"
csv_cde = "C,D,E\nx,2025-08-01,5\ny,2025-03-01,6\nz,2025-10-01,7\n"

def test_to_unique():
    """should remove duplicates from the dataframe"""

    # ARRANGE
    dup_df = pd.read_csv(StringIO(csv_full))
    expected_results = csv_full_unique

    # ACT
    to_unique(dup_df)
    results = dup_df.to_csv(index=False)

    # ASSERT
    assert results == expected_results

def test_to_int():
    """should a data frame column to int dataframe"""

    # ARRANGE
    df = pd.read_csv(StringIO(csv_full), dtype={"A":str, "B":str, "C":str, "D":str, "E":str})
    expected_results = "int64"

    # ACT
    to_int(df,'A')
    results = str(df["A"].dtype)
    
    # ASSERT
    assert results == expected_results

def test_to_text():
    """should a data frame column to int dataframe"""

    # ARRANGE
    df = pd.read_csv(StringIO(csv_full), dtype={"A":str, "B":str, "C":str, "D":str, "E":str})
    expected_results = "object"

    # ACT
    to_text(df,'A')
    results = str(df["A"].dtype)
    
    # ASSERT
    assert results == expected_results

def test_to_float():
    """should a data frame column to int dataframe"""

    # ARRANGE
    df = pd.read_csv(StringIO(csv_full), dtype={"A":str, "B":str, "C":str, "D":str, "E":str})
    expected_results = "float64"

    # ACT
    to_float(df,'A')
    results = str(df["A"].dtype)
    
    # ASSERT
    assert results == expected_results

def test_to_date():
    """should a data frame column to int dataframe"""

    # ARRANGE
    data = csv_cde
    df = pd.read_csv(StringIO(data), dtype={"A":str, "B":str, "C":str, "D":str, "E":str})
    expected_results = "datetime64[ns]"

    # ACT
    to_date(df,"D")
    results = str(df["D"].dtype)
    
    # ASSERT
    assert results == expected_results

def test_to_run_table_transformations():
    """should transform all columns based on the configured transformations"""

    # ARRANGE
    transformations = [
                    {
                        "function": "to_unique",
                        "args": []
                    }
                ]
    dup_df = pd.read_csv(StringIO(csv_full))
    expected_results = csv_full_unique

    # ACT
    run_table_transformations(dup_df, transformations)
    results = dup_df.to_csv(index=False)

    # ASSERT
    assert results == expected_results

def test_run_column_transformations():
    """should transform a table based on the configured transformations"""

    # ARRANGE
    transformations = mapping[0]["extract_tables"][0]["columns"][0]["column_xforms"]
    df = pd.read_csv(StringIO(csv_ab))
    expected_results = "int64"

    # ACT
    run_column_transformations(df, "A", transformations)
    results = str(df["A"].dtype)
    
    # ASSERT
    assert results == expected_results