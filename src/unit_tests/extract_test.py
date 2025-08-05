import pandas as pd
from pandas import DataFrame
import json

from src.etl_pipeline.tasks.extract import invoke, load_source_from_file

# ARRANGE (global)
mapping =[ 
    {
        "source_name": "src/unit_tests/input_output/test.csv",
        "format": "file",
        "type": "csv",
        "extract_tables": [
            {
                "name": "extract_test_ab",
                "columns": [
                    {
                        "source_column": "A",
                        "target_column": "value_a",
                        "column_xforms": []
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
                "columns": [
                    {
                        "source_column": "C",
                        "target_column": "value_c",
                        "column_xforms": []
                    },
                    {
                        "source_column": "D",
                        "target_column": "value_d",
                        "column_xforms": []
                    },
                    {
                        "source_column": "E",
                        "target_column": "value_e",
                        "column_xforms": []
                    }
                ]
            }
        ]
    }
]

csv_full = "A,B,C,D,E\n1,a,a,x,5\n2,b,b,y,6\n3,c,c,z,7\n"
csv_ab = "A,B\n1,a\n2,b\n3,c\n"
csv_cde = "C,D,E\na,x,5\nb,y,6\nc,z,7\n"

def test_load_source_from_file():
    # should return a pandas dataframes per the source in the mapping file
    # When converted back to the csv, it should equal the origina file content
    # TODO - Update to support other datasource types

    # ARRANGE
    input = mapping[0]
    expected_results = csv_full
    # Create a file if it doesn't exist or overwrite it if it does.
    with open(input["source_name"], "w") as file:
        file.write(csv_full)

    # ACT
    # Load to dataframe then convert back to CSV string, without index
    results = load_source_from_file(input).to_csv(index=False)

    # ASSERT
    assert results == expected_results


def test_invoke(subtests):
    # should return a dict of pandas dataframes with correct values

    # ARRANGE
    input = mapping[0]
    test_cases = [
        ("extract_test_ab", csv_ab),
        ("extract_test_cde", csv_cde)
    ]

    # ACT
    results = invoke(input)

    # ASSERT
    for expected_key, expected_value in test_cases:
        with subtests.test(msg=f"Extract from CSV Source: {expected_key}"):
            assert results[expected_key].to_csv(index=False) == expected_value