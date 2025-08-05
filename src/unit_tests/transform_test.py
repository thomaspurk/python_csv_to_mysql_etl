import pandas as pd
from pandas import DataFrame
from io import StringIO

from src.etl_pipeline.tasks.transform import invoke, run_column_transformations, run_table_transformations, to_date, to_decimal, to_int, to_text, to_unique

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

def test_to_unique(df: DataFrame):
    """should remove duplicates from the dataframe"""

    # ARRANGE
    dup_csv_ab = csv_ab + "\n1,a\n2,b\n3,c\n"
    df = pd.read_csv(StringIO(csv_ab))
    dup_df = pd.read_csv(StringIO(dup_csv_ab))

    # ACT

    # ASSERT