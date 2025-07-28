import pandas as pd
from pandas import DataFrame

def invoke (etl_mapping: dict, extract_tables: dict):
    """
    TODO - Refactor deep nesting for readability.
    """

    for table_name in extract_tables.keys():

        for table in etl_mapping["extract_tables"]:
            if table["name"] == table_name:
                # Table Transformations
                run_table_transformations(
                    df=extract_tables[table_name],
                    transformations=table["table_xforms"]
                )
                column_rename = {}
                for column in table["columns"]:
                    column_rename[column["source_column"]] = column["target_column"]
                    # Columns Transformations
                    run_column_transformations(
                        df=extract_tables[table_name],
                        column_name=column["source_column"],
                        transformations=column["column_xforms"]
                    )
                
                # Complete the rename
                extract_tables[table_name].rename(columns=column_rename, inplace=True)

def run_table_transformations(df: DataFrame, transformations: list):
    """ 
    TODO
    """

    for xform in transformations:
        xform_function = globals()[xform["function"]]
        xform_mapping = xform.get("mapping", None)
        
        xform_function(df)

def run_column_transformations( df: DataFrame, column_name: str, transformations: list):
    """ 
    TODO
    """

    for xform in transformations:
        xform_function = globals()[xform["function"]]
        xform_mapping = xform.get("mapping", None)
        
        xform_function(df,column_name, *xform["args"])

def to_unique(df: DataFrame):
    """
    TODO
    """
    df.drop_duplicates(inplace=True)

def to_int(df: DataFrame, column_name: str, *args):
    """ 
    TODO
    """

    df[column_name] = df[column_name].astype(int)

def to_text(df: DataFrame, column_name: str, *args):
    """ TODO
    """

    df[column_name] = df[column_name].astype(str)

def to_date(df: DataFrame, column_name: str, *args):
    """ TODO
    """

    df[column_name] = pd.to_datetime(df[column_name])

def to_decimal(df: DataFrame, column_name: str, *args):
    """ TODO
    """
    x=""
    #df[column_name] = pd.to_datetime(df[column_name])

def append_value(df: DataFrame, column_name: str, *args):
    """ TODO
    """

    df[column_name] = df[[column_name, args[0]]].astype(str).agg(args[1].join, axis=1)
       
def drop_column(df: DataFrame, column_name: str, *args):
    """ TODO
    """

    df.drop(columns=args[0], inplace=True)              
