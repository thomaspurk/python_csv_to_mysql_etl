import os
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine

DB_CONFIG = {
    'host':os.environ.get("DB_HOST"),
    'user':os.environ.get("DB_USER"),
    'password':os.environ.get("DB_PASSWORD"),
}




def invoke (etl_mapping: dict, extract_tables: dict):
    """
    TODO - Refactor deep nesting for readability.
    """

    
    for table_name in extract_tables.keys():

        for config_table in etl_mapping["extract_tables"]:
            if config_table["name"] == table_name:
                
                service = os.environ.get(config_table["target"]["service"])
                host = os.environ.get(config_table["target"]["host"])
                port = os.environ.get(config_table["target"]["port"])
                database = os.environ.get(config_table["target"]["database"])
                service = os.environ.get(config_table["target"]["service"])
                user = os.environ.get(config_table["target"]["user"])
                password = os.environ.get(config_table["target"]["password"])
                table = config_table["target"]["table"]

                connection_string = f"{service}://{user}:{password}@{host}/{database}"
                engine = create_engine(url=connection_string)

                df: DataFrame = extract_tables[table_name]
                df.to_sql(
                    name=table,  # type: ignore
                    con=engine, 
                    if_exists='append', 
                    index=False
                    )
                engine.dispose()