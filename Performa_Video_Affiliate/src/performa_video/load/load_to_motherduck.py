import pandas as pd

if_limit = "LIMIT 100"

def load_df_duckdb(
    df:pd.DataFrame, 
    table_id: str, 
    connection: str, 
    if_exists: str
):
    if if_exists == "replace":
        connection.sql(
            f"CREATE OR REPLACE TABLE {table_id} AS SELECT * FROM df {if_limit}"
        )
    elif if_exists == "append":
        exists = connection.sql(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_id}'
        """).fetchone()[0]

        if not exists:
            connection.sql(
                f"CREATE TABLE {table_id} AS SELECT * FROM df {if_limit}"
            )
        else:
            connection.sql(
                f"INSERT INTO {table_id} SELECT * FROM df {if_limit}"
            )
