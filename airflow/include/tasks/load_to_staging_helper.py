from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import pandas as pd
import io
import gc
from include.tasks.extract_data_helper import extract_data_helper
# from include.tasks.transform_df_helper import transform_df_helper

def load_to_staging_helper(
    table_name: str,
    schema: str,
    postgres_conn_id: str = "postgres_conn",
    year: int = 2019,
    color: str = "green",
    chunksize: int = 100_000,
):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    print(f"Using Postgres URI: {hook.get_uri()}")
    engine = create_engine(hook.get_uri())

    # Use generic target columns for both yellow and green taxis
    target_columns = [
        "vendorid",
        "pickup_datetime", # common name
        "dropoff_datetime", # common name
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "store_and_fwd_flag",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge"
    ]
    
    full_table = f"{schema}.{table_name}"

    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        for i, df in enumerate(extract_data_helper(year=year, chunksize=chunksize, color=color)):
            # lower case all column names
            df.columns = [c.lower() for c in df.columns]

            # map of date columns to rename
            rename_map = {
                'lpep_pickup_datetime': 'pickup_datetime',
                'lpep_dropoff_datetime': 'dropoff_datetime',
                'tpep_pickup_datetime': 'pickup_datetime',
                'tpep_dropoff_datetime': 'dropoff_datetime'
            }
            df = df.rename(columns=rename_map)

            # Find inetersection of target columns and dataframe columns
            # This handles cases where one dataset might be missing a column
            present_cols = [c for c in target_columns if c in df.columns]
            df = df[present_cols]

            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            # Copy command uses columns present in current dataset
            cursor.copy_expert(
                sql=f"COPY {full_table} ({','.join(present_cols)}) FROM STDIN WITH CSV",
                file=buffer,
            )

            conn.commit()
            print(f"Loaded chunk {i + 1} for {color} taxi")

            del df, buffer
            gc.collect()

    finally:
        cursor.close()
        conn.close()
