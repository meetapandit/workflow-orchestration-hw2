from airflow.sdk import dag, task
from pendulum import datetime
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

# from include.tasks.extract_data import extract_data
from include.tasks.load_to_staging_helper import load_to_staging_helper

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    description="Workflow for ETL from Postgres Taxi Data",
    catchup=False,
    tags=["taxi_data", "postgres", "etl"],
    max_consecutive_failed_dag_runs=3,
    # Define UI Parameters here
    params={
        # The 'enum' list creates the dropdown menu in the UI
        "color": Param(
            "green", 
            type="string", 
            enum=["green", "yellow"], 
            title="Taxi Color",
            description="Select the color of the taxi data to process."
        ),
        "schema": Param(
            "ny_taxi", 
            type="string", 
            title="Database Schema",
            description="The target schema in your Postgres database."
        ),
        "year": Param(
            2021, 
            type="integer", 
            enum=[2019, 2020, 2021],
            title="Data Year"
        ),
    },
)
def postgres_taxi_etl():

    # 1. Define Templated Table Name
    # This string is evaluated at runtime using Jinja
    templated_table = "{{ params.color }}_taxi_data"
    templated_schema = "{{ params.schema }}"

   # 1. Create Main Table
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_conn",
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {templated_schema};

        CREATE TABLE IF NOT EXISTS {templated_schema}.{templated_table} (
            unique_row_id TEXT,
            source TEXT,
            vendorid INTEGER,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE PRECISION,
            ratecodeid INTEGER,
            store_and_fwd_flag TEXT,
            pulocationid INTEGER,
            dolocationid INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            ingestion_ts TIMESTAMP DEFAULT NOW()
        );
        """
    )
    
    # 2. Create Staging Table
    create_staging_table = SQLExecuteQueryOperator(
        task_id="create_staging_table",
        conn_id="postgres_conn",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {templated_schema}.{templated_table}_staging 
        AS TABLE {templated_schema}.{templated_table} WITH NO DATA;
        """
    )

    # 3. Load Data to Staging (Python Task)
    @task
    def load_to_staging(params=None):
        load_to_staging_helper(
            table_name=f"{params['color']}_taxi_data_staging",
            schema=params["schema"],
            postgres_conn_id="postgres_conn",
            year=params["year"],
            color=params["color"],
            chunksize=20_000,
        )

    # 4. Transform Data in Staging
    # Fixed: Updated COALESCE to use generic pickup/dropoff names
    transform_staging_data = SQLExecuteQueryOperator(
        task_id="transform_staging_data",
        conn_id="postgres_conn",
        sql=f"""
            UPDATE {templated_schema}.{templated_table}_staging
            SET unique_row_id = md5(
                COALESCE(vendorid::text, '') ||
                COALESCE(pickup_datetime::text, '') ||
                COALESCE(dropoff_datetime::text, '') ||
                COALESCE(pulocationid::text, '') ||
                COALESCE(dolocationid::text, '') ||
                COALESCE(fare_amount::text, '') ||
                COALESCE(trip_distance::text, '')
            ),
            source = '{{{{ params.color }}}}_taxi';
        """
    )

    # 5. Merge and Cleanup
    merge_into_main_table = SQLExecuteQueryOperator(
        task_id="merge_into_main_table",
        conn_id="postgres_conn",
        sql=f"""
        MERGE INTO {templated_schema}.{templated_table} AS main
        USING {templated_schema}.{templated_table}_staging AS staging
        ON main.unique_row_id = staging.unique_row_id
        WHEN NOT MATCHED THEN
            INSERT (unique_row_id, source, vendorid, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge)
            VALUES (staging.unique_row_id, staging.source, staging.vendorid, staging.pickup_datetime, staging.dropoff_datetime, staging.passenger_count, staging.trip_distance, staging.ratecodeid, staging.store_and_fwd_flag, staging.pulocationid, staging.dolocationid, staging.payment_type, staging.fare_amount, staging.extra, staging.mta_tax, staging.tip_amount, staging.tolls_amount, staging.improvement_surcharge);

        TRUNCATE TABLE {templated_schema}.{templated_table}_staging;
        """
    )
 
    # Workflow
    create_table >> create_staging_table >> load_to_staging() >> transform_staging_data >> merge_into_main_table

# Instantiate
postgres_taxi_etl()
