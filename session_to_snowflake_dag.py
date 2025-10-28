from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Get variables from Airflow
snowflake_database = Variable.get("snowflake_database", default_var="USER_DB_PIKACHU")
snowflake_schema = Variable.get("snowflake_schema", default_var="raw")
snowflake_warehouse = Variable.get("snowflake_warehouse", default_var="MINNOW")

# Default arguments for the DAG
default_args = {
    'owner': 'HARITHA',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'SessionToSnowflake',
    default_args=default_args,
    description='Load session data from S3 to Snowflake',
    schedule_interval='@daily',
    catchup=False,
    tags=['snowflake', 's3', 'data-loading'],
)

# Task 1: Create or replace stage with S3 bucket (LIST/READ privileges required)
create_stage = SnowflakeOperator(
    task_id='set_stage',
    snowflake_conn_id='snowflake_default',
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema,
    sql="""
    CREATE OR REPLACE STAGE raw.blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """,
    dag=dag,
)

# Task 2: Load data into user_session_channel from S3
load_user_session_channel = SnowflakeOperator(
    task_id='load',
    snowflake_conn_id='snowflake_default',
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema,
    sql="""
    COPY INTO raw.user_session_channel
    FROM @raw.blob_stage/user_session_channel.csv;
    
    COPY INTO raw.session_timestamp
    FROM @raw.blob_stage/session_timestamp.csv;
    """,
    dag=dag,
)

# Set task dependencies - stage must be created before loading
create_stage >> load_user_session_channel