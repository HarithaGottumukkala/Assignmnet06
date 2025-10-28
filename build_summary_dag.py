from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Get variables from Airflow
snowflake_database = Variable.get("snowflake_database", default_var="USER_DB_PIKACHU")
snowflake_warehouse = Variable.get("snowflake_warehouse", default_var="MINNOW")

# Default arguments for the DAG
default_args = {
    'owner': 'Haritha',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'BuildSummary',
    default_args=default_args,
    description='ELT pipeline to create session_summary table with duplicate check',
    schedule_interval='@daily',
    catchup=False,
    tags=['elt', 'analytics', 'snowflake'],
)

# Task 1: Create analytics schema if not exists
create_analytics_schema = SnowflakeOperator(
    task_id='create_schema',
    snowflake_conn_id='snowflake_default',
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    sql="CREATE SCHEMA IF NOT EXISTS analytics;",
    dag=dag,
)

# Task 2: Check for duplicate records in source tables
check_duplicates = SnowflakeOperator(
    task_id='check_duplicates',
    snowflake_conn_id='snowflake_default',
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    sql="""
    -- Check for duplicates in user_session_channel
    SELECT 
        'user_session_channel' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT sessionId) as unique_sessions,
        COUNT(*) - COUNT(DISTINCT sessionId) as duplicate_count
    FROM raw.user_session_channel
    
    UNION ALL
    
    -- Check for duplicates in session_timestamp
    SELECT 
        'session_timestamp' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT sessionId) as unique_sessions,
        COUNT(*) - COUNT(DISTINCT sessionId) as duplicate_count
    FROM raw.session_timestamp;
    """,
    dag=dag,
)

# Task 3: Create or replace session_summary table with JOIN
create_session_summary = SnowflakeOperator(
    task_id='build_summary',
    snowflake_conn_id='snowflake_default',
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    sql="""
    CREATE OR REPLACE TABLE analytics.session_summary AS
    SELECT 
        usc.sessionId,
        usc.userId,
        usc.channel,
        st.ts as session_timestamp,
        DATE(st.ts) as session_date,
        HOUR(st.ts) as session_hour
    FROM raw.user_session_channel usc
    INNER JOIN raw.session_timestamp st
        ON usc.sessionId = st.sessionId;
    """,
    dag=dag,
)

# Task 4: Validate the created table
validate_summary = SnowflakeOperator(
    task_id='validate_summary',
    snowflake_conn_id='snowflake_default',
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    sql="""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT sessionId) as unique_sessions,
        COUNT(DISTINCT userId) as unique_users,
        COUNT(DISTINCT channel) as unique_channels,
        MIN(session_timestamp) as earliest_session,
        MAX(session_timestamp) as latest_session
    FROM analytics.session_summary;
    """,
    dag=dag,
)

# Task 5: Check for any duplicate sessionIds in final table
verify_no_duplicates = SnowflakeOperator(
    task_id='verify_no_duplicates',
    snowflake_conn_id='snowflake_default',
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    sql="""
    SELECT 
        CASE 
            WHEN COUNT(*) = COUNT(DISTINCT sessionId) THEN 'PASS: No duplicates found'
            ELSE 'FAIL: Duplicates detected'
        END as duplicate_check_status,
        COUNT(*) as total_records,
        COUNT(DISTINCT sessionId) as unique_sessions,
        COUNT(*) - COUNT(DISTINCT sessionId) as duplicate_count
    FROM analytics.session_summary;
    """,
    dag=dag,
)

# Set task dependencies
create_analytics_schema >> check_duplicates >> create_session_summary >> validate_summary >> verify_no_duplicates