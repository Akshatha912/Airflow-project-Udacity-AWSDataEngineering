from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries


# -------------------------------------------------------------------------
# Default arguments for the DAG
# -------------------------------------------------------------------------
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


# -------------------------------------------------------------------------
# Define the DAG
# -------------------------------------------------------------------------
dag = DAG(
    'final_project_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',  # once per hour
    max_active_runs=1,
    tags=['udacity', 'sparkify', 'redshift']
)

# -------------------------------------------------------------------------
# Define start and end DummyOperators
# -------------------------------------------------------------------------
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


# -------------------------------------------------------------------------
# Task: Create Tables (optional, but good for first run)
# -------------------------------------------------------------------------
create_tables = PostgresOperator(
    task_id='Create_tables',
    postgres_conn_id='redshift',
    sql='create_tables.sql',
    dag=dag
)

# -------------------------------------------------------------------------
# Stage Events and Songs to Redshift
# -------------------------------------------------------------------------
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='airflow-project-bucket123',
    s3_key='log-data',
    copy_json_option='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='uc-de-airflow-aws',
    s3_key='song-data',
    copy_json_option='auto'
)

# -------------------------------------------------------------------------
# Load Fact Table
# -------------------------------------------------------------------------
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

# ---------
