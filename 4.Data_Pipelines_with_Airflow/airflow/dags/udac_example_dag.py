from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

from airflow.operators import PostgresOperator


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'quanvm4',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udacity_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
#     sql = "SELECT * FROM information_schema.tables"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    bucket='udacity-dend',
    key='log_data',
    json_path='s3://udacity-dend/log_json_path.json',
    region='us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    bucket='udacity-dend',
    key='song_data',
    json_path='auto',
    region='us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    songplay_insert_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    dim_insert_sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    dim_insert_sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    dim_insert_sql=SqlQueries.artist_table_insert,
    append_insert=True,
    primary_key="artistid"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    dim_insert_sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
        {'test_sql': "SELECT COUNT(*) FROM songplays", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM time", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM users", 'expected_result': 0},
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table \
>> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
>> run_quality_checks >> end_operator







