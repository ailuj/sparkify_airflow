from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_events_table = PostgresOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_events_create
)

create_staging_songs_table = PostgresOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_songs_create
)

schema_created = DummyOperator(task_id='Schema_created', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table_name='public.staging_events',
    s3_bucket='s3://udacity-dend/log_data',
    JSON_formatting = "JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table_name='public.staging_songs',
    s3_bucket='s3://udacity-dend/song_data',
    JSON_formatting = "FORMAT AS JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    fact_insert_sql = SqlQueries.songplay_table_insert,
    truncate_table = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    insert_query_sql = SqlQueries.user_table_insert,
    table_name = 'public.users',
    delete_data = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    insert_query_sql = SqlQueries.song_table_insert,
    table_name = 'public.songs',
    delete_data = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    insert_query_sql = SqlQueries.artist_table_insert,
    table_name = 'public.artists',
    delete_data = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'public.time',
    insert_query_sql = SqlQueries.time_table_insert,
    delete_data = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables_list=['public.songplays', 'public.users', 'public.artists', 'public.songs'],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_staging_songs_table
start_operator >> create_staging_events_table 
create_staging_songs_table >> schema_created
create_staging_events_table >> schema_created
schema_created >> stage_songs_to_redshift
schema_created >> stage_events_to_redshift
                              
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
