from datetime import datetime, timedelta
import os
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator)
from airflow.models import Variable
from helpers import SqlQueries


default_args = dict(
    owner='Marcus Reaiche',
    start_date=datetime(2019, 1, 12),
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_failure=False,
    email_on_retry=False,
    catchup=False,
)


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly')
def udac_example_dag():

    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        conn_id='aws_credentials',
        redshift_conn_id='redshift',
        table='staging_events',
        s3_bucket=Variable.get('s3_bucket'),
        s3_key=Variable.get('log_data'),
        option=Variable.get('log_jsonpath'),
        region='us-west-2',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        conn_id='aws_credentials',
        redshift_conn_id='redshift',
        table='staging_songs',
        s3_bucket=Variable.get('s3_bucket'),
        s3_key=Variable.get('song_data'),
        option='auto',
        region='us-west-2',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id='redshift',
        sql=SqlQueries.songplay_table_insert,
        table='songplays'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert,
        insert_mode='delete-load')

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert,
        insert_mode='delete-load'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        insert_mode='delete-load'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert,
        insert_mode='delete-load'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    # Task dependencies
    stage_tasks = [stage_events_to_redshift, stage_songs_to_redshift]
    load_dimension_tables = [
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
        load_user_dimension_table]

    start_operator >> stage_tasks
    stage_tasks >> load_songplays_table
    load_songplays_table >> load_dimension_tables
    load_dimension_tables >> run_quality_checks
    run_quality_checks >> end_operator

udac_example = udac_example_dag()
