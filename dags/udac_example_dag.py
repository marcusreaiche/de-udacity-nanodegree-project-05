from datetime import datetime, timedelta
import os
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = dict(
    owner='Marcus Reaiche',
    start_date=datetime(2019, 1, 12),
    catchup=False,
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_failure=False,
    email_on_retry=False,
)


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly')
def udac_example_dag():

    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
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
