from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from operators.create_table import CreateTableOperator
from helpers.sql_queries import sql_create_tables_dict


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


@dag(default_args=default_args,
     schedule_interval=None,
     description='Create tables in Redshift with Airflow')
def create_tables_dag():
    start_task = EmptyOperator(task_id='start')
    tasks = []
    for table, sql in sql_create_tables_dict.items():
        task = CreateTableOperator(
            task_id=f'create_{table}_table_task',
            redshift_conn_id='redshift',
            table=table)
        tasks.append(task)
    end_task = EmptyOperator(task_id='end')
    # Tasks dependencies
    start_task >> tasks
    tasks >> end_task


create_table = create_tables_dag()
