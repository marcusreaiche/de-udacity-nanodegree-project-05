from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.sql_queries import sql_create_tables_dict


class DropTableOperator(BaseOperator):
    """
    Drop table operator
    """

    query = """
    drop table if exists {table}
    """

    def __init__(self, redshift_conn_id, table, *args, **kwargs):
        """
        Drops specified table if exists

        Arguments
        ---------
        redshift_conn_id: str (default: 'redshift')
        Connection id to database

        table: str (default: '')
        table to be dropped
        """
        super(DropTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(DropTableOperator.query.format(table=self.table))
