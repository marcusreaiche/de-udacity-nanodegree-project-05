from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.sql_queries import sql_create_tables_dict


class CreateTableOperator(BaseOperator):
    """
    Operator that creates a table in a specified database
    """
    def __init__(self, redshift_conn_id, table, sql=None, *args, **kwargs):
        """

        Arguments
        ---------
        redshift_conn_id: str (default: 'redshift')
        Connection id to database

        table: str (default: '')
        Table to be created. Must be a key of helpers.sql_queries.sql_create_tables_dict
        if sql argument is not provided

        sql: str (default: None)
        SQL command to create table. If None (or falsifiable), then table is used
        to get SQL from helpers.sql_queries.sql_create_tables_dict
        """

        self._check_init_args(table, sql)
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        if not sql:
            self.sql = sql_create_tables_dict[table]
        else:
            self.sql = sql

    def _check_init_args(self, table, sql):
        if not sql and table not in sql_create_tables_dict:
            raise ValueError(
                f'Table {table} is not one of {", ".join(sql_create_tables_dict)}.\n'
                + 'Provide "sql" argument with "create table..." command')

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.sql)
