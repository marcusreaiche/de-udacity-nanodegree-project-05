from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    INSERT_MODE_OPTS = ['append-load', 'delete-load']
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id='redshift',
                 table='',
                 sql='',
                 insert_mode='append-load',
                 *args, **kwargs):
        """
        Instantiates a LoadDimensionOperator

        Arguments
        ---------
        conn_id: str (default: 'redshift')
        Connection id to database cluster

        table: str (default: '')
        Target dimension table

        sql: str (default: '')
        Insert SQL command to load data to target table

        insert_mode: 'append-load' (default) | 'delete-load'
        Insert mode used in the load
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self._check_params(insert_mode)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql.strip()
        self.insert_mode = insert_mode

    def _check_params(self, insert_mode):
        opts = LoadDimensionOperator.INSERT_MODE_OPTS
        if insert_mode not in opts:
            raise ValueError(
                f'Insert mode must be one of {",".join(opts)}. ' +
                f'Insert mode {insert_mode} option is not supported.')

    def execute(self, context):
        self.log.info(
            f'Running LoadDimensionOperator for dimension table {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        query = self._build_transformation()
        redshift.run(query)

    def _build_transformation(self):
        delete_transformation = ''
        if self.insert_mode == 'delete-load':
            delete_transformation = f'delete from {self.table} where true;'
        load_transformation = f"""
            insert into {self.table}
            {self.sql}""".strip()
        return f"""
            begin;
            {delete_transformation}
            {load_transformation}
            commit;"""
