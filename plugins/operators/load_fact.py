from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id='redshift',
                 sql='',
                 table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql.strip()
        self.table = table

    def execute(self, context):
        self.log.info('Runnig LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        query = self._build_transformation()
        redshift.run(query)

    def _build_transformation(self):
        return f"""
            insert into {self.table}
            {self.sql}"""
