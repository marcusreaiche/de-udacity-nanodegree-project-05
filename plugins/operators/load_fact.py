from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Load data to fact table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql='',
                 table='',
                 *args, **kwargs):
        """
        LoadFactOperator constructor

        Arguments
        ---------
        redshift_conn_id: str (default: 'redshift')
        Connection id to database

        sql: str (default: '')
        SQL select query whose result will be inserted in target fact table

        table: str (default: '')
        Target fact table
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql.strip()
        self.table = table

    def execute(self, context):
        self.log.info('Runnig LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        query = self._build_transformation()
        redshift.run(query)

    def _build_transformation(self):
        return f"""
            insert into {self.table}
            {self.sql}"""
