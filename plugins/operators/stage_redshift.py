from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    template_fields = ('s3_key', )
    ui_color = '#358140'

    sql = """
        copy {table}
        from '{s3_path}'
        access_key_id '{login}'
        secret_access_key '{password}'
        json '{option}'
        region '{region}'
    """

    @apply_defaults
    def __init__(self,
                 conn_id='aws_credentials',
                 redshift_conn_id='redshift',
                 s3_bucket='',
                 s3_key='',
                 table='',
                 region='',
                 option='auto',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.region = region
        self.option = option

    def execute(self, context):
        self.log.info('Running StageToRedshiftOperator')
        metastore = MetastoreBackend()
        aws_credentials = metastore.get_connection(conn_id='aws_credentials')
        redshift = PostgresHook(postgres_conn_id='redshift')
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        sql = StageToRedshiftOperator.sql.format(
            table=self.table,
            s3_path=s3_path,
            login=aws_credentials.login,
            password=aws_credentials.password,
            option=self.option,
            region=self.region)
        redshift.run(sql)
