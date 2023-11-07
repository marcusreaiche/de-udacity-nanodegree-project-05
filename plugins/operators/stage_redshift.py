from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    """
    Copy data from the S3 path to the staging table.
    """

    INSERT_MODE_OPTS = ['append-load', 'delete-load']
    template_fields = ('s3_key', )
    ui_color = '#358140'

    sql = """
        copy {table}
        from '{s3_path}'
        access_key_id '{login}'
        secret_access_key '{password}'
        json '{option}'
        region '{region}';
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
                 insert_mode='delete-load',
                 *args, **kwargs):
        """
        StageToRedshiftOperator constructor

        Arguments
        ---------
        conn_id: str (default: 'aws_credentials')
        Connection id to Amazon Web Services

        redshift_conn_id: str (default: 'redshift')
        Connection id to database

        s3_bucket: str (default: '')
        S3 bucket where data is stored

        s3_key: str (default: '')
        S3 key to copy the data from

        table: str (default: '')
        Target staging table

        region: str (default: '')
        AWS region of S3 bucket

        option: str (default: 'auto')
        Option to process JSON files

        insert_mode: 'delete-load' (default) | 'append-load'
        Insert mode used in the load
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.region = region
        self.option = option
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info('Running StageToRedshiftOperator')
        metastore = MetastoreBackend()
        aws_credentials = metastore.get_connection(conn_id=self.conn_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        sql = self._build_transformation(s3_path=s3_path,
                                         login=aws_credentials.login,
                                         password=aws_credentials.password)
        self.log.info(f'Copying data from {s3_path} to {self.table}')
        redshift.run(sql)

    def _check_params(self, insert_mode):
        opts = StageToRedshiftOperator.INSERT_MODE_OPTS
        if insert_mode not in opts:
            raise ValueError(
                f'Insert mode must be one of {",".join(opts)}. ' +
                f'Insert mode {insert_mode} option is not supported.')

    def _build_transformation(self, s3_path, login, password):
        delete_transformation = ''
        if self.insert_mode == 'delete-load':
            delete_transformation = f'delete from {self.table} where true;'
        load_transformation = (
            StageToRedshiftOperator.sql.format(
                table=self.table,
                s3_path=s3_path,
                login=login,
                password=password,
                option=self.option,
                region=self.region)
            .strip())
        return f"""
            begin;
            {delete_transformation}
            {load_transformation}
            commit;"""

