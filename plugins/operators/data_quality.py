from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 conn_id='redshift',
                 test_cases=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        if test_cases is None:
            test_cases = []
        self.test_cases = test_cases

    def execute(self, context):
        self.log.info('Running DataQualityOperator')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        raise_error = False
        for sql, expected_result in self.test_cases:
            records = redshift.get_records(sql)
            result = records[0][0]
            if result != expected_result:
                raise_error = True
                self.log.error(f'{sql} - result: {result} != {expected_result}: expected')
        if raise_error:
            task_id = context['task'].task_id
            raise ValueError(f'{task_id} has unexpected results')
