from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Perform data quality checks
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id='redshift',
                 test_cases=None,
                 *args, **kwargs):
        """
        Arguments
        ---------
        redshift_conn_id: str (default: 'redshift')
        Connection id to database

        test_cases: iterable (default: None)
        test_cases should be an iterable of pairs such as [(sql1, res1), ...]
        The first element of the pair is an SQL query and the second element is the
        query expected result.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        if test_cases is None:
            test_cases = []
        self.test_cases = test_cases

    def execute(self, context):
        self.log.info('Running DataQualityOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
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
