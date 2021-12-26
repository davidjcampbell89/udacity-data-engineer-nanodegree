from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_stmt="",
                 result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.result = result

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # CHECK THAT THE TABLE IS POPULATED WITH RECORDS RETURNED
        for expected, actual in zip(self.result, self.sql_stmt):
            actual_rendered = redshift_hook.get_records(actual)
            if expected != actual_rendered[0][0]:
                raise ValueError(f"Data quality check failed. Expected {expected}, but returned result {actual_rendered[0][0]}")
            self.log.info(f"Data quality check passed. Returned result {actual_rendered[0][0]} agrees with expected result {expected}")
