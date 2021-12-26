from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    # TRUNCATE TABLE logic taken from https://www.sqlservertutorial.net/sql-server-basics/sql-server-truncate-table/
    truncate_sql_template = """
    TRUNCATE TABLE {destination_table}
    """
    
    insert_sql_template = """
    INSERT INTO {destination_table}
    {sql_stmt}
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_stmt="",
                 truncate_option=True, # set default option to True so that we delete all rows from the destination table unless otherwise specified
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_stmt = sql_stmt
        self.truncate_option = truncate_option 

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # If the truncate_option field is set to True then we will first truncate
        if self.truncate_option:
            self.log.info("Start deleting data from dimensions table {destination_table}")
            truncate_sql = LoadDimensionOperator.truncate_sql_template.format(
                destination_table=self.destination_table,
            )
            redshift.run(truncate_sql)
            self.log.info("Finished deleting data from dimensions table {destination_table}")

        # Load data from staging area to the fact table
        self.log.info("Start loading data from staging into dimensions table {destination_table}")
        insert_sql = LoadDimensionOperator.insert_sql_template.format(
            destination_table=self.destination_table,
            sql_stmt=self.sql_stmt
        )
        redshift.run(insert_sql)
        self.log.info("Finished loading data from staging into dimensions table {destination_table}")