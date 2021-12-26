from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    # TRUNCATE TABLE logic taken from https://www.sqlservertutorial.net/sql-server-basics/sql-server-truncate-table/
    truncate_sql_template = """
    TRUNCATE TABLE {destination_table}
    """
    
    insert_sql_template = """
    INSERT INTO {destination_table}
    {sql_stmt}
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Truncate data from fact table
        self.log.info("Start deleting data from fact table {destination_table}")
        truncate_sql = LoadFactOperator.truncate_sql_template.format(
            destination_table=self.destination_table,
        )
        redshift.run(truncate_sql)
        self.log.info("Finished deleting from fact table {destination_table}")

        # Load data from staging area to the fact table
        self.log.info("Start loading data from staging into fact table {destination_table}")
        facts_sql = LoadFactOperator.insert_sql_template.format(
            destination_table=self.destination_table,
            sql_stmt=self.sql_stmt
        )
        redshift.run(facts_sql)
        self.log.info("Finished loading data from staging into fact table {destination_table}")