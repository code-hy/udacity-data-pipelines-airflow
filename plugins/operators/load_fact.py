from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 sql="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate


    def execute(self, context):
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Truncate the table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')

        self.log.info(f'Loading the fact table {self.table}')
#        redshift.run(f'INSERT INTO {self.table} {self.sql}')
        redshift.run({self.sql})
        self.log.info('Finished loading fact table')


        self.log.info('LoadFactOperator not implemented yet')
