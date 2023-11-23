from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_list=[],
                 check_type="count_check",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
        self.check_type = check_type


    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        self.log.info('Performing data quality checks on tables: {self.tables}')
        # connect to redshift
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.table_list:
           self.log.info('performing {self.check_type} check on table: {table}')
         # perform data quality check based on check_type
           if self.check_type == 'count_check':
              self.count_check(redshift, table)

    def count_check(self, redshift, table):
        result = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
        count = result[0][0]
        if count == 0:
           raise ValueError(f"Data quality check failed for table {table}. Count is 0. ")
        else:
           self.log.info(f"Data quality check passed for table {table}. Count: {count}") 
 #    self.log.info('Executing data quality check for {self.table_list}')
 #       record_count = redshift.get_records(self.table_list)
 #       if (record_count[0][0] < 1):
 #           raise ValueError(f"Data quality check failed. returned no results")
 #       if int(self.sql_check_expected_value) != record_count[0][0]:
 #           raise ValueError(f"Data quality check failed. \n Expected value is  {self.sql_check_expected_value} \n Real value is {record_count[0][0]}")
 #       self.log.info("Data quality checks all passed ")  