from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_cmd = """
       COPY {}
       FROM '{}'
       ACCESS_KEY_ID '{}'
       SECRET_ACCESS_KEY '{}'
       REGION '{}'
       JSON '{}';
     """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 aws_credentials_id = "",
                 redshift_conn_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_path = "",
                 s3_key = "",
                 json_path = "auto",
                 file_format="json",
                 region = "us-west-2",
                 json_option = "",
                 copy_options= "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.s3_key = s3_key
        self.file_format = file_format
        self.region = region
        self.json_option = json_option



    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        ##self.log.info("Delete Data from Redshift Staging table")
        ##redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Truncate Data from Redshift Staging table")
        redshift.run("DELETE FROM {}".format(self.table))
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("Copy data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_cmd.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_option,
        )
        redshift.run(formatted_sql)
        self.log.info("Finished Delete and Copy from S3 to Redshift")

               


        self.log.info('StageToRedshiftOperator not implemented yet')





