from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    sql_json_statement = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
    """
    
    sql_csv_statement = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        IGNOREHEADER {}
        DELIMETER '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="redshift",
                 table="",
                 aws_credentials_id="aws_credentials",
                 s3_bucket="udacity-dend",
                 s3_key="",
                 region="us-east-2",
                 file_format="JSON",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):
        

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table = table
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.file_format = file_format
        self.delimiter= delimiter,
        self.ignore_headers=ignore_headers,
        self.execution_date = kwargs.get('execution_date')
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator - execution has begun')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
#         self.log.info(f"redshift_id = {redshift} || ")
        
        
#         self.log.info("Deleting data from destination Redshift table before load")
#         redshift.run("DELETE FROM {}".format(self.table))
      
        self.log.info("Creating S3 path from parameters")  
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"s3_path is: {s3_path}")
        
        if self.file_format == "JSON":
            self.log.info("Copying data from S3 (JSON) to Redshift")  
            self.log.info(f"s3_path is: {s3_path}")
            formatted_sql = StageToRedshiftOperator.sql_json_statement.format(
                    self.table, 
                    s3_path, 
                    aws_credentials.access_key,
                    aws_credentials.secret_key, 
                    self.region
                )
        
            redshift.run(formatted_sql)
        
        if self.file_type == "csv":
            self.log.info("Copying data from S3 (csv) to Redshift")
            self.log.info(f"s3_path is: {s3_path}")
            formatted_sql = StageToRedshiftOperator.sql_csv_statement.format(
                self.table,
                s3_path, 
                aws_credentials.access_key,
                aws_credentials.secret_key,
                self.region,
                self.ignore_headers,
                self.delimiter
            )
            redshift.run(formatted_sql)


