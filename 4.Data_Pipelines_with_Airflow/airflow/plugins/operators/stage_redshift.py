from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    # SQL template for CSV input format
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 bucket="",
                 key="",
                 json_path="auto",
                 region="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, 
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.bucket = bucket
        self.key = key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        # Create aws hook 
        self.log.info("Create aws hook")
        aws_hook = AwsHook(self.aws_credentials_id)
        # Get the aws credentials
        credentials = aws_hook.get_credentials()
        
        # Create redshift hook 
        self.log.info("Create redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Delete data from table we want to load data into
        self.log.info("Delete data from table we want to load data into")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # Copy data from S3 to Redshift
        self.log.info("Copy data from S3 to Redshift")
        rendered_key = self.key.format(**context)
        path = "s3://{}/{}".format(self.bucket, rendered_key)
        sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(sql)

    



