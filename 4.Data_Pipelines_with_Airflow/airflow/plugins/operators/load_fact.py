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
                 table="",
                 songplay_insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.songplay_insert_sql = songplay_insert_sql

    def execute(self, context):
        # Get the redshift credentials
        self.log.info("Get the redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Load data into songplays table
        self.log.info("Load data into songplays table")
        sql = f"""
            INSERT INTO {self.table}
            {self.songplay_insert_sql}
        """
        redshift_hook.run(sql)