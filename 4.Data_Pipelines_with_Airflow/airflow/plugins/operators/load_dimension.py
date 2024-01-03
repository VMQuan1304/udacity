from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 dim_insert_sql="",
                 append_insert=False,
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.dim_insert_sql = dim_insert_sql
        self.append_insert = append_insert
        self.primary_key = primary_key

    def execute(self, context):
        # Redshift credentials
        self.log.info("Redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_insert:
            sql = f"""
                create temp table stage_{self.table} (like {self.table}); 
                
                insert into stage_{self.table}
                {self.dim_insert_sql};
                
                delete from {self.table}
                using stage_{self.table}
                where {self.table}.{self.primary_key} = stage_{self.table}.{self.primary_key};
                
                insert into {self.table}
                select * from stage_{self.table};
            """
        else:
            sql = f"""
                insert into {self.table}
                {self.dim_insert_sql}
            """
            
            # Clearing data from dimension table
            self.log.info("logging message")
            redshift_hook.run(f"TRUNCATE TABLE {self.table};")
        
        # Load data into dimension table
        self.log.info("Load data into dimension table")
        redshift_hook.run(sql)
