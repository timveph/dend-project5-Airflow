from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
        # Source: https://stackoverflow.com/questions/40947490/truncate-table-before-inserting-new-data 
        
    sql_insert = """
        truncate {};
        insert into {}
        {};
        commit;
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_statement="",                 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_statement=sql_statement
        self.execution_date = kwargs.get('execution_date')


    def execute(self, context):
        self.log.info(f'LoadDimensionOperator for {self.table} table has begun')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Truncating and then Inserting data into dimension table {self.table}')
        formatted_sql = LoadDimensionOperator.sql_insert.format(
            self.table, 
            self.table,
            self.sql_statement
        )
        redshift.run(formatted_sql)
