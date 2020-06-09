from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    sql_insert = """
        insert into {}
        {};
        commit; 
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table = "",
                 sql_statement = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_statement=sql_statement
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        self.log.info('LoadFactOperator implementation begun')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
#         aws_hook = AwsHook(self.aws_credentials_id)
#         aws_credentials = aws_hook.get_credentials()
        
        self.log.info('Inserting data into fact table')
        formatted_sql = LoadFactOperator.sql_insert.format(
            self.table,
            self.sql_statement
        )
        redshift.run(formatted_sql)
        