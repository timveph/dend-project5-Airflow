from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 params="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.params=params
        

    def execute(self, context):
        self.log.info('DataQualityOperator - Data Quality checks under way...')
        table = kwargs["params"]["table"]
        hook_redshift = PostgresHook(self.redshift_conn_id)
        records = hook_redshift.get_records(f"SELECT COUNT(*) FROM {table}")
        # lesson/demo 4
#         num_records = records[0][0]
#             if num_records < 1:
        if records is None or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.error(f"Data quality on table {table} check passed with {records[0][0]} records")
        self.log.info('Data Quality check complete')
        