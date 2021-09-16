from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_query_sql="",
                 table_name="",
                 delete_data=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_query_sql = insert_query_sql
        self.delete_data=delete_data
        self.table_name=table_name

    def execute(self, context):
        self.log.info('Loading Dimension table into Redshift cluster')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_data:
            redshift.run(f'TRUNCATE TABLE {self.table_name};')

        redshift.run(self.insert_query_sql)
