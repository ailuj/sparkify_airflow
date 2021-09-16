from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 JSON_formatting='',
                 append_data = False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.JSON_formatting = JSON_formatting

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 source bucket to Redshift table")
        
        copy_query = """
                    COPY {table_name}
                    FROM '{s3_bucket}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {JSON_formatting};
                """.format(table_name=self.table_name,
                           s3_bucket=self.s3_bucket,
                           #s3_prefix=self.s3_prefix,
                           access_key=aws_credentials.access_key,
                           secret_key=aws_credentials.secret_key,
                           JSON_formatting=self.JSON_formatting)
        redshift.run(copy_query)




