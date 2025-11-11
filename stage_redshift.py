# plugins/operators/stage_redshift.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from plugins.helpers.sql_queries import copy_sql_template

class StageToRedshiftOperator(BaseOperator):
"""Copy JSON/CSV data from S3 to Redshift staging table.
   Template fields allow jinja templating of s3_key to support partitioning by execution date.
"""
    ui_color = '#358140'
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_iam_role='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='us-west-2',
                 file_format='json', # 'json' or 'csv'
                 json_path='auto',
                 delimiter=',',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_iam_role = aws_iam_role
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.json_path = json_path
        self.delimiter = delimiter

    def execute(self, context):
        self.log.info('StageToRedshiftOperator starting')


        if not all([self.redshift_conn_id, self.aws_iam_role, self.table, self.s3_bucket, self.s3_key]):
            raise AirflowException('Missing required parameter for StageToRedshiftOperator')


        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

# Build format-specific clauses
        if self.file_format.lower() == 'json':
            format_clause = 'JSON'
            if self.json_path and self.json_path.lower() != 'auto':
# allow absolute s3 path or 'auto'
               json_path_clause = f"FORMAT AS JSON '{self.json_path}'"
            else:
               json_path_clause = "FORMAT AS JSON 'auto'"
        elif self.file_format.lower() == 'csv':
            format_clause = 'CSV'
            json_path_clause = f"DELIMITER '{self.delimiter}' IGNOREHEADER 1"
        else:
            raise AirflowException(f'Unsupported file_format {self.file_format}. Use "json" or "csv"')

# The copy_sql_template expects placeholders; we will build final SQL matching that template
# but to keep it clear, supply format and json_path_clause separately
       final_sql = copy_sql_template.format(
           table=self.table,
           s3_path=s3_path,
           iam_role=self.aws_iam_role,
           format=format_clause,
           json_path_clause=json_path_clause,
           region=self.region
    )


    self.log.info(f'Clearing data from destination table {self.table} before COPY')
    redshift.run(f'TRUNCATE TABLE {self.table}')


    self.log.info(f'Running COPY command from {s3_path} into {self.table}')
    self.log.debug('COPY SQL: %s', final_sql)
    redshift.run(final_sql)


    self.log.info('StageToRedshiftOperator completed')          
