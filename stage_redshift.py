# plugins/operators/stage_redshift.py

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
import logging

class StageToRedshiftOperator(BaseOperator):
    """
    Copies data from S3 to Redshift staging table.
    - Supports JSON and CSV.
    - template_fields allows s3_key templating (execution date partitions etc).
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
                 file_format='json',     # 'json' or 'csv'
                 json_path='auto',       # s3 path or 'auto'
                 delimiter=',',          # for CSV
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_iam_role = aws_iam_role
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format.lower()
        self.json_path = json_path
        self.delimiter = delimiter

    def _build_copy_sql(self, s3_path):
        """
        Build COPY SQL depending on file_format and parameters.
        """
        if self.file_format == 'json':
            # Use JSON 'auto' or user supplied json_path
            json_clause = f"JSON '{self.json_path}'" if self.json_path and self.json_path.lower() != 'auto' else "JSON 'auto'"
            copy_sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                IAM_ROLE '{self.aws_iam_role}'
                {json_clause}
                REGION '{self.region}'
                TIMEFORMAT as 'epochmillisecs'
                COMPUPDATE OFF
                STATUPDATE OFF;
            """
        elif self.file_format == 'csv':
            # For CSV: provide delimiter and optionally IGNOREHEADER
            copy_sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                IAM_ROLE '{self.aws_iam_role}'
                DELIMITER '{self.delimiter}'
                IGNOREHEADER 1
                REGION '{self.region}'
                COMPUPDATE OFF
                STATUPDATE OFF;
            """
        else:
            raise AirflowException(f"Unsupported file_format: {self.file_format}. Use 'json' or 'csv'.")
        return copy_sql

    def execute(self, context):
        self.log.info("StageToRedshiftOperator: starting")
        if not all([self.redshift_conn_id, self.aws_iam_role, self.table, self.s3_bucket, self.s3_key]):
            raise AirflowException("StageToRedshiftOperator requires redshift_conn_id, aws_iam_role, table, s3_bucket, s3_key")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info(f"StageToRedshiftOperator: clearing destination table {self.table}")
        # truncate staging table to avoid duplicate staging rows across runs
        redshift.run(f"TRUNCATE TABLE {self.table}")

        copy_sql = self._build_copy_sql(s3_path)
        self.log.info(f"StageToRedshiftOperator: running COPY for {s3_path} into {self.table}")
        logging.debug("COPY SQL: %s", copy_sql)
        redshift.run(copy_sql)
        self.log.info("StageToRedshiftOperator: completed")
