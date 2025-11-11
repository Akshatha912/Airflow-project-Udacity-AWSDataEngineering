# plugins/operators/load_fact.py

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class LoadFactOperator(BaseOperator):
    """
    Loads fact table.
    mode: 'append' or 'delete-load' (truncate before insert)
    insert_sql: SQL string to insert/select rows into the fact table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 insert_sql='',
                 mode='append',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql
        self.mode = mode

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode not in ('append', 'delete-load'):
            raise ValueError("LoadFactOperator mode must be 'append' or 'delete-load'")

        if self.mode == 'delete-load':
            self.log.info(f"LoadFactOperator: deleting data from {self.table}")
            hook.run(f"DELETE FROM {self.table}")

        self.log.info(f"LoadFactOperator: inserting data into {self.table}")
        if not self.insert_sql:
            raise ValueError("LoadFactOperator requires insert_sql parameter")
        hook.run(self.insert_sql)
        self.log.info("LoadFactOperator: completed")
