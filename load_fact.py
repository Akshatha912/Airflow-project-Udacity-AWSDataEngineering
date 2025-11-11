# plugins/operators/load_fact.py


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook




class LoadFactOperator(BaseOperator):
ui_color = '#F98866'


@apply_defaults
def __init__(self,
redshift_conn_id='redshift',
table='',
insert_sql='',
mode='append', # 'append' or 'delete-load'
*args, **kwargs):
super(LoadFactOperator, self).__init__(*args, **kwargs)
self.redshift_conn_id = redshift_conn_id
self.table = table
self.insert_sql = insert_sql
self.mode = mode


def execute(self, context):
redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


if self.mode not in ('append', 'delete-load'):
raise ValueError('mode must be "append" or "delete-load"')


if self.mode == 'delete-load':
self.log.info(f'Deleting data from fact table {self.table} before load')
redshift.run(f'DELETE FROM {self.table}')


self.log.info(f'Inserting data into fact table {self.table}')
redshift.run(self.insert_sql)
self.log.info('LoadFactOperator completed')
