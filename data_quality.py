# plugins/operators/data_quality.py

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class DataQualityOperator(BaseOperator):
    """
    Run a list of data quality tests.
    tests: list of dicts, each with:
      - 'check_sql': SQL returning scalar (e.g. COUNT(*))
      - 'expected_result': can be
           * int  -> equality
           * dict like {'gt':0} or {'eq':5} or {'gte':1}
           * None -> default to > 0
    Example:
      tests = [
        {'check_sql': 'SELECT COUNT(*) FROM users', 'expected_result': {'gt': 0}},
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE user_id IS NULL', 'expected_result': 0}
      ]
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 tests=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def _evaluate(self, actual, expected):
        if expected is None:
            return actual > 0
        if isinstance(expected, int):
            return actual == expected
        if isinstance(expected, dict):
            if 'gt' in expected:
                return actual > expected['gt']
            if 'gte' in expected:
                return actual >= expected['gte']
            if 'lt' in expected:
                return actual < expected['lt']
            if 'lte' in expected:
                return actual <= expected['lte']
            if 'eq' in expected:
                return actual == expected['eq']
        # unknown expected type -> fail safe
        return False

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.tests:
            raise ValueError("DataQualityOperator requires a non-empty tests list")

        failures = []
        for idx, test in enumerate(self.tests, start=1):
            sql = test.get('check_sql')
            expected = test.get('expected_result', None)

            if not sql:
                raise ValueError(f"DataQualityOperator test #{idx} missing 'check_sql'")

            self.log.info(f"DataQualityOperator: running test #{idx}: {sql}")
            result = hook.get_first(sql)
            if result is None or len(result) == 0:
                failures.append((idx, sql, 'No result returned'))
                continue

            actual = result[0]
            if not self._evaluate(actual, expected):
                failures.append((idx, sql, f'Actual={actual} Expected={expected}'))
            else:
                self.log.info(f"DataQualityOperator: test #{idx} passed (Actual={actual}, Expected={expected})")

        if failures:
            for f in failures:
                self.log.error("DataQualityOperator FAILED: test #%s SQL: %s — %s", f[0], f[1], f[2])
            raise ValueError("DataQualityOperator: Data quality checks failed. See logs for details")

        self.log.info("DataQualityOperator: All data quality checks passed")
