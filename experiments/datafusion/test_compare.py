import unittest
import pyarrow as pa
from run import canonical_rows, datafusion_context, prepared_datafusion, prepared_duckdb, bridge


class Compatibility(unittest.TestCase):
    def test_exact_null_integer_uuid_duplicate_order_results(self):
        tables = {'core.Values': pa.table({
            'id': pa.array(['00000000-0000-0000-0000-000000000001'] * 3),
            'value': pa.array([-(2**63), 2**63 - 1, None], type=pa.int64()),
            'text': pa.array(['None', None, 'None'], type=pa.string()),
        })}
        sql = 'SELECT id,value,text FROM core."Values" ORDER BY value ASC NULLS LAST'
        ctx = datafusion_context(tables)
        expected = canonical_rows([(tables['core.Values']['id'][0].as_py(), -(2**63), 'None'),
                                   (tables['core.Values']['id'][0].as_py(), 2**63 - 1, None),
                                   (tables['core.Values']['id'][0].as_py(), None, 'None')])
        query = prepared_datafusion(ctx, sql)
        self.assertEqual(query(), expected)
        self.assertEqual(query(), expected)  # cached physical plan must be reusable
        con = bridge.connect(tables, False)  # duplicate IDs intentionally have no PK index
        self.assertEqual(prepared_duckdb(con, sql, 'q')(), expected)
        con.close()

    def test_invalid_sql_and_unknown_relation_fail(self):
        ctx = datafusion_context({'core.Values': pa.table({'value': [1]})})
        for sql in ['SELECT missing FROM core."Values"', 'SELECT * FROM protected.secret', 'SELECT (']:
            with self.assertRaises(Exception):
                prepared_datafusion(ctx, sql)

    def test_empty_and_duplicate_multiplicity(self):
        tables = {'core.Values': pa.table({'id': ['a', 'a'], 'value': [7, 7]})}
        ctx = datafusion_context(tables)
        self.assertEqual(prepared_datafusion(ctx, 'SELECT value FROM core."Values" ORDER BY value')(), [['7'], ['7']])
        self.assertEqual(prepared_datafusion(ctx, 'SELECT value FROM core."Values" WHERE value>9')(), [])

    def test_repeated_top_k_rebuilds_dynamic_filter_execution_state(self):
        pairs = [(f'{i:06}', (i * 7) % 120) for i in range(4000)]
        tables = {'core.Values': pa.table({'id': [p[0] for p in pairs], 'value': [p[1] for p in pairs]})}
        ctx = datafusion_context(tables)
        query = prepared_datafusion(ctx, 'SELECT id,value FROM core."Values" WHERE value>=40 AND value<60 ORDER BY value,id LIMIT 100')
        expected = canonical_rows(sorted((p for p in pairs if 40 <= p[1] < 60), key=lambda p: (p[1], p[0]))[:100])
        for _ in range(20):
            self.assertEqual(query(), expected)


if __name__ == '__main__':
    unittest.main()
