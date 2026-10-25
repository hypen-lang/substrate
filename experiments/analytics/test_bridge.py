"""Checks for the limited trusted-host benchmark encoding and execution boundary."""
import unittest

from run import arrow_tables, canonical, connect


class BridgeTests(unittest.TestCase):
    def fixture(self):
        return [{"name": "core.Person", "fields": [
            {"name": "id", "type": "utf8"},
            {"name": "name", "type": "utf8"},
            {"name": "amount", "type": "int64"}], "columns": [
                ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"],
                ["same", "same"], [2**53 + 1, -(2**63)]]}]

    def test_exact_int64_uuid_order_and_bag_semantics(self):
        arrow = arrow_tables(self.fixture())
        self.assertEqual(arrow["core.Person"]["amount"].to_pylist(), [2**53 + 1, -(2**63)])
        for materialized in (False, True):
            con = connect(arrow, materialized)
            self.assertEqual(canonical(con.execute('SELECT amount FROM core."Person" ORDER BY id').fetchall()),
                             [[str(2**53 + 1)], [str(-(2**63))]])
            self.assertEqual(con.execute('SELECT name FROM core."Person" ORDER BY id').fetchall(),
                             [("same",), ("same",)])
            self.assertEqual(con.execute("SELECT current_setting('enable_external_access')").fetchone(), (False,))
            con.close()

    def test_int64_overflow_is_rejected(self):
        fixture = self.fixture()
        fixture[0]["columns"][2][0] = 2**63
        with self.assertRaises(OverflowError):
            arrow_tables(fixture)

    def test_column_count_mismatch_is_rejected(self):
        fixture = self.fixture()
        fixture[0]["columns"].pop()
        with self.assertRaises(ValueError):
            arrow_tables(fixture)


if __name__ == "__main__":
    unittest.main()
