"""Cloud normalization unit checks only, not a Spark/Delta/identity test."""
import importlib.util
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('cloud_contract', Path(__file__).parent / 'databricks/contract.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)


class CloudContractTests(unittest.TestCase):
    def row(self, **changes):
        result = {'tpep_pickup_datetime': datetime(2024, 1, 1, 12),
                  'tpep_dropoff_datetime': datetime(2024, 1, 1, 12, 5),
                  'PULocationID': 264, 'DOLocationID': 265, 'trip_distance': 1.2,
                  'fare_amount': 4.125, 'total_amount': 7.5}
        result.update(changes)
        return result

    def test_unknown_zones_are_valid_and_money_half_up(self):
        row = c.normalize(self.row(), '2024-01')
        self.assertEqual(row['row_status'], 'regular')
        self.assertEqual(row['fare_amount'], Decimal('4.13'))
        self.assertEqual(row['reject_reasons'], [])

    def test_negative_total_is_adjustment(self):
        self.assertEqual(c.normalize(self.row(total_amount=-2), '2024-01')['row_status'], 'adjustment')

    def test_negative_amount_flag_precedes_cent_rounding(self):
        row = c.normalize(self.row(total_amount=-0.001), '2024-01')
        self.assertEqual(row['row_status'], 'adjustment')
        self.assertEqual(row['total_amount'], Decimal('0.00'))

    def test_nonfinite_values_are_quarantined_and_json_is_valid(self):
        import json
        row = c.normalize(self.row(trip_distance=float('nan'), total_amount=float('inf')), '2024-01')
        self.assertEqual(set(row['reject_reasons']), {'invalid_trip_distance', 'invalid_money'})
        self.assertEqual(json.loads(row['raw_payload'])['total_amount'], 'inf')

    def test_missing_bad_and_out_of_month_timestamps(self):
        self.assertIn('invalid_timestamp', c.normalize(self.row(tpep_pickup_datetime='nonsense'), '2024-01')['reject_reasons'])
        self.assertIn('pickup_outside_source_month', c.normalize(self.row(), '2024-02')['reject_reasons'])
        aware = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.assertIn('invalid_timestamp', c.normalize(self.row(tpep_pickup_datetime=aware), '2024-01')['reject_reasons'])

    def test_nonpositive_duration_and_bad_ids(self):
        row = c.normalize(self.row(tpep_dropoff_datetime=datetime(2024, 1, 1), PULocationID=1.5), '2024-01')
        self.assertEqual(set(row['reject_reasons']), {'nonpositive_duration', 'invalid_location_id'})

    def test_duplicate_rows_retained_by_caller_identity(self):
        source = [self.row(), self.row()]
        normalized = [(i, c.normalize(row, '2024-01')) for i, row in enumerate(source)]
        self.assertEqual(len(normalized), 2)
        self.assertNotEqual(normalized[0][0], normalized[1][0])
        self.assertEqual(normalized[0][1], normalized[1][1])

    def test_additive_date_time_fields_remain_json_serializable(self):
        import json
        row = self.row(extra_date=date(2024, 1, 1), extra_time=time(12, 30))
        parsed = json.loads(c.normalize(row, '2024-01')['raw_payload'])
        self.assertEqual(parsed['extra_date'], '2024-01-01')
        self.assertEqual(parsed['extra_time'], '12:30:00')


if __name__ == '__main__':
    unittest.main()
