"""Compare pure cloud row normalization to local SQL; this does not run Spark."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

import duckdb
from mobility_lakehouse.pipeline import ingest
from scripts.generate_fixture import sample_rows, trip, write_fixture

path = Path(__file__).resolve().parents[1] / 'azure' / 'databricks' / 'contract.py'
spec = importlib.util.spec_from_file_location('mobility_cloud_contract', path)
cloud = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cloud)


class PureContractParityTests(unittest.TestCase):
    def test_fixture_and_numeric_edges_match_local_normalization(self):
        rows = sample_rows() + [
            trip(fare_amount=2.625,total_amount=-2.625),
            trip(fare_amount=1.2349999996,total_amount=-0.001),
            trip(total_amount=-0.0000000001),
            trip(trip_distance=float('nan')),
            trip(fare_amount=float('inf')),
            trip(total_amount=1e30),
            trip(total_amount=None),
            trip(PULocationID=0),
            trip(DOLocationID=266),
            trip(PULocationID=264,DOLocationID=265),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = write_fixture(root / 'source.parquet',rows)
            ingest(source,'2024-01',root / 'warehouse')
            with duckdb.connect(str(root / 'warehouse/warehouse.duckdb'),read_only=True) as con:
                accepted = {r[0]:r[1:] for r in con.execute('''SELECT source_row,pickup_at,dropoff_at,
                    pickup_zone_id,dropoff_zone_id,trip_distance,fare_amount,total_amount,
                    negative_adjustment FROM silver_trips''').fetchall()}
                rejected = dict(con.execute('SELECT source_row,reasons FROM quarantine').fetchall())
            keys = ['pickup_at','dropoff_at','pickup_zone_id','dropoff_zone_id','trip_distance',
                    'fare_amount','total_amount','negative_adjustment']
            for ordinal,row in enumerate(rows):
                normalized = cloud.normalize(row,'2024-01')
                with self.subTest(source_row=ordinal):
                    if ordinal in accepted:
                        self.assertEqual(normalized['reject_reasons'],[])
                        self.assertEqual(tuple(normalized[k] for k in keys),accepted[ordinal])
                    else:
                        self.assertEqual(normalized['reject_reasons'],rejected[ordinal])


if __name__ == '__main__':
    unittest.main()
