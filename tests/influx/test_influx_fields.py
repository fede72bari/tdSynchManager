"""
Test InfluxDB FIRST/LAST on actual FIELDS (not time column)
"""

import time
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3

# Get InfluxDB credentials
influx = get_influx_credentials()
influx_token = influx['token']
influx_url = influx.get('url', 'http://127.0.0.1:8181')
influx_bucket = influx.get('bucket', 'ThetaData')

client = InfluxDBClient3(
    host=influx_url,
    token=influx_token,
    database=influx_bucket
)

measurement = "XOM-option-5m"

print("=" * 80)
print(f"Testing FIRST/LAST on measurement: {measurement}")
print("=" * 80)

# Step 1: See what fields exist
print("\n" + "=" * 80)
print("Step 1: Query schema to see available fields")
print("=" * 80)

try:
    print(f"\n[SCHEMA] SQL: SELECT * FROM \"{measurement}\" LIMIT 1")
    result = client.query(f'SELECT * FROM "{measurement}" LIMIT 1')
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    print(f"[SCHEMA] Columns: {list(df.columns)}")
    print(f"[SCHEMA] Sample row:\n{df}")
except Exception as e:
    print(f"[ERROR] {e}")

# Step 2: Use FIRST/LAST on a real FIELD (not time)
print("\n" + "=" * 80)
print("Step 2: InfluxQL FIRST/LAST on a real field (e.g., 'bid' or 'ask')")
print("=" * 80)

# Try common fields
test_fields = ["bid", "ask", "close", "open", "high", "low", "volume"]

for field in test_fields:
    try:
        print(f"\n[TEST] InfluxQL: SELECT FIRST({field}), LAST({field}) FROM \"{measurement}\"")
        t0 = time.time()
        result = client.query(
            f'SELECT FIRST({field}), LAST({field}) FROM "{measurement}"',
            language="influxql"
        )
        elapsed = time.time() - t0
        df = result.to_pandas() if hasattr(result, "to_pandas") else result

        print(f"[RESULT] Elapsed: {elapsed:.2f}s")
        print(f"[RESULT] Shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")

        if hasattr(df, 'columns') and len(df) > 0:
            print(f"[RESULT] Columns: {list(df.columns)}")
            # The time column should show first/last timestamps
            if 'time' in df.columns:
                print(f"[SUCCESS] First timestamp: {df['time'].iloc[0]}")
                if len(df) > 1:
                    print(f"[SUCCESS] Last timestamp: {df['time'].iloc[1]}")
                else:
                    print(f"[INFO] Only one row (might have both FIRST and LAST in same row)")
            break  # Found working field, stop
        else:
            print(f"[INFO] Empty result for field '{field}'")

    except Exception as e:
        print(f"[INFO] Field '{field}' not found or error: {e}")
        continue

print("\n" + "=" * 80)
print("TEST COMPLETED")
print("=" * 80)
