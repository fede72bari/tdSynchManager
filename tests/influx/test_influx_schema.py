"""
Quick test to see schema of XOM-option-5m
"""

import time
from influxdb_client_3 import InfluxDBClient3

influx_token = 'apiv3_reUhe6AEm4FjG4PHtLEW5wbt8MVUtiRtHPgm3Qw487pJFpVj6DlPTRxR1tvcW8bkY1IPM_PQEzHn5b1DVwZc2w'

client = InfluxDBClient3(
    host="http://127.0.0.1:8181",
    token=influx_token,
    database="ThetaData"
)

measurement = "XOM-option-5m"

# Use SQL but with time filter to reduce data volume
query = f'SELECT * FROM "{measurement}" WHERE time >= \'2025-11-25T00:00:00Z\' LIMIT 1'

print(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result

print(f"\n[RESULT] Elapsed: {elapsed:.2f}s")
print(f"[RESULT] Rows: {len(df)}")
print(f"[RESULT] Columns ({len(df.columns)}): {list(df.columns)}")
print(f"\n[RESULT] Sample row:")
print(df.iloc[0] if len(df) > 0 else "No rows")
