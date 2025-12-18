"""
Quick test to see schema of XOM-option-5m
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
