"""
Test WHERE performance: TAG vs FIELD
Simple focused test as requested by user
"""

import os
import time
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3

# Load environment variables
load_dotenv()

influx_token = os.getenv('INFLUX_TOKEN')
if not influx_token:
    raise ValueError("INFLUX_TOKEN environment variable is required. Please set it in your .env file.")

client = InfluxDBClient3(
    host=os.getenv('INFLUX_URL', 'http://127.0.0.1:8181'),
    token=influx_token,
    database=os.getenv('INFLUX_BUCKET', 'ThetaData')
)

measurement = "XOM-option-5m"

print("=" * 80)
print(f"WHERE Performance Test: TAG vs FIELD")
print(f"Measurement: {measurement} (17.5M rows)")
print("=" * 80)

# Test 1: WHERE on TAG (expiration is a tag)
print("\n" + "=" * 80)
print("Test 1: WHERE on TAG")
print("Query: WHERE expiration = '2025-11-28'")
print("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE expiration = \'2025-11-28\''
print(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
print(f"[RESULT] Elapsed: {elapsed:.2f}s")
print(f"[RESULT] Columns: {list(df.columns)}")
print(f"[RESULT] Count: {df.iloc[0][0] if len(df) > 0 else 'N/A'}")

# Test 2: WHERE on FIELD (bid is a field)
print("\n" + "=" * 80)
print("Test 2: WHERE on FIELD")
print("Query: WHERE bid > 10")
print("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE bid > 10'
print(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
print(f"[RESULT] Elapsed: {elapsed:.2f}s")
print(f"[RESULT] Columns: {list(df.columns)}")
print(f"[RESULT] Count: {df.iloc[0][0] if len(df) > 0 else 'N/A'}")

# Test 3: WHERE on TIME (for comparison - should be fast)
print("\n" + "=" * 80)
print("Test 3: WHERE on TIME (baseline)")
print("Query: WHERE time >= '2025-11-25T00:00:00Z'")
print("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE time >= \'2025-11-25T00:00:00Z\''
print(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
print(f"[RESULT] Elapsed: {elapsed:.2f}s")
print(f"[RESULT] Columns: {list(df.columns)}")
print(f"[RESULT] Count: {df.iloc[0][0] if len(df) > 0 else 'N/A'}")

print("\n" + "=" * 80)
print("TEST COMPLETED")
print("=" * 80)
