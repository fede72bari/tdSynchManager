"""
Direct InfluxDB v3 query test - trying different methods to get first/last timestamp
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
print(f"Testing different query methods for measurement: {measurement}")
print("=" * 80)

# Test 1: InfluxQL FIRST() and LAST()
print("\n" + "=" * 80)
print("Test 1: InfluxQL FIRST(time) and LAST(time)")
print("=" * 80)

try:
    print(f"\n[TEST 1A] InfluxQL: SELECT FIRST(time) FROM \"{measurement}\"")
    t0 = time.time()
    result = client.query(f'SELECT FIRST(time) FROM "{measurement}"', language="influxql")
    elapsed = time.time() - t0
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Result:\n{df}")
except Exception as e:
    print(f"[ERROR] {e}")

try:
    print(f"\n[TEST 1B] InfluxQL: SELECT LAST(time) FROM \"{measurement}\"")
    t0 = time.time()
    result = client.query(f'SELECT LAST(time) FROM "{measurement}"', language="influxql")
    elapsed = time.time() - t0
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Result:\n{df}")
except Exception as e:
    print(f"[ERROR] {e}")

# Test 2: InfluxQL FIRST(*) to get any field
print("\n" + "=" * 80)
print("Test 2: InfluxQL FIRST(*) and LAST(*)")
print("=" * 80)

try:
    print(f"\n[TEST 2A] InfluxQL: SELECT FIRST(*) FROM \"{measurement}\"")
    t0 = time.time()
    result = client.query(f'SELECT FIRST(*) FROM "{measurement}"', language="influxql")
    elapsed = time.time() - t0
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Result shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")
    if hasattr(df, 'columns'):
        print(f"[RESULT] Columns: {list(df.columns)}")
        if 'time' in df.columns:
            print(f"[RESULT] First timestamp: {df['time'].iloc[0]}")
except Exception as e:
    print(f"[ERROR] {e}")

try:
    print(f"\n[TEST 2B] InfluxQL: SELECT LAST(*) FROM \"{measurement}\"")
    t0 = time.time()
    result = client.query(f'SELECT LAST(*) FROM "{measurement}"', language="influxql")
    elapsed = time.time() - t0
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Result shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")
    if hasattr(df, 'columns'):
        print(f"[RESULT] Columns: {list(df.columns)}")
        if 'time' in df.columns:
            print(f"[RESULT] Last timestamp: {df['time'].iloc[0]}")
except Exception as e:
    print(f"[ERROR] {e}")

# Test 3: SQL selector_first() and selector_last()
print("\n" + "=" * 80)
print("Test 3: SQL selector_first() and selector_last()")
print("=" * 80)

try:
    print(f"\n[TEST 3A] SQL: SELECT selector_first(time, time) FROM \"{measurement}\"")
    t0 = time.time()
    result = client.query(f'SELECT selector_first(time, time) FROM "{measurement}"')
    elapsed = time.time() - t0
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Result:\n{df}")
except Exception as e:
    print(f"[ERROR] {e}")

try:
    print(f"\n[TEST 3B] SQL: SELECT selector_last(time, time) FROM \"{measurement}\"")
    t0 = time.time()
    result = client.query(f'SELECT selector_last(time, time) FROM "{measurement}"')
    elapsed = time.time() - t0
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Result:\n{df}")
except Exception as e:
    print(f"[ERROR] {e}")

# Test 4: Count rows to see table size
print("\n" + "=" * 80)
print("Test 4: Count rows to understand table size")
print("=" * 80)

try:
    print(f"\n[TEST 4] SQL: SELECT COUNT(*) FROM \"{measurement}\"")
    t0 = time.time()
    result = client.query(f'SELECT COUNT(*) FROM "{measurement}"')
    elapsed = time.time() - t0
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Result:\n{df}")
except Exception as e:
    print(f"[ERROR] {e}")

print("\n" + "=" * 80)
print("TEST COMPLETED")
print("=" * 80)
