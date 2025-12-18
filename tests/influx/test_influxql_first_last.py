"""
Test InfluxQL FIRST/LAST with language="influxql"
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
print(f"Testing InfluxQL FIRST/LAST with language='influxql'")
print(f"Measurement: {measurement}")
print("=" * 80)

# Test 1: InfluxQL FIRST(bid) with language parameter
print("\n" + "=" * 80)
print("Test 1: InfluxQL FIRST(bid) with language='influxql'")
print("=" * 80)

try:
    query = f'SELECT FIRST(bid) FROM "{measurement}"'
    print(f"[QUERY] {query}")

    t0 = time.time()
    result = client.query(query, language="influxql")  # <-- CRITICAL: language="influxql"
    elapsed = time.time() - t0

    df = result.to_pandas() if hasattr(result, "to_pandas") else result

    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")
    print(f"[RESULT] Columns: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")
    print(f"[RESULT] Data:\n{df}")

    if 'time' in df.columns and len(df) > 0:
        print(f"\n[SUCCESS] First timestamp: {df['time'].iloc[0]}")

except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()

# Test 2: InfluxQL LAST(bid) with language parameter
print("\n" + "=" * 80)
print("Test 2: InfluxQL LAST(bid) with language='influxql'")
print("=" * 80)

try:
    query = f'SELECT LAST(bid) FROM "{measurement}"'
    print(f"[QUERY] {query}")

    t0 = time.time()
    result = client.query(query, language="influxql")  # <-- CRITICAL: language="influxql"
    elapsed = time.time() - t0

    df = result.to_pandas() if hasattr(result, "to_pandas") else result

    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")
    print(f"[RESULT] Columns: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")
    print(f"[RESULT] Data:\n{df}")

    if 'time' in df.columns and len(df) > 0:
        print(f"\n[SUCCESS] Last timestamp: {df['time'].iloc[0]}")

except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()

# Test 3: Combined FIRST and LAST in one query
print("\n" + "=" * 80)
print("Test 3: Combined FIRST(bid), LAST(bid) in one query")
print("=" * 80)

try:
    query = f'SELECT FIRST(bid), LAST(bid) FROM "{measurement}"'
    print(f"[QUERY] {query}")

    t0 = time.time()
    result = client.query(query, language="influxql")
    elapsed = time.time() - t0

    df = result.to_pandas() if hasattr(result, "to_pandas") else result

    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")
    print(f"[RESULT] Columns: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")
    print(f"[RESULT] Data:\n{df}")

except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()

# Test 4: Test on AAL-option-1d (smaller table)
print("\n" + "=" * 80)
print("Test 4: AAL-option-1d (smaller table) - FIRST/LAST combined")
print("=" * 80)

measurement2 = "AAL-option-1d"

try:
    query = f'SELECT FIRST(bid), LAST(bid) FROM "{measurement2}"'
    print(f"[QUERY] {query}")

    t0 = time.time()
    result = client.query(query, language="influxql")
    elapsed = time.time() - t0

    df = result.to_pandas() if hasattr(result, "to_pandas") else result

    print(f"[RESULT] Elapsed: {elapsed:.2f}s")
    print(f"[RESULT] Shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")
    print(f"[RESULT] Columns: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")
    print(f"[RESULT] Data:\n{df}")

except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("TEST COMPLETED")
print("=" * 80)
