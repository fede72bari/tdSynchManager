"""
Test query performance with WHERE on tag and field
"""
from console_log import log_console

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

log_console("=" * 80)
log_console(f"Testing WHERE performance on tag and field")
log_console(f"Measurement: {measurement} (17.5M rows)")
log_console("=" * 80)

# Test 1: WHERE on tag only (symbol, expiration, right, strike)
log_console("\n" + "=" * 80)
log_console("Test 1: WHERE on TAG (expiration)")
log_console("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE expiration = \'2025-11-28\''
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Count: {df.iloc[0]['count'] if len(df) > 0 else 'N/A'}")

# Test 2: WHERE on field only (bid)
log_console("\n" + "=" * 80)
log_console("Test 2: WHERE on FIELD (bid > 10)")
log_console("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE bid > 10'
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Count: {df.iloc[0]['count'] if len(df) > 0 else 'N/A'}")

# Test 3: WHERE on tag + field
log_console("\n" + "=" * 80)
log_console("Test 3: WHERE on TAG + FIELD (expiration + bid)")
log_console("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE expiration = \'2025-11-28\' AND bid > 10'
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Count: {df.iloc[0]['count'] if len(df) > 0 else 'N/A'}")

# Test 4: WHERE on multiple tags
log_console("\n" + "=" * 80)
log_console("Test 4: WHERE on multiple TAGs (expiration + right + strike)")
log_console("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE expiration = \'2025-11-28\' AND right = \'call\' AND strike = 101.0'
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Count: {df.iloc[0]['count'] if len(df) > 0 else 'N/A'}")

# Test 5: WHERE on time range (should be fast)
log_console("\n" + "=" * 80)
log_console("Test 5: WHERE on TIME (last day)")
log_console("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE time >= \'2025-11-25T00:00:00Z\''
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Count: {df.iloc[0]['count'] if len(df) > 0 else 'N/A'}")

# Test 6: FIRST/LAST with time range
log_console("\n" + "=" * 80)
log_console("Test 6: FIRST(bid) with time range (2025 only)")
log_console("=" * 80)

query = f'SELECT FIRST(bid) FROM "{measurement}" WHERE time >= \'2025-01-01T00:00:00Z\''
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query, language="influxql")
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Data:\n{df}")

log_console("\n" + "=" * 80)
log_console("TEST COMPLETED")
log_console("=" * 80)
