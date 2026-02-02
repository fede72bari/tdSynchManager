"""
Test WHERE performance: TAG vs FIELD
Simple focused test as requested by user
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
log_console(f"WHERE Performance Test: TAG vs FIELD")
log_console(f"Measurement: {measurement} (17.5M rows)")
log_console("=" * 80)

# Test 1: WHERE on TAG (expiration is a tag)
log_console("\n" + "=" * 80)
log_console("Test 1: WHERE on TAG")
log_console("Query: WHERE expiration = '2025-11-28'")
log_console("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE expiration = \'2025-11-28\''
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Columns: {list(df.columns)}")
log_console(f"[RESULT] Count: {df.iloc[0][0] if len(df) > 0 else 'N/A'}")

# Test 2: WHERE on FIELD (bid is a field)
log_console("\n" + "=" * 80)
log_console("Test 2: WHERE on FIELD")
log_console("Query: WHERE bid > 10")
log_console("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE bid > 10'
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Columns: {list(df.columns)}")
log_console(f"[RESULT] Count: {df.iloc[0][0] if len(df) > 0 else 'N/A'}")

# Test 3: WHERE on TIME (for comparison - should be fast)
log_console("\n" + "=" * 80)
log_console("Test 3: WHERE on TIME (baseline)")
log_console("Query: WHERE time >= '2025-11-25T00:00:00Z'")
log_console("=" * 80)

query = f'SELECT COUNT(*) FROM "{measurement}" WHERE time >= \'2025-11-25T00:00:00Z\''
log_console(f"[QUERY] {query}")

t0 = time.time()
result = client.query(query)
elapsed = time.time() - t0

df = result.to_pandas() if hasattr(result, "to_pandas") else result
log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
log_console(f"[RESULT] Columns: {list(df.columns)}")
log_console(f"[RESULT] Count: {df.iloc[0][0] if len(df) > 0 else 'N/A'}")

log_console("\n" + "=" * 80)
log_console("TEST COMPLETED")
log_console("=" * 80)
