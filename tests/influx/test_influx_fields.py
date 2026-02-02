"""
Test InfluxDB FIRST/LAST on actual FIELDS (not time column)
"""
from console_log import log_console

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

log_console("=" * 80)
log_console(f"Testing FIRST/LAST on measurement: {measurement}")
log_console("=" * 80)

# Step 1: See what fields exist
log_console("\n" + "=" * 80)
log_console("Step 1: Query schema to see available fields")
log_console("=" * 80)

try:
    log_console(f"\n[SCHEMA] SQL: SELECT * FROM \"{measurement}\" LIMIT 1")
    result = client.query(f'SELECT * FROM "{measurement}" LIMIT 1')
    df = result.to_pandas() if hasattr(result, "to_pandas") else result
    log_console(f"[SCHEMA] Columns: {list(df.columns)}")
    log_console(f"[SCHEMA] Sample row:\n{df}")
except Exception as e:
    log_console(f"[ERROR] {e}")

# Step 2: Use FIRST/LAST on a real FIELD (not time)
log_console("\n" + "=" * 80)
log_console("Step 2: InfluxQL FIRST/LAST on a real field (e.g., 'bid' or 'ask')")
log_console("=" * 80)

# Try common fields
test_fields = ["bid", "ask", "close", "open", "high", "low", "volume"]

for field in test_fields:
    try:
        log_console(f"\n[TEST] InfluxQL: SELECT FIRST({field}), LAST({field}) FROM \"{measurement}\"")
        t0 = time.time()
        result = client.query(
            f'SELECT FIRST({field}), LAST({field}) FROM "{measurement}"',
            language="influxql"
        )
        elapsed = time.time() - t0
        df = result.to_pandas() if hasattr(result, "to_pandas") else result

        log_console(f"[RESULT] Elapsed: {elapsed:.2f}s")
        log_console(f"[RESULT] Shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")

        if hasattr(df, 'columns') and len(df) > 0:
            log_console(f"[RESULT] Columns: {list(df.columns)}")
            # The time column should show first/last timestamps
            if 'time' in df.columns:
                log_console(f"[SUCCESS] First timestamp: {df['time'].iloc[0]}")
                if len(df) > 1:
                    log_console(f"[SUCCESS] Last timestamp: {df['time'].iloc[1]}")
                else:
                    log_console(f"[INFO] Only one row (might have both FIRST and LAST in same row)")
            break  # Found working field, stop
        else:
            log_console(f"[INFO] Empty result for field '{field}'")

    except Exception as e:
        log_console(f"[INFO] Field '{field}' not found or error: {e}")
        continue

log_console("\n" + "=" * 80)
log_console("TEST COMPLETED")
log_console("=" * 80)
