"""
Test the new fast metadata query approach
"""
from console_log import log_console

import time
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3
import pandas as pd

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

# Test on both large (XOM-option-5m) and small (AAL-option-1d) tables
test_measurements = [
    ("XOM-option-5m", "17.5M rows"),
    ("AAL-option-1d", "smaller table")
]

log_console("=" * 80)
log_console("Testing FAST Parquet metadata query")
log_console("=" * 80)

for meas, desc in test_measurements:
    log_console(f"\n{'=' * 80}")
    log_console(f"Measurement: {meas} ({desc})")
    log_console(f"{'=' * 80}")

    sql = f"""
    SELECT MIN(min_time) AS start_ts, MAX(max_time) AS end_ts
    FROM system.parquet_files
    WHERE table_name = '{meas}'
    """

    log_console(f"[QUERY] {sql.strip()}")

    t0 = time.time()
    try:
        result = client.query(sql)
        elapsed = time.time() - t0

        df = result.to_pandas() if hasattr(result, "to_pandas") else result

        if df is not None and len(df) > 0:
            row = df.iloc[0]
            start_ns = row.get("start_ts")
            end_ns = row.get("end_ts")

            if pd.notna(start_ns):
                first_ts = pd.Timestamp(start_ns, unit='ns', tz='UTC')
                first_iso = first_ts.isoformat()
            else:
                first_iso = "N/A"

            if pd.notna(end_ns):
                last_ts = pd.Timestamp(end_ns, unit='ns', tz='UTC')
                last_iso = last_ts.isoformat()
            else:
                last_iso = "N/A"

            log_console(f"[RESULT] SUCCESS - Elapsed: {elapsed:.3f}s")
            log_console(f"[RESULT] First timestamp: {first_iso}")
            log_console(f"[RESULT] Last timestamp:  {last_iso}")
        else:
            log_console(f"[RESULT] WARNING - No data found (table might be empty or not exist)")

    except Exception as e:
        elapsed = time.time() - t0
        log_console(f"[ERROR] FAILED after {elapsed:.3f}s: {e}")

log_console(f"\n{'=' * 80}")
log_console("TEST COMPLETED")
log_console("=" * 80)
