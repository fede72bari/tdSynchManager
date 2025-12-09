"""
Test the new fast metadata query approach
"""

import time
from influxdb_client_3 import InfluxDBClient3
import pandas as pd

influx_token = 'apiv3_reUhe6AEm4FjG4PHtLEW5wbt8MVUtiRtHPgm3Qw487pJFpVj6DlPTRxR1tvcW8bkY1IPM_PQEzHn5b1DVwZc2w'

client = InfluxDBClient3(
    host="http://127.0.0.1:8181",
    token=influx_token,
    database="ThetaData"
)

# Test on both large (XOM-option-5m) and small (AAL-option-1d) tables
test_measurements = [
    ("XOM-option-5m", "17.5M rows"),
    ("AAL-option-1d", "smaller table")
]

print("=" * 80)
print("Testing FAST Parquet metadata query")
print("=" * 80)

for meas, desc in test_measurements:
    print(f"\n{'=' * 80}")
    print(f"Measurement: {meas} ({desc})")
    print(f"{'=' * 80}")

    sql = f"""
    SELECT MIN(min_time) AS start_ts, MAX(max_time) AS end_ts
    FROM system.parquet_files
    WHERE table_name = '{meas}'
    """

    print(f"[QUERY] {sql.strip()}")

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

            print(f"[RESULT] SUCCESS - Elapsed: {elapsed:.3f}s")
            print(f"[RESULT] First timestamp: {first_iso}")
            print(f"[RESULT] Last timestamp:  {last_iso}")
        else:
            print(f"[RESULT] WARNING - No data found (table might be empty or not exist)")

    except Exception as e:
        elapsed = time.time() - t0
        print(f"[ERROR] FAILED after {elapsed:.3f}s: {e}")

print(f"\n{'=' * 80}")
print("TEST COMPLETED")
print("=" * 80)
