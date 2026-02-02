"""
Verify InfluxDB data for AAL option 1d after EOD OI shift test.

This script queries InfluxDB to verify:
1. Main data table (AAL-option-1d) has correct data
2. available_data table shows correct coverage
3. OI data has correct effective_date
"""
from console_log import log_console

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3


def verify_influx_data():
    """Query InfluxDB to verify AAL option 1d data."""
    log_console("\n" + "="*80)
    log_console("INFLUXDB DATA VERIFICATION: AAL option 1d")
    log_console("="*80)

    # Get credentials
    creds = get_influx_credentials()

    # Create client
    client = InfluxDBClient3(
        host=creds['url'],
        token=creds['token'],
        database=creds['bucket'],
    )

    log_console(f"\nConnected to InfluxDB: {creds['url']}")
    log_console(f"Database: {creds['bucket']}")

    # =========================================================================
    # 1. Query main data table
    # =========================================================================
    log_console("\n" + "-"*80)
    log_console("1. Main data table: AAL-option-1d")
    log_console("-"*80)

    query_main = """
    SELECT
        time,
        symbol,
        expiration,
        strike,
        right,
        close,
        volume,
        "last_day_OI",
        effective_date_oi
    FROM "AAL-option-1d"
    WHERE time >= '2026-01-05T00:00:00Z' AND time <= '2026-01-09T23:59:59Z'
    ORDER BY time ASC
    LIMIT 20
    """

    log_console(f"\nQuery:\n{query_main}")

    try:
        result = client.query(query_main)
        df = result.to_pandas()

        if df.empty:
            log_console("\n[WARN] No data found in AAL-option-1d table")
        else:
            log_console(f"\n[SUCCESS] Found {len(df)} rows")
            log_console("\nFirst 10 rows:")
            log_console(df.head(10).to_string())

            # Group by date to see coverage
            df['date'] = df['time'].dt.date
            date_counts = df.groupby('date').size()
            log_console(f"\nRows per date:")
            for date, count in date_counts.items():
                log_console(f"  {date}: {count} rows")

            # Check effective_date_oi
            if 'effective_date_oi' in df.columns:
                log_console(f"\neffective_date_oi values:")
                log_console(df.groupby(['date', 'effective_date_oi']).size().to_string())
            else:
                log_console("\n[WARN] effective_date_oi column not found")

    except Exception as e:
        log_console(f"\n[ERROR] Query failed: {e}")

    # =========================================================================
    # 2. Query available_dates table
    # =========================================================================
    log_console("\n" + "-"*80)
    log_console("2. available_dates table: AAL-option-1d_available_dates")
    log_console("-"*80)

    query_available = """
    SELECT *
    FROM "AAL-option-1d_available_dates"
    ORDER BY time DESC
    LIMIT 10
    """

    log_console(f"\nQuery:\n{query_available}")

    try:
        result = client.query(query_available)
        df_avail = result.to_pandas()

        if df_avail.empty:
            log_console("\n[WARN] No data found in AAL-option-1d_available_dates table")
        else:
            log_console(f"\n[SUCCESS] Found {len(df_avail)} records")
            log_console("\nAll records:")
            log_console(df_avail.to_string(index=False))

    except Exception as e:
        log_console(f"\n[ERROR] Query failed: {e}")

    # =========================================================================
    # 3. Detailed check for specific dates
    # =========================================================================
    log_console("\n" + "-"*80)
    log_console("3. Detailed check for downloaded dates")
    log_console("-"*80)

    for test_date in ['2026-01-05', '2026-01-08']:
        log_console(f"\n--- Date: {test_date} ---")

        query_detail = f"""
        SELECT
            time,
            symbol,
            expiration,
            strike,
            right,
            close,
            "last_day_OI",
            effective_date_oi
        FROM "AAL-option-1d"
        WHERE time >= '{test_date}T00:00:00Z' AND time <= '{test_date}T23:59:59Z'
        LIMIT 5
        """

        try:
            result = client.query(query_detail)
            df_detail = result.to_pandas()

            if df_detail.empty:
                log_console(f"  [WARN] No data found for {test_date}")
            else:
                log_console(f"  [SUCCESS] Found {len(df_detail)} rows (showing first 5)")
                log_console(f"\n  Sample data:")
                log_console(df_detail.to_string(index=False))

                # Verify effective_date_oi matches test_date
                if 'effective_date_oi' in df_detail.columns:
                    import pandas as pd
                    # Convert Unix timestamp (nanoseconds) to date string
                    df_detail['effective_date_oi_converted'] = pd.to_datetime(
                        df_detail['effective_date_oi'], unit='ns', utc=True
                    ).dt.tz_convert('America/New_York').dt.date.astype(str)

                    unique_dates = df_detail['effective_date_oi_converted'].unique()
                    log_console(f"\n  Unique effective_date_oi values (converted): {unique_dates}")
                    if len(unique_dates) == 1 and unique_dates[0] == test_date:
                        log_console(f"  [PASS] effective_date_oi = {test_date} (correct!)")
                    else:
                        log_console(f"  [FAIL] effective_date_oi mismatch! Expected {test_date}, got {unique_dates}")

        except Exception as e:
            log_console(f"  [ERROR] Query failed: {e}")

    log_console("\n" + "="*80)
    log_console("VERIFICATION COMPLETE")
    log_console("="*80)

    client.close()


if __name__ == "__main__":
    verify_influx_data()
