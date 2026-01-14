"""
Verify InfluxDB data for AAL option 1d after EOD OI shift test.

This script queries InfluxDB to verify:
1. Main data table (AAL-option-1d) has correct data
2. available_data table shows correct coverage
3. OI data has correct effective_date
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3


def verify_influx_data():
    """Query InfluxDB to verify AAL option 1d data."""
    print("\n" + "="*80)
    print("INFLUXDB DATA VERIFICATION: AAL option 1d")
    print("="*80)

    # Get credentials
    creds = get_influx_credentials()

    # Create client
    client = InfluxDBClient3(
        host=creds['url'],
        token=creds['token'],
        database=creds['bucket'],
    )

    print(f"\nConnected to InfluxDB: {creds['url']}")
    print(f"Database: {creds['bucket']}")

    # =========================================================================
    # 1. Query main data table
    # =========================================================================
    print("\n" + "-"*80)
    print("1. Main data table: AAL-option-1d")
    print("-"*80)

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

    print(f"\nQuery:\n{query_main}")

    try:
        result = client.query(query_main)
        df = result.to_pandas()

        if df.empty:
            print("\n[WARN] No data found in AAL-option-1d table")
        else:
            print(f"\n[SUCCESS] Found {len(df)} rows")
            print("\nFirst 10 rows:")
            print(df.head(10).to_string())

            # Group by date to see coverage
            df['date'] = df['time'].dt.date
            date_counts = df.groupby('date').size()
            print(f"\nRows per date:")
            for date, count in date_counts.items():
                print(f"  {date}: {count} rows")

            # Check effective_date_oi
            if 'effective_date_oi' in df.columns:
                print(f"\neffective_date_oi values:")
                print(df.groupby(['date', 'effective_date_oi']).size().to_string())
            else:
                print("\n[WARN] effective_date_oi column not found")

    except Exception as e:
        print(f"\n[ERROR] Query failed: {e}")

    # =========================================================================
    # 2. Query available_dates table
    # =========================================================================
    print("\n" + "-"*80)
    print("2. available_dates table: AAL-option-1d_available_dates")
    print("-"*80)

    query_available = """
    SELECT *
    FROM "AAL-option-1d_available_dates"
    ORDER BY time DESC
    LIMIT 10
    """

    print(f"\nQuery:\n{query_available}")

    try:
        result = client.query(query_available)
        df_avail = result.to_pandas()

        if df_avail.empty:
            print("\n[WARN] No data found in AAL-option-1d_available_dates table")
        else:
            print(f"\n[SUCCESS] Found {len(df_avail)} records")
            print("\nAll records:")
            print(df_avail.to_string(index=False))

    except Exception as e:
        print(f"\n[ERROR] Query failed: {e}")

    # =========================================================================
    # 3. Detailed check for specific dates
    # =========================================================================
    print("\n" + "-"*80)
    print("3. Detailed check for downloaded dates")
    print("-"*80)

    for test_date in ['2026-01-05', '2026-01-08']:
        print(f"\n--- Date: {test_date} ---")

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
                print(f"  [WARN] No data found for {test_date}")
            else:
                print(f"  [SUCCESS] Found {len(df_detail)} rows (showing first 5)")
                print(f"\n  Sample data:")
                print(df_detail.to_string(index=False))

                # Verify effective_date_oi matches test_date
                if 'effective_date_oi' in df_detail.columns:
                    import pandas as pd
                    # Convert Unix timestamp (nanoseconds) to date string
                    df_detail['effective_date_oi_converted'] = pd.to_datetime(
                        df_detail['effective_date_oi'], unit='ns', utc=True
                    ).dt.tz_convert('America/New_York').dt.date.astype(str)

                    unique_dates = df_detail['effective_date_oi_converted'].unique()
                    print(f"\n  Unique effective_date_oi values (converted): {unique_dates}")
                    if len(unique_dates) == 1 and unique_dates[0] == test_date:
                        print(f"  [PASS] effective_date_oi = {test_date} (correct!)")
                    else:
                        print(f"  [FAIL] effective_date_oi mismatch! Expected {test_date}, got {unique_dates}")

        except Exception as e:
            print(f"  [ERROR] Query failed: {e}")

    print("\n" + "="*80)
    print("VERIFICATION COMPLETE")
    print("="*80)

    client.close()


if __name__ == "__main__":
    verify_influx_data()
