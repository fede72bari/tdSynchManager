"""
Debug OI effective_date mismatch for 2026-01-05.

Query InfluxDB to find exactly which contracts have wrong effective_date.
"""
from console_log import log_console

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3


def debug_oi_dates():
    """Find contracts with wrong effective_date_oi."""
    log_console("\n" + "="*80)
    log_console("DEBUG: OI effective_date_oi mismatch for 2026-01-05")
    log_console("="*80)

    # Get credentials
    creds = get_influx_credentials()

    # Create client
    client = InfluxDBClient3(
        host=creds['url'],
        token=creds['token'],
        database=creds['bucket'],
    )

    # Query all 2026-01-05 data with OI fields
    query = """
    SELECT
        time,
        symbol,
        expiration,
        strike,
        right,
        close,
        "last_day_OI",
        timestamp_oi,
        effective_date_oi
    FROM "AAL-option-1d"
    WHERE time >= '2026-01-05T00:00:00Z' AND time <= '2026-01-05T23:59:59Z'
    ORDER BY time ASC
    """

    log_console(f"\nQuerying all 2026-01-05 data...")

    result = client.query(query)
    df = result.to_pandas()

    log_console(f"Total rows: {len(df)}")

    # Convert effective_date_oi from Unix timestamp to date
    df['effective_date_oi_converted'] = pd.to_datetime(
        df['effective_date_oi'], unit='ns', utc=True
    ).dt.tz_convert('America/New_York').dt.date.astype(str)

    # Also convert timestamp_oi
    df['timestamp_oi_converted'] = pd.to_datetime(
        df['timestamp_oi'], unit='ns', utc=True
    ).dt.tz_convert('America/New_York')

    # Group by effective_date
    log_console(f"\nGrouping by effective_date_oi:")
    date_counts = df.groupby('effective_date_oi_converted').size()
    for date, count in date_counts.items():
        log_console(f"  {date}: {count} contracts ({count/len(df)*100:.1f}%)")

    # Find contracts with wrong date (not 2026-01-05)
    wrong_date = df[df['effective_date_oi_converted'] != '2026-01-05']

    if wrong_date.empty:
        log_console("\n[SUCCESS] All contracts have correct effective_date = 2026-01-05")
    else:
        log_console(f"\n[PROBLEM] Found {len(wrong_date)} contracts with WRONG effective_date:")
        log_console(f"\nWrong contracts (showing all):")
        log_console(wrong_date[['time', 'expiration', 'strike', 'right', 'close',
                          'last_day_OI', 'timestamp_oi_converted',
                          'effective_date_oi_converted']].to_string(index=False))

        # Check if these are specific expirations
        log_console(f"\nExpirations with wrong dates:")
        exp_counts = wrong_date.groupby('expiration').size()
        for exp, count in exp_counts.items():
            log_console(f"  {exp}: {count} contracts")

        # Check timestamp_oi values
        log_console(f"\nTimestamp_oi values for wrong contracts:")
        log_console(wrong_date[['expiration', 'strike', 'right',
                          'timestamp_oi_converted', 'effective_date_oi_converted']].to_string(index=False))

    # Show correct contracts sample
    correct_date = df[df['effective_date_oi_converted'] == '2026-01-05']
    log_console(f"\n[INFO] Sample of CORRECT contracts (first 5):")
    log_console(correct_date[['time', 'expiration', 'strike', 'right',
                        'timestamp_oi_converted', 'effective_date_oi_converted']].head().to_string(index=False))

    log_console("\n" + "="*80)
    log_console("DEBUG COMPLETE")
    log_console("="*80)

    client.close()


if __name__ == "__main__":
    debug_oi_dates()
