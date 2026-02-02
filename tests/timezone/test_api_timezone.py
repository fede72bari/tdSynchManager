"""
Test per verificare il fuso orario dei timestamp restituiti dall'API ThetaData.
"""
from console_log import log_console
import sys
import os
import asyncio
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from tdSynchManager.config import ManagerConfig
import pandas as pd
import io

async def test_api_timestamp_format():
    log_console("=== Testing ThetaData API Timestamp Format ===\n")

    # Create client
    cfg = ManagerConfig(root_dir=r"tests/data", max_concurrency=1)
    client = ThetaDataV3Client()
    await client.connect()

    try:
        # Test with QQQ options 5m for 2025-12-08
        symbol = "QQQ"
        date = "20251208"
        interval = "5m"
        expiration = "20251209"  # Next day expiration

        log_console(f"Fetching {symbol} options {interval} data for {date}")
        log_console(f"Expiration: {expiration}")
        log_console(f"Expected ET market hours: 09:30-16:00")
        log_console(f"Expected UTC market hours: 14:30-21:00 (ET+5 in winter)\n")

        csv_text, url = await client.option_history_ohlc(
            symbol=symbol,
            expiration=expiration,
            date=date,
            interval=interval,
            strike="*",
            right="both",
            format_type="csv"
        )

        if not csv_text:
            log_console("ERROR: Empty response from API")
            return

        # Parse CSV
        df = pd.read_csv(io.StringIO(csv_text), dtype=str)

        log_console(f"=== RAW API RESPONSE ===")
        log_console(f"Total rows: {len(df)}")
        log_console(f"\nFirst 3 rows:")
        log_console(df.head(3))

        log_console(f"\n=== TIMESTAMP ANALYSIS ===")
        log_console(f"First timestamp (RAW string): {df['timestamp'].iloc[0]}")
        log_console(f"Last timestamp (RAW string): {df['timestamp'].iloc[-1]}")

        # Try parsing
        df['ts_parsed'] = pd.to_datetime(df['timestamp'], errors='coerce')
        log_console(f"\nFirst timestamp (parsed, no tz): {df['ts_parsed'].iloc[0]}")
        log_console(f"Last timestamp (parsed, no tz): {df['ts_parsed'].iloc[-1]}")

        # Check if timestamps have timezone info
        has_tz = df['timestamp'].iloc[0].endswith('Z') or '+' in df['timestamp'].iloc[0] or df['timestamp'].iloc[0].endswith('00')
        log_console(f"\nTimezone indicator in string: {has_tz}")

        # Check underlying_timestamp if present
        if 'underlying_timestamp' in df.columns:
            log_console(f"\nFirst underlying_timestamp (RAW): {df['underlying_timestamp'].iloc[0]}")

        log_console(f"\n=== INTERPRETATION ===")
        first_ts = df['ts_parsed'].iloc[0]
        last_ts = df['ts_parsed'].iloc[-1]

        log_console(f"If these are ET times:")
        log_console(f"  First bar: {first_ts} ET → {first_ts.tz_localize('America/New_York').tz_convert('UTC')} UTC")
        log_console(f"  Last bar: {last_ts} ET → {last_ts.tz_localize('America/New_York').tz_convert('UTC')} UTC")

        log_console(f"\nIf these are already UTC times:")
        log_console(f"  First bar: {first_ts} UTC → {pd.Timestamp(first_ts, tz='UTC').tz_convert('America/New_York')} ET")
        log_console(f"  Last bar: {last_ts} UTC → {pd.Timestamp(last_ts, tz='UTC').tz_convert('America/New_York')} ET")

        log_console(f"\n=== EXPECTED vs ACTUAL ===")
        log_console(f"Market should be:")
        log_console(f"  ET: 09:30:00 to 16:00:00")
        log_console(f"  UTC: 14:30:00 to 21:00:00")

        log_console(f"\nActual timestamps in CSV:")
        log_console(f"  Start: {first_ts.strftime('%H:%M:%S')}")
        log_console(f"  End: {last_ts.strftime('%H:%M:%S')}")

    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(test_api_timestamp_format())
