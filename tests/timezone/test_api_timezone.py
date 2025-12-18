"""
Test per verificare il fuso orario dei timestamp restituiti dall'API ThetaData.
"""
import sys
import os
import asyncio
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from tdSynchManager.config import ManagerConfig
import pandas as pd
import io

async def test_api_timestamp_format():
    print("=== Testing ThetaData API Timestamp Format ===\n")

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

        print(f"Fetching {symbol} options {interval} data for {date}")
        print(f"Expiration: {expiration}")
        print(f"Expected ET market hours: 09:30-16:00")
        print(f"Expected UTC market hours: 14:30-21:00 (ET+5 in winter)\n")

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
            print("ERROR: Empty response from API")
            return

        # Parse CSV
        df = pd.read_csv(io.StringIO(csv_text), dtype=str)

        print(f"=== RAW API RESPONSE ===")
        print(f"Total rows: {len(df)}")
        print(f"\nFirst 3 rows:")
        print(df.head(3))

        print(f"\n=== TIMESTAMP ANALYSIS ===")
        print(f"First timestamp (RAW string): {df['timestamp'].iloc[0]}")
        print(f"Last timestamp (RAW string): {df['timestamp'].iloc[-1]}")

        # Try parsing
        df['ts_parsed'] = pd.to_datetime(df['timestamp'], errors='coerce')
        print(f"\nFirst timestamp (parsed, no tz): {df['ts_parsed'].iloc[0]}")
        print(f"Last timestamp (parsed, no tz): {df['ts_parsed'].iloc[-1]}")

        # Check if timestamps have timezone info
        has_tz = df['timestamp'].iloc[0].endswith('Z') or '+' in df['timestamp'].iloc[0] or df['timestamp'].iloc[0].endswith('00')
        print(f"\nTimezone indicator in string: {has_tz}")

        # Check underlying_timestamp if present
        if 'underlying_timestamp' in df.columns:
            print(f"\nFirst underlying_timestamp (RAW): {df['underlying_timestamp'].iloc[0]}")

        print(f"\n=== INTERPRETATION ===")
        first_ts = df['ts_parsed'].iloc[0]
        last_ts = df['ts_parsed'].iloc[-1]

        print(f"If these are ET times:")
        print(f"  First bar: {first_ts} ET → {first_ts.tz_localize('America/New_York').tz_convert('UTC')} UTC")
        print(f"  Last bar: {last_ts} ET → {last_ts.tz_localize('America/New_York').tz_convert('UTC')} UTC")

        print(f"\nIf these are already UTC times:")
        print(f"  First bar: {first_ts} UTC → {pd.Timestamp(first_ts, tz='UTC').tz_convert('America/New_York')} ET")
        print(f"  Last bar: {last_ts} UTC → {pd.Timestamp(last_ts, tz='UTC').tz_convert('America/New_York')} ET")

        print(f"\n=== EXPECTED vs ACTUAL ===")
        print(f"Market should be:")
        print(f"  ET: 09:30:00 to 16:00:00")
        print(f"  UTC: 14:30:00 to 21:00:00")

        print(f"\nActual timestamps in CSV:")
        print(f"  Start: {first_ts.strftime('%H:%M:%S')}")
        print(f"  End: {last_ts.strftime('%H:%M:%S')}")

    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(test_api_timestamp_format())
