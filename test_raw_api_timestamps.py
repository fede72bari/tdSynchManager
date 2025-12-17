"""
Test per verificare il formato RAW dei timestamp restituiti dall'API ThetaData
PRIMA di qualsiasi conversione timezone.
"""
import sys
import os
import asyncio
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
import io

async def test_raw_api_response():
    print("=== Test RAW API Response Timestamps ===\n")

    client = ThetaDataV3Client()

    try:
        # Test with QQQ options 5m for 2025-12-08
        symbol = "QQQ"
        date = "20251208"
        interval = "5m"
        expiration = "20251209"  # 1DTE

        print(f"Fetching: {symbol} options {interval} exp={expiration} date={date}")
        print(f"Expected market hours:")
        print(f"  ET: 09:30-16:00")
        print(f"  UTC: 14:30-21:00 (ET+5 in December)\n")

        # Get OHLC data
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
            print("ERROR: Empty response")
            return

        # Parse WITHOUT any conversion
        lines = csv_text.strip().split('\n')
        header = lines[0]
        first_row = lines[1] if len(lines) > 1 else None
        last_row = lines[-1]

        print(f"=== RAW CSV RESPONSE ===")
        print(f"Total rows: {len(lines) - 1}")
        print(f"\nHeader:")
        print(f"  {header}")
        print(f"\nFirst data row:")
        print(f"  {first_row}")
        print(f"\nLast data row:")
        print(f"  {last_row}")

        # Extract timestamp column (assuming it's in the CSV)
        header_cols = header.split(',')
        if 'timestamp' in header_cols:
            ts_idx = header_cols.index('timestamp')
            first_ts = first_row.split(',')[ts_idx] if first_row else None
            last_ts = last_row.split(',')[ts_idx]

            print(f"\n=== TIMESTAMP ANALYSIS ===")
            print(f"Timestamp column index: {ts_idx}")
            print(f"First timestamp (RAW): {first_ts}")
            print(f"Last timestamp (RAW): {last_ts}")

            # Check format
            if first_ts:
                has_z = first_ts.endswith('Z')
                has_plus = '+' in first_ts
                has_t = 'T' in first_ts

                print(f"\nTimestamp format indicators:")
                print(f"  Has 'T' separator: {has_t}")
                print(f"  Has 'Z' suffix (UTC): {has_z}")
                print(f"  Has '+' (timezone offset): {has_plus}")

                print(f"\n=== INTERPRETATION ===")

                # Parse first timestamp manually
                if 'T' in first_ts:
                    date_part, time_part = first_ts.split('T')
                    time_clean = time_part.replace('Z', '').replace('+00:00', '')
                    print(f"Date: {date_part}")
                    print(f"Time: {time_clean}")

                    hour = int(time_clean.split(':')[0])

                    print(f"\nIf this is ET time:")
                    print(f"  {time_clean} ET should match 09:30-16:00 range")
                    if 9 <= hour <= 16:
                        print(f"  ✓ MATCHES ET market hours")
                    else:
                        print(f"  ✗ OUTSIDE ET market hours")

                    print(f"\nIf this is UTC time:")
                    print(f"  {time_clean} UTC should match 14:30-21:00 range")
                    if 14 <= hour <= 21:
                        print(f"  ✓ MATCHES UTC market hours")
                    else:
                        print(f"  ✗ OUTSIDE UTC market hours")
                else:
                    print(f"Non-ISO format: {first_ts}")

        # Also check underlying_timestamp if present
        if 'underlying_timestamp' in header_cols:
            ut_idx = header_cols.index('underlying_timestamp')
            first_ut = first_row.split(',')[ut_idx] if first_row else None
            print(f"\n=== UNDERLYING_TIMESTAMP ===")
            print(f"First underlying_timestamp (RAW): {first_ut}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_raw_api_response())
