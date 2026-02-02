"""
Test per verificare il formato RAW dei timestamp restituiti dall'API ThetaData
PRIMA di qualsiasi conversione timezone.
"""
from console_log import log_console
import sys
import os
import asyncio
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
import io

async def test_raw_api_response():
    log_console("=== Test RAW API Response Timestamps ===\n")

    client = ThetaDataV3Client()

    try:
        # Test with QQQ options 5m for 2025-12-08
        symbol = "QQQ"
        date = "20251208"
        interval = "5m"
        expiration = "20251209"  # 1DTE

        log_console(f"Fetching: {symbol} options {interval} exp={expiration} date={date}")
        log_console(f"Expected market hours:")
        log_console(f"  ET: 09:30-16:00")
        log_console(f"  UTC: 14:30-21:00 (ET+5 in December)\n")

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
            log_console("ERROR: Empty response")
            return

        # Parse WITHOUT any conversion
        lines = csv_text.strip().split('\n')
        header = lines[0]
        first_row = lines[1] if len(lines) > 1 else None
        last_row = lines[-1]

        log_console(f"=== RAW CSV RESPONSE ===")
        log_console(f"Total rows: {len(lines) - 1}")
        log_console(f"\nHeader:")
        log_console(f"  {header}")
        log_console(f"\nFirst data row:")
        log_console(f"  {first_row}")
        log_console(f"\nLast data row:")
        log_console(f"  {last_row}")

        # Extract timestamp column (assuming it's in the CSV)
        header_cols = header.split(',')
        if 'timestamp' in header_cols:
            ts_idx = header_cols.index('timestamp')
            first_ts = first_row.split(',')[ts_idx] if first_row else None
            last_ts = last_row.split(',')[ts_idx]

            log_console(f"\n=== TIMESTAMP ANALYSIS ===")
            log_console(f"Timestamp column index: {ts_idx}")
            log_console(f"First timestamp (RAW): {first_ts}")
            log_console(f"Last timestamp (RAW): {last_ts}")

            # Check format
            if first_ts:
                has_z = first_ts.endswith('Z')
                has_plus = '+' in first_ts
                has_t = 'T' in first_ts

                log_console(f"\nTimestamp format indicators:")
                log_console(f"  Has 'T' separator: {has_t}")
                log_console(f"  Has 'Z' suffix (UTC): {has_z}")
                log_console(f"  Has '+' (timezone offset): {has_plus}")

                log_console(f"\n=== INTERPRETATION ===")

                # Parse first timestamp manually
                if 'T' in first_ts:
                    date_part, time_part = first_ts.split('T')
                    time_clean = time_part.replace('Z', '').replace('+00:00', '')
                    log_console(f"Date: {date_part}")
                    log_console(f"Time: {time_clean}")

                    hour = int(time_clean.split(':')[0])

                    log_console(f"\nIf this is ET time:")
                    log_console(f"  {time_clean} ET should match 09:30-16:00 range")
                    if 9 <= hour <= 16:
                        log_console(f"  ✓ MATCHES ET market hours")
                    else:
                        log_console(f"  ✗ OUTSIDE ET market hours")

                    log_console(f"\nIf this is UTC time:")
                    log_console(f"  {time_clean} UTC should match 14:30-21:00 range")
                    if 14 <= hour <= 21:
                        log_console(f"  ✓ MATCHES UTC market hours")
                    else:
                        log_console(f"  ✗ OUTSIDE UTC market hours")
                else:
                    log_console(f"Non-ISO format: {first_ts}")

        # Also check underlying_timestamp if present
        if 'underlying_timestamp' in header_cols:
            ut_idx = header_cols.index('underlying_timestamp')
            first_ut = first_row.split(',')[ut_idx] if first_row else None
            log_console(f"\n=== UNDERLYING_TIMESTAMP ===")
            log_console(f"First underlying_timestamp (RAW): {first_ut}")

    except Exception as e:
        log_console(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_raw_api_response())
