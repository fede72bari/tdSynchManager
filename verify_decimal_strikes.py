"""
Verify decimal strikes in InfluxDB vs ThetaData raw data
Checks if decimal strikes (e.g., 174.78) exist in both sources
"""
from console_log import log_console
import asyncio
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from tdSynchManager import ThetaSyncManager, ManagerConfig
from clients.ThetaDataV3Client import ThetaDataV3Client


async def main():
    # Configuration
    influx_url = "http://127.0.0.1:8181"
    influx_bucket = "ThetaData"
    influx_token = 'apiv3_reUhe6AEm4FjG4PHtLEW5wbt8MVUtiRtHPgm3Qw487pJFpVj6DlPTRxR1tvcW8bkY1IPM_PQEzHn5b1DVwZc2w'

    cfg = ManagerConfig(
        root_dir=r"C:\Users\Federico\Downloads",
        influx_url=influx_url,
        influx_bucket=influx_bucket,
        influx_token=influx_token,
    )

    # Test parameters
    symbol = "QQQ"
    interval = "5m"
    date_str = "2026-01-16"
    expiration_0dte = "20260116"  # 0DTE
    start_time = "14:00:00"
    end_time = "14:30:00"

    log_console("=" * 80)
    log_console("VERIFICATION: Decimal Strikes in QQQ 0DTE Options")
    log_console("=" * 80)
    log_console(f"Symbol: {symbol}")
    log_console(f"Date: {date_str}")
    log_console(f"Expiration (0DTE): {expiration_0dte}")
    log_console(f"Time Range: {start_time} - {end_time} ET")
    log_console(f"Interval: {interval}")
    log_console()

    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(cfg, client=client)

        # ================================================================
        # STEP 1: Query InfluxDB via Manager
        # ================================================================
        log_console("[STEP 1] Querying InfluxDB via Manager...")
        log_console("-" * 80)

        # Build timestamp range (ET -> UTC)
        et_tz = ZoneInfo("America/New_York")
        start_dt = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M:%S")
        start_dt = start_dt.replace(tzinfo=et_tz)
        end_dt = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M:%S")
        end_dt = end_dt.replace(tzinfo=et_tz)

        log_console(f"Start (ET): {start_dt}")
        log_console(f"End (ET): {end_dt}")

        df_influx, warnings = manager.query_local_data(
            asset="option",
            symbol=symbol,
            interval=interval,
            sink="influxdb",
            start_date=date_str,
            end_date=date_str,
        )

        if df_influx is None or df_influx.empty:
            log_console(f"[X] No data in InfluxDB: {warnings}")
            return

        # Filter by time range and 0DTE expiration
        if 'timestamp' in df_influx.columns:
            df_influx['timestamp'] = pd.to_datetime(df_influx['timestamp'])
            df_influx = df_influx[
                (df_influx['timestamp'] >= start_dt.astimezone(timezone.utc).replace(tzinfo=None)) &
                (df_influx['timestamp'] < end_dt.astimezone(timezone.utc).replace(tzinfo=None))
            ]

        if 'expiration' in df_influx.columns:
            df_influx['expiration'] = df_influx['expiration'].astype(str).str.replace('-', '')
            df_influx = df_influx[df_influx['expiration'] == expiration_0dte]

        log_console(f"[OK] InfluxDB rows after filtering: {len(df_influx)}")

        if df_influx.empty:
            log_console("[X] No data in specified time range")
            return

        # Check strikes
        if 'strike' not in df_influx.columns:
            log_console("[X] No 'strike' column in data")
            return

        df_influx['strike'] = pd.to_numeric(df_influx['strike'], errors='coerce')
        strikes_influx = df_influx['strike'].dropna().unique()
        strikes_influx_sorted = sorted(strikes_influx)

        # Find decimal strikes
        decimal_strikes_influx = [s for s in strikes_influx if s % 1 != 0]
        integer_strikes_influx = [s for s in strikes_influx if s % 1 == 0]

        log_console()
        log_console(f"Total unique strikes: {len(strikes_influx)}")
        log_console(f"Integer strikes: {len(integer_strikes_influx)}")
        log_console(f"Decimal strikes: {len(decimal_strikes_influx)} ({len(decimal_strikes_influx)/len(strikes_influx)*100:.1f}%)")

        if decimal_strikes_influx:
            log_console(f"\n[DETAIL] Sample decimal strikes from InfluxDB (first 10):")
            for strike in sorted(decimal_strikes_influx)[:10]:
                count = len(df_influx[df_influx['strike'] == strike])
                log_console(f"  - {strike:.2f} ({count} rows)")

        # ================================================================
        # STEP 2: Query ThetaData API directly
        # ================================================================
        log_console()
        log_console("[STEP 2] Querying ThetaData API directly...")
        log_console("-" * 80)

        import io
        csv_ohlc, _ = await client.option_history_ohlc(
            symbol=symbol,
            expiration=expiration_0dte,
            date=date_str.replace('-', ''),
            interval=interval,
            strike="*",
            right="both",
            format_type="csv",
            start_time=start_time,  # ET time
        )

        if not csv_ohlc:
            log_console("[X] No data from ThetaData API")
            return

        df_theta = pd.read_csv(io.StringIO(csv_ohlc))
        log_console(f"[OK] ThetaData API rows: {len(df_theta)}")

        if 'strike' not in df_theta.columns:
            log_console("[X] No 'strike' column in ThetaData response")
            log_console(f"Columns: {df_theta.columns.tolist()}")
            return

        # Check strikes from ThetaData
        df_theta['strike'] = pd.to_numeric(df_theta['strike'], errors='coerce')
        strikes_theta = df_theta['strike'].dropna().unique()
        strikes_theta_sorted = sorted(strikes_theta)

        decimal_strikes_theta = [s for s in strikes_theta if s % 1 != 0]
        integer_strikes_theta = [s for s in strikes_theta if s % 1 == 0]

        log_console()
        log_console(f"Total unique strikes: {len(strikes_theta)}")
        log_console(f"Integer strikes: {len(integer_strikes_theta)}")
        log_console(f"Decimal strikes: {len(decimal_strikes_theta)} ({len(decimal_strikes_theta)/len(strikes_theta)*100:.1f}%)")

        if decimal_strikes_theta:
            log_console(f"\n[DETAIL] Sample decimal strikes from ThetaData (first 10):")
            for strike in sorted(decimal_strikes_theta)[:10]:
                count = len(df_theta[df_theta['strike'] == strike])
                log_console(f"  - {strike:.2f} ({count} rows)")

        # ================================================================
        # STEP 3: Compare Results
        # ================================================================
        log_console()
        log_console("[STEP 3] Comparison")
        log_console("=" * 80)

        decimal_in_influx = len(decimal_strikes_influx) > 0
        decimal_in_theta = len(decimal_strikes_theta) > 0

        if decimal_in_influx and decimal_in_theta:
            log_console("[OK] RESULT: Decimal strikes exist in BOTH InfluxDB and ThetaData")
            log_console("   -> This is CORRECT data from ThetaData API")
            log_console("   -> No parsing issue in tdSynchManager")
        elif decimal_in_influx and not decimal_in_theta:
            log_console("[ERROR] RESULT: Decimal strikes ONLY in InfluxDB")
            log_console("   -> PARSING BUG in tdSynchManager!")
            log_console("   -> Data corruption during download/storage")
        elif not decimal_in_influx and decimal_in_theta:
            log_console("[WARNING] RESULT: Decimal strikes ONLY in ThetaData")
            log_console("   -> Filtering issue in query?")
        else:
            log_console("[OK] RESULT: NO decimal strikes in either source")
            log_console("   -> All strikes are integer values")

        # Detailed comparison
        if decimal_in_influx or decimal_in_theta:
            log_console()
            log_console("Detailed Strike Comparison:")
            log_console("-" * 80)

            common_decimals = set(decimal_strikes_influx) & set(decimal_strikes_theta)
            only_influx = set(decimal_strikes_influx) - set(decimal_strikes_theta)
            only_theta = set(decimal_strikes_theta) - set(decimal_strikes_influx)

            if common_decimals:
                log_console(f"Common decimal strikes: {len(common_decimals)}")
                log_console(f"  Examples: {sorted(common_decimals)[:5]}")

            if only_influx:
                log_console(f"Decimal strikes ONLY in InfluxDB: {len(only_influx)}")
                log_console(f"  Examples: {sorted(only_influx)[:5]}")
                log_console("  [WARNING] These might be parsing errors!")

            if only_theta:
                log_console(f"Decimal strikes ONLY in ThetaData: {len(only_theta)}")
                log_console(f"  Examples: {sorted(only_theta)[:5]}")

        # ================================================================
        # STEP 4: Sample Data Inspection
        # ================================================================
        if decimal_strikes_influx:
            log_console()
            log_console("[STEP 4] Sample Data Inspection")
            log_console("=" * 80)

            sample_strike = sorted(decimal_strikes_influx)[0]
            log_console(f"\nInspecting strike: {sample_strike:.2f}")

            # InfluxDB sample
            sample_influx = df_influx[df_influx['strike'] == sample_strike].head(3)
            log_console(f"\nInfluxDB sample ({len(sample_influx)} rows):")
            log_console(sample_influx[['timestamp', 'strike', 'expiration', 'right']].to_string())

            # ThetaData sample
            if sample_strike in strikes_theta:
                sample_theta = df_theta[df_theta['strike'] == sample_strike].head(3)
                log_console(f"\nThetaData sample ({len(sample_theta)} rows):")
                cols = ['strike']
                if 'ms_of_day' in df_theta.columns:
                    cols.append('ms_of_day')
                if 'right' in df_theta.columns:
                    cols.append('right')
                log_console(sample_theta[cols].to_string())


if __name__ == "__main__":
    asyncio.run(main())
