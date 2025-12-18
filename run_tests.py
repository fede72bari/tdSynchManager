"""
Test Data Consistency - Real Execution
Tests real-time validation, retry, InfluxDB verification, and post-hoc coherence checking
"""

import asyncio
import pandas as pd
from src.tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from src.tdSynchManager.manager import ThetaSyncManager, install_td_server_error_logger
from src.tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from src.tdSynchManager.coherence import CoherenceChecker
from src.tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3

# Load credentials from .credentials.json
influx = get_influx_credentials()
influx_url = influx['url']
influx_bucket = influx['bucket']
influx_token = influx['token']
symbols = ["TLRY"]

cfg = ManagerConfig(
    root_dir=r"C:\Users\Federico\Downloads",
    max_concurrency=80,
    max_file_mb=16,
    overlap_seconds=60,
    influx_url=influx_url,
    influx_bucket=influx_bucket,
    influx_token=influx_token,
    influx_org=None,
    influx_precision="nanosecond",
    influx_measure_prefix="",
    influx_write_batch=5000,

    # Enable validation with strict mode
    enable_data_validation=True,
    validation_strict_mode=True,
)


async def test_1_real_time_download():
    """Test 1: Real-time download with validation and retry"""
    print("\n" + "="*80)
    print("TEST 1: Real-Time Download with Validation and Retry")
    print("="*80)

    tasks = [
        Task(
            asset="option",
            symbols=symbols,
            intervals=["1d"],
            sink="influxdb",
            enrich_bar_greeks=True,
            enrich_tick_greeks=True,
            first_date_override="20250821",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="skip")
        ),
        Task(
            asset="option",
            symbols=symbols,
            intervals=["5m"],
            sink="influxdb",
            enrich_bar_greeks=True,
            enrich_tick_greeks=True,
            first_date_override="20250821",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="skip")
        ),
    ]

    print("\n[TEST] Running download tasks...")
    print("Expected behavior:")
    print("  - Download data for TLRY options")
    print("  - Validate completeness (candles, columns, volume)")
    print("  - Retry up to N times if validation fails")
    print("  - Write to InfluxDB with verification")
    print("  - Retry only missing rows if partial write detected")
    print("")

    async with ThetaDataV3Client() as client:
        install_td_server_error_logger(client)
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

    print("\n[TEST] Download completed. Check logs above for:")
    print("  [OK] [VALIDATION] messages showing validation results")
    print("  [OK] [INFLUX][WRITE] messages showing write operations")
    print("  [OK] Retry attempts if validation failed")
    print("  [OK] Verification after write")


async def test_2_list_measurements():
    """Test 2: List available measurements in InfluxDB"""
    print("\n" + "="*80)
    print("TEST 2: List Available InfluxDB Measurements")
    print("="*80 + "\n")

    client = InfluxDBClient3(
        host=influx_url,
        database=influx_bucket,
        token=influx_token
    )

    try:
        query = "SHOW TABLES"
        result = client.query(query)
        df_tables = result.to_pandas()

        print(df_tables)
        print(f"\nTotal measurements: {len(df_tables)}")

        measurements = df_tables['table_name'].tolist()
        client.close()
        return measurements

    except Exception as e:
        print(f"Error listing tables: {e}")
        client.close()
        return []


async def test_3_coherence_checking(measurements):
    """Test 3: Post-hoc coherence checking on all measurements"""
    print("\n" + "="*80)
    print("TEST 3: Post-Hoc Coherence Checking on All Measurements")
    print("="*80)

    async with ThetaDataV3Client() as client:
        install_td_server_error_logger(client)
        manager = ThetaSyncManager(cfg, client=client)
        checker = CoherenceChecker(manager)

        # Focus on TLRY measurements
        tlry_measurements = [m for m in measurements if 'TLRY' in m]

        for measurement in tlry_measurements:
            print(f"\n{'='*60}")
            print(f"Checking: {measurement}")
            print(f"{'='*60}")

            try:
                parts = measurement.split('-')
                if len(parts) >= 3:
                    symbol = parts[0]
                    asset = parts[1]
                    interval = parts[2]

                    end_date = pd.Timestamp.now().date()
                    start_date = end_date - pd.Timedelta(days=30)

                    print(f"Symbol: {symbol}, Asset: {asset}, Interval: {interval}")
                    print(f"Checking range: {start_date} to {end_date}")

                    report = await checker.check(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        sink="influxdb",
                        start_date=str(start_date),
                        end_date=str(end_date)
                    )

                    if report.issues:
                        print(f"\n[WARNING] Found {len(report.issues)} issues:")
                        for issue in report.issues:
                            print(f"  - {issue.issue_type}: {issue.description}")
                            if 'problem_segments' in issue.details:
                                for seg in issue.details['problem_segments'][:5]:
                                    seg_start = seg.get('segment_start', 'N/A')
                                    seg_end = seg.get('segment_end', 'N/A')
                                    seg_issue = seg.get('issue', 'N/A')
                                    print(f"    {seg_start}-{seg_end}: {seg_issue}")
                    else:
                        print("[OK] No coherence issues found")

            except Exception as e:
                print(f"[ERROR] Error checking {measurement}: {e}")
                import traceback
                traceback.print_exc()


async def test_4_check_logs():
    """Test 4: Check data consistency logs"""
    import os
    import glob

    print("\n" + "="*80)
    print("TEST 4: Data Consistency Logs")
    print("="*80 + "\n")

    log_dir = os.path.join(cfg.root_dir, "logs")

    if os.path.exists(log_dir):
        log_files = glob.glob(os.path.join(log_dir, "data_consistency_*.parquet"))

        if log_files:
            print(f"Found {len(log_files)} log files:\n")

            latest_log = max(log_files, key=os.path.getmtime)
            print(f"Latest log: {os.path.basename(latest_log)}")

            try:
                log_df = pd.read_parquet(latest_log)
                print(f"\nLog entries: {len(log_df)}")
                print("\nRecent log entries (last 20):")
                print(log_df.tail(20).to_string())

                print("\n" + "-"*80)
                print("Summary by event type:")
                print(log_df['event_type'].value_counts())

                print("\n" + "-"*80)
                print("Summary by severity:")
                if 'severity' in log_df.columns:
                    print(log_df['severity'].value_counts())

            except Exception as e:
                print(f"Error reading log: {e}")
        else:
            print("No log files found")
    else:
        print(f"Log directory not found: {log_dir}")


async def test_5_query_specific_data():
    """Test 5: Query specific data from InfluxDB to verify writes"""
    print("\n" + "="*80)
    print("TEST 5: Query Recent Data from InfluxDB")
    print("="*80 + "\n")

    client = InfluxDBClient3(
        host=influx_url,
        database=influx_bucket,
        token=influx_token
    )

    try:
        # Query recent TLRY data
        measurements = ["TLRY-option-1d", "TLRY-option-5m"]

        for measurement in measurements:
            print(f"\nQuerying {measurement}...")

            # Get last 10 rows
            query = f'''
                SELECT time, symbol, close, volume
                FROM "{measurement}"
                ORDER BY time DESC
                LIMIT 10
            '''

            try:
                result = client.query(query)
                df = result.to_pandas()

                if not df.empty:
                    print(f"[OK] Found {len(df)} rows:")
                    print(df.to_string())
                else:
                    print(f"[WARNING] No data found in {measurement}")

            except Exception as e:
                print(f"[ERROR] Error querying {measurement}: {e}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


async def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("DATA CONSISTENCY COMPREHENSIVE TEST SUITE")
    print("="*80)
    print(f"Testing symbols: {symbols}")
    print(f"Validation enabled: {cfg.enable_data_validation}")
    print(f"Strict mode: {cfg.validation_strict_mode}")

    # Test 1: Real-time download
    await test_1_real_time_download()

    # Test 2: List measurements
    measurements = await test_2_list_measurements()

    # Test 3: Coherence checking
    if measurements:
        await test_3_coherence_checking(measurements)

    # Test 4: Check logs
    await test_4_check_logs()

    # Test 5: Query data
    await test_5_query_specific_data()

    print("\n" + "="*80)
    print("ALL TESTS COMPLETED")
    print("="*80)


if __name__ == "__main__":
    asyncio.run(main())
