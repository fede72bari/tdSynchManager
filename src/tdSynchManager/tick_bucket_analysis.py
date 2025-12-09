"""Granular tick bucket analysis - compares tick data against intraday bars.

This module provides standalone analysis to identify problematic time buckets
by downloading intraday data (30m bars) and comparing volume aggregation against
tick data within each bucket.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd

if TYPE_CHECKING:
    from .manager import ThetaSyncManager


@dataclass
class BucketAnalysisResult:
    """Result of tick bucket analysis for a single time bucket.

    Attributes
    ----------
    bucket_start : str
        Start time of bucket (HH:MM:SS format)
    bucket_end : str
        End time of bucket (HH:MM:SS format)
    tick_volume : float
        Sum of tick.size within this bucket
    intraday_volume : float
        Volume from intraday bar for this bucket
    tick_count : int
        Number of tick records in this bucket
    diff_pct : float
        Percentage difference between tick and intraday volume
    is_coherent : bool
        True if difference is within tolerance
    """
    bucket_start: str
    bucket_end: str
    tick_volume: float
    intraday_volume: float
    tick_count: int
    diff_pct: float
    is_coherent: bool


@dataclass
class DayBucketAnalysisReport:
    """Report for bucket analysis of a single day.

    Attributes
    ----------
    date_iso : str
        Date analyzed (YYYY-MM-DD)
    symbol : str
        Symbol analyzed
    asset : str
        Asset type ('option' or 'stock')
    sink : str
        Data source ('csv', 'parquet', 'influxdb')
    intraday_interval : str
        Interval used for intraday bars (e.g., '30m')
    buckets : List[BucketAnalysisResult]
        Analysis result for each bucket
    is_coherent : bool
        True if all buckets are coherent
    total_tick_volume : float
        Total tick volume for the day
    total_intraday_volume : float
        Total intraday volume for the day
    tolerance : float
        Tolerance used for comparison
    """
    date_iso: str
    symbol: str
    asset: str
    sink: str
    intraday_interval: str
    buckets: List[BucketAnalysisResult] = field(default_factory=list)
    is_coherent: bool = True
    total_tick_volume: float = 0.0
    total_intraday_volume: float = 0.0
    tolerance: float = 0.01


async def analyze_tick_buckets(
    manager: 'ThetaSyncManager',
    symbol: str,
    asset: str,
    date_iso: str,
    sink: str,
    intraday_interval: str = "30m",
    tolerance: float = 0.01
) -> DayBucketAnalysisReport:
    """Analyze tick data coherence by comparing against intraday bars.

    This function provides granular analysis by:
    1. Downloading intraday bars from ThetaData API (e.g., 30m intervals)
    2. Loading tick data from local storage (CSV/Parquet/InfluxDB)
    3. For each intraday bar bucket:
       - Aggregate tick.size within the bucket time range
       - Compare against intraday bar.volume
       - Flag buckets with significant discrepancies

    **Timezone handling:**
    - ThetaData intraday timestamps are in America/New_York (ET)
    - Timestamp represents the **open time** of the bar
    - Bar [09:30:00] includes data from 09:30:00 (inclusive) to 10:00:00 (exclusive)
    - Local storage may have different timezone, ensure proper conversion

    Parameters
    ----------
    manager : ThetaSyncManager
        Manager instance with client and configuration
    symbol : str
        Symbol to analyze (e.g., "XLE")
    asset : str
        Asset type ('option' or 'stock')
    date_iso : str
        Date to analyze (YYYY-MM-DD format)
    sink : str
        Storage type ('csv', 'parquet', 'influxdb')
    intraday_interval : str
        Interval for intraday bars (default: '30m')
        Supported: '1m', '5m', '10m', '15m', '30m', '1h'
    tolerance : float
        Tolerance for volume comparison (default: 0.01 = 1%)

    Returns
    -------
    DayBucketAnalysisReport
        Detailed report with per-bucket analysis

    Example Usage
    -------------
    ```python
    async with ThetaDataV3Client() as client:
        mgr = ThetaSyncManager(cfg, client)

        # Analyze single day with 30-minute buckets
        report = await analyze_tick_buckets(
            manager=mgr,
            symbol="XLE",
            asset="option",
            date_iso="2025-08-21",
            sink="csv",
            intraday_interval="30m",
            tolerance=0.01
        )

        # Print problematic buckets
        for bucket in report.buckets:
            if not bucket.is_coherent:
                print(f"[{bucket.bucket_start}-{bucket.bucket_end}] "
                      f"tick={bucket.tick_volume} intraday={bucket.intraday_volume} "
                      f"diff={bucket.diff_pct:.2%}")
    ```
    """
    report = DayBucketAnalysisReport(
        date_iso=date_iso,
        symbol=symbol,
        asset=asset,
        sink=sink,
        intraday_interval=intraday_interval,
        tolerance=tolerance
    )

    try:
        # Step 1: Download intraday bars from API
        print(f"\n[BUCKET-ANALYSIS] {date_iso} - Downloading {intraday_interval} bars from ThetaData API...")
        intraday_df = await _fetch_intraday_bars(
            manager=manager,
            symbol=symbol,
            asset=asset,
            date_iso=date_iso,
            interval=intraday_interval
        )

        if intraday_df is None or intraday_df.empty:
            print(f"[BUCKET-ANALYSIS] No intraday data available for {date_iso}")
            return report

        print(f"[BUCKET-ANALYSIS] Retrieved {len(intraday_df)} intraday bars")

        # Step 2: Load tick data from local storage
        print(f"[BUCKET-ANALYSIS] Loading tick data from {sink}...")
        tick_df = await _load_tick_data(
            manager=manager,
            symbol=symbol,
            asset=asset,
            date_iso=date_iso,
            sink=sink
        )

        if tick_df is None or tick_df.empty:
            print(f"[BUCKET-ANALYSIS] No tick data found in {sink} for {date_iso}")
            return report

        print(f"[BUCKET-ANALYSIS] Loaded {len(tick_df)} tick records")

        # Step 3: Compare bucket by bucket
        report.buckets = await _compare_buckets(
            tick_df=tick_df,
            intraday_df=intraday_df,
            asset=asset,
            date_iso=date_iso,
            tolerance=tolerance
        )

        # Step 4: Calculate totals and coherence
        report.total_tick_volume = sum(b.tick_volume for b in report.buckets)
        report.total_intraday_volume = sum(b.intraday_volume for b in report.buckets)
        report.is_coherent = all(b.is_coherent for b in report.buckets)

        # Validate that intraday total is reasonable
        # Sum of intraday bars should approximately equal sum of tick data
        # (and both should be close to EOD volume)
        if report.total_tick_volume > 0:
            total_diff_pct = abs(report.total_tick_volume - report.total_intraday_volume) / report.total_tick_volume
            if total_diff_pct > 0.10:  # More than 10% difference
                print(f"\n[BUCKET-ANALYSIS] WARNING: Large discrepancy between totals!")
                print(f"  Total tick volume: {int(report.total_tick_volume)}")
                print(f"  Total intraday volume: {int(report.total_intraday_volume)}")
                print(f"  Difference: {total_diff_pct:.1%}")
                print(f"  This suggests incomplete intraday data (missing expirations or strikes)")

        # Log summary
        print(f"\n[BUCKET-ANALYSIS] {date_iso} - Summary:")
        print(f"  Total buckets: {len(report.buckets)}")
        print(f"  Coherent buckets: {sum(1 for b in report.buckets if b.is_coherent)}")
        print(f"  Problematic buckets: {sum(1 for b in report.buckets if not b.is_coherent)}")
        print(f"  Total tick volume: {int(report.total_tick_volume)}")
        print(f"  Total intraday volume: {int(report.total_intraday_volume)}")
        print(f"  Overall coherent: {report.is_coherent}")

        # Log problematic buckets
        problematic = [b for b in report.buckets if not b.is_coherent]
        if problematic:
            print(f"\n[BUCKET-ANALYSIS] Problematic buckets:")
            for b in problematic:
                print(f"  [{b.bucket_start}-{b.bucket_end}] tick={int(b.tick_volume)} "
                      f"intraday={int(b.intraday_volume)} diff={b.diff_pct:.2%} ticks={b.tick_count}")

    except Exception as e:
        print(f"[BUCKET-ANALYSIS] Error during analysis: {e}")
        import traceback
        traceback.print_exc()

    return report


async def _fetch_intraday_bars(
    manager: 'ThetaSyncManager',
    symbol: str,
    asset: str,
    date_iso: str,
    interval: str
) -> Optional[pd.DataFrame]:
    """Fetch intraday bars from ThetaData API.

    Returns DataFrame with columns: timestamp, volume, [other OHLC fields]
    """
    if asset != "option":
        # TODO: implement for stock/index
        raise NotImplementedError("Bucket analysis currently supports option only")

    from io import StringIO

    # Step 1: OPTIMIZED - Use same method as normal run to discover expirations
    # Query ThetaData API for all expirations that traded on this date
    # This is independent of local DB (for coherence checking) and very fast (1 API call)
    print(f"[BUCKET-ANALYSIS] Querying ThetaData for expirations that traded on {date_iso}...")
    active_expirations = await manager._expirations_that_traded(symbol, date_iso, req_type="trade")

    # Fallback to quotes if no trades found
    if not active_expirations:
        active_expirations = await manager._expirations_that_traded(symbol, date_iso, req_type="quote")

    if not active_expirations:
        print(f"[BUCKET-ANALYSIS] No expirations found for {symbol} on {date_iso} (neither trades nor quotes)")
        return None

    # Convert from YYYYMMDD to YYYY-MM-DD format for API calls
    active_expirations = [
        f"{exp[:4]}-{exp[4:6]}-{exp[6:8]}" if len(exp) == 8 else exp
        for exp in active_expirations
    ]

    # Step 2: Fetch intraday data for each expiration and concatenate
    all_dfs = []

    print(f"[BUCKET-ANALYSIS] Found {len(active_expirations)} expirations with trading activity on {date_iso}")

    # Process ALL expirations (no arbitrary limit)
    # We need complete data to match EOD volume
    success_count = 0
    no_data_count = 0

    for exp in active_expirations:
        try:
            csv_text, _ = await manager.client.option_history_ohlc(
                symbol=symbol,
                expiration=exp,
                date=date_iso,
                interval=interval,
                strike="*",  # all strikes
                right="both",  # both call and put
                format_type="csv"
            )

            if csv_text and csv_text.strip():
                df = pd.read_csv(StringIO(csv_text))
                if not df.empty:
                    all_dfs.append(df)
                    success_count += 1
        except Exception as e:
            # Some expirations may not have traded on this date - this is normal
            if "No data found" in str(e):
                no_data_count += 1
            else:
                # Log only unexpected errors
                print(f"[BUCKET-ANALYSIS] Error fetching {exp}: {e}")
            continue

    if no_data_count > 0:
        print(f"[BUCKET-ANALYSIS] Successfully fetched {success_count} expirations, {no_data_count} had no data (normal for illiquid strikes)")
    else:
        print(f"[BUCKET-ANALYSIS] Successfully fetched all {success_count} expirations with data")

    if not all_dfs:
        return None

    # Concatenate all dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)

    if combined_df.empty:
        return None

    # Ensure timestamp column exists
    if 'timestamp' not in combined_df.columns:
        return None

    # Parse timestamp
    # ThetaData returns ISO format strings for intraday (e.g., '2025-08-21T09:30:00')
    # Times are in America/New_York (ET) timezone
    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], utc=True)

    # IMPORTANT: Aggregate bars by timestamp
    # Each timestamp has multiple bars (one per contract: strike/expiration/right)
    # We need to sum volume across all contracts for each timestamp
    if 'volume' in combined_df.columns:
        aggregated_df = combined_df.groupby('timestamp', as_index=False).agg({
            'volume': 'sum'
        })
        return aggregated_df
    else:
        return None


async def _load_tick_data(
    manager: 'ThetaSyncManager',
    symbol: str,
    asset: str,
    date_iso: str,
    sink: str
) -> Optional[pd.DataFrame]:
    """Load tick data from local storage.

    Returns DataFrame with columns: timestamp (or ms_of_day), size, [other tick fields]
    """
    if sink.lower() == "influxdb":
        # Query InfluxDB
        measurement = f"{symbol}-{asset}-tick"
        next_day = (datetime.fromisoformat(date_iso) + timedelta(days=1)).strftime('%Y-%m-%d')

        query = f"""
            SELECT *
            FROM "{measurement}"
            WHERE time >= to_timestamp('{date_iso}T00:00:00Z')
              AND time < to_timestamp('{next_day}T00:00:00Z')
            ORDER BY time
        """

        try:
            df = manager._influx_query_dataframe(query)
            if df.empty:
                return None

            # Rename 'time' to standard column name
            if 'time' in df.columns:
                df = df.rename(columns={'time': 'timestamp'})

            return df
        except Exception as e:
            print(f"[BUCKET-ANALYSIS] Error querying InfluxDB: {e}")
            return None

    else:
        # CSV/Parquet: read from file
        files = manager._list_series_files(asset, symbol, 'tick', sink.lower())

        # Filter by date - there may be multiple _partNN files for the same date
        matching_files = [f for f in files if date_iso in f]

        if not matching_files:
            return None

        # Read and concatenate ALL matching files (multi-part support)
        dfs = []
        for file_path in matching_files:
            if sink.lower() == "csv":
                df_part = pd.read_csv(file_path)
            elif sink.lower() == "parquet":
                df_part = pd.read_parquet(file_path)
            else:
                continue

            if not df_part.empty:
                dfs.append(df_part)

        if not dfs:
            return None

        # Concatenate all parts and deduplicate
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates()

        return df


async def _compare_buckets(
    tick_df: pd.DataFrame,
    intraday_df: pd.DataFrame,
    asset: str,
    date_iso: str,
    tolerance: float
) -> List[BucketAnalysisResult]:
    """Compare tick volume vs intraday volume for each bucket.

    **Key logic:**
    - Intraday timestamp = open time of bar
    - Bar [09:30:00] with interval 30m → covers [09:30:00, 10:00:00)
    - Group ticks by matching time range
    - Sum tick.size and compare to intraday.volume
    """
    results = []

    # Determine volume column for tick data
    volume_col = 'size' if asset == "option" else 'volume'

    if volume_col not in tick_df.columns:
        print(f"[BUCKET-ANALYSIS] Warning: '{volume_col}' column not found in tick data")
        return results

    # Ensure tick data has timestamp column
    # Prefer quote_timestamp for options (has correct date), fallback to timestamp
    ts_col = None
    for col in ['quote_timestamp', 'timestamp', 'created', 'ms_of_day']:
        if col in tick_df.columns:
            ts_col = col
            break

    if ts_col is None:
        print(f"[BUCKET-ANALYSIS] Warning: no timestamp column in tick data")
        return results

    tick_df = tick_df.copy()

    if ts_col == 'ms_of_day':
        # Convert ms_of_day to timestamp
        base_date = pd.to_datetime(date_iso)
        tick_df['timestamp'] = base_date + pd.to_timedelta(tick_df[ts_col], unit='ms')
    else:
        # Use the identified timestamp column and rename to 'timestamp'
        if ts_col != 'timestamp':
            tick_df['timestamp'] = tick_df[ts_col]

        # Parse timestamps
        tick_df['timestamp'] = pd.to_datetime(tick_df['timestamp'], errors='coerce', utc=True)

    # Sort intraday by timestamp
    intraday_df = intraday_df.sort_values('timestamp').copy()

    # Infer bucket duration from first two bars
    if len(intraday_df) >= 2:
        bucket_duration = intraday_df['timestamp'].iloc[1] - intraday_df['timestamp'].iloc[0]
    else:
        # Fallback: assume 30 minutes
        bucket_duration = timedelta(minutes=30)

    # Process each intraday bar
    for idx, bar in intraday_df.iterrows():
        bucket_start_ts = bar['timestamp']
        bucket_end_ts = bucket_start_ts + bucket_duration

        # Filter ticks within this bucket
        bucket_ticks = tick_df[
            (tick_df['timestamp'] >= bucket_start_ts) &
            (tick_df['timestamp'] < bucket_end_ts)
        ]

        tick_volume = bucket_ticks[volume_col].sum() if not bucket_ticks.empty else 0
        intraday_volume = bar.get('volume', 0)
        tick_count = len(bucket_ticks)

        # Calculate difference
        if intraday_volume == 0:
            diff_pct = 1.0 if tick_volume > 0 else 0.0
        else:
            diff_pct = abs(tick_volume - intraday_volume) / intraday_volume

        is_coherent = diff_pct <= tolerance

        results.append(BucketAnalysisResult(
            bucket_start=bucket_start_ts.strftime("%H:%M:%S"),
            bucket_end=bucket_end_ts.strftime("%H:%M:%S"),
            tick_volume=float(tick_volume),
            intraday_volume=float(intraday_volume),
            tick_count=tick_count,
            diff_pct=diff_pct,
            is_coherent=is_coherent
        ))

    return results
