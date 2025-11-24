"""InfluxDB write verification and recovery.

This module provides post-write verification for InfluxDB to detect partial writes
and automatically retry missing data.
"""

from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Tuple, Optional, Set

import numpy as np
import pandas as pd


@dataclass
class InfluxWriteResult:
    """Result of InfluxDB write verification.

    Attributes
    ----------
    total_attempted : int
        Total rows attempted to write
    successfully_written : int
        Rows confirmed written
    missing_count : int
        Number of rows not found after write
    missing_indices : List[int]
        DataFrame indices of missing rows
    success : bool
        True if all rows written successfully
    """
    total_attempted: int
    successfully_written: int
    missing_count: int
    missing_indices: List[int]
    success: bool


async def verify_influx_write(
    influx_client,
    measurement: str,
    df_original: pd.DataFrame,
    key_cols: List[str],
    time_col: str = '__ts_utc'
) -> InfluxWriteResult:
    """Verify that all rows from df_original were written to InfluxDB.

    Queries InfluxDB after write to check which rows are present.
    Identifies missing rows by comparing key columns.

    Parameters
    ----------
    influx_client
        InfluxDB client instance
    measurement : str
        Measurement name
    df_original : pd.DataFrame
        Original DataFrame that was attempted to be written
    key_cols : List[str]
        Columns that uniquely identify a row (e.g., ['__ts_utc', 'symbol', 'strike', 'expiration', 'right'])
    time_col : str
        Time column name (default '__ts_utc')

    Returns
    -------
    InfluxWriteResult
        Verification result with missing row details

    Example Usage
    -------------
    result = await verify_influx_write(
        influx_client=client,
        measurement='stock_AAPL_5m',
        df_original=df,
        key_cols=['__ts_utc', 'symbol']
    )

    if not result.success:
        print(f"Missing {result.missing_count} rows")
        missing_df = df.iloc[result.missing_indices]
        # Retry write with missing_df
    """
    if df_original.empty:
        return InfluxWriteResult(
            total_attempted=0,
            successfully_written=0,
            missing_count=0,
            missing_indices=[],
            success=True
        )

    total = len(df_original)

    # Build query to check what was written
    min_time = df_original[time_col].min()
    max_time = df_original[time_col].max()

    # Convert to nanoseconds for InfluxDB query
    if hasattr(min_time, 'value'):
        min_ns = min_time.value
        max_ns = max_time.value
    else:
        min_ns = pd.Timestamp(min_time).value
        max_ns = pd.Timestamp(max_time).value

    # Build SELECT query with all key columns
    select_cols = ', '.join([f'"{col}"' for col in key_cols if col != time_col])
    query = f"""
        SELECT time as __ts_utc, {select_cols}
        FROM "{measurement}"
        WHERE time >= to_timestamp_ns({min_ns}) AND time <= to_timestamp_ns({max_ns})
    """

    def _canonical_time(value):
        if pd.isna(value):
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return int(ts.value)

    def _canonical_scalar(value):
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            ts = value
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            return ts.isoformat()
        if isinstance(value, datetime):
            ts = pd.Timestamp(value, tz="UTC")
            return ts.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, np.generic):
            return value.item()
        return value

    try:
        # Execute query
        result_table = influx_client.query(query)

        if result_table is None or result_table.num_rows == 0:
            # Nothing written
            return InfluxWriteResult(
                total_attempted=total,
                successfully_written=0,
                missing_count=total,
                missing_indices=list(range(total)),
                success=False
            )

        # Convert result to DataFrame
        df_written = result_table.to_pandas()

        # FlightSQL may not properly alias 'time as __ts_utc', so rename explicitly if needed
        if time_col not in df_written.columns and 'time' in df_written.columns:
            df_written = df_written.rename(columns={'time': time_col})

        # Verify we have all required key columns
        missing_cols = [col for col in key_cols if col not in df_written.columns]
        if missing_cols:
            # Query returned data but missing expected columns - assume nothing written
            print(f"[INFLUX][VERIFY] Query returned columns: {list(df_written.columns)}")
            print(f"[INFLUX][VERIFY] Expected key_cols: {key_cols}, missing: {missing_cols}")
            return InfluxWriteResult(
                total_attempted=total,
                successfully_written=0,
                missing_count=total,
                missing_indices=list(range(total)),
                success=False
            )

        # Compare key sets
        original_keys = set()
        for idx, row in df_original.iterrows():
            key_tuple = []
            for col in key_cols:
                val = row[col]
                if col == time_col:
                    key_tuple.append(_canonical_time(val))
                else:
                    key_tuple.append(_canonical_scalar(val))
            original_keys.add((idx, tuple(key_tuple)))

        written_keys = set()
        for _, row in df_written.iterrows():
            key_tuple = []
            for col in key_cols:
                val = row[col]
                if col == time_col:
                    key_tuple.append(_canonical_time(val))
                else:
                    key_tuple.append(_canonical_scalar(val))
            written_keys.add(tuple(key_tuple))

        # Find missing
        missing_indices = []
        for idx, key in original_keys:
            if key not in written_keys:
                missing_indices.append(idx)

        missing_count = len(missing_indices)
        successfully_written = total - missing_count

        return InfluxWriteResult(
            total_attempted=total,
            successfully_written=successfully_written,
            missing_count=missing_count,
            missing_indices=missing_indices,
            success=(missing_count == 0)
        )

    except Exception as e:
        # Query failed - assume worst case (nothing written)
        return InfluxWriteResult(
            total_attempted=total,
            successfully_written=0,
            missing_count=total,
            missing_indices=list(range(total)),
            success=False
        )


async def write_influx_with_verification(
    write_func,
    verify_func,
    df: pd.DataFrame,
    retry_policy,
    logger,
    context: dict
) -> Tuple[int, bool]:
    """Write to InfluxDB with post-write verification and retry.

    Writes data, verifies what was actually written, and retries missing rows
    up to K times (configured in retry_policy).

    Parameters
    ----------
    write_func : callable
        Async function that writes DataFrame to InfluxDB (returns rows_written)
    verify_func : callable
        Async function that verifies write (returns InfluxWriteResult)
    df : pd.DataFrame
        Data to write
    retry_policy : RetryPolicy
        Retry configuration
    logger : DataConsistencyLogger
        Logger instance
    context : dict
        Context with {symbol, asset, interval, sink, measurement, ...}

    Returns
    -------
    tuple
        (total_written: int, success: bool)

    Example Usage
    -------------
    wrote, success = await write_influx_with_verification(
        write_func=lambda df: manager._append_influx_df(base_path, df),
        verify_func=lambda df: verify_influx_write(client, measurement, df, key_cols),
        df=dataframe,
        retry_policy=manager.cfg.retry_policy,
        logger=manager.logger,
        context={'symbol': 'AAPL', 'measurement': 'stock_AAPL_5m', ...}
    )
    """
    import asyncio

    symbol = context.get('symbol')
    asset = context.get('asset')
    interval = context.get('interval')
    measurement = context.get('measurement')
    date_range = context.get('date_range', ('', ''))

    current_df = df.copy()
    total_written = 0
    max_attempts = retry_policy.max_attempts

    for attempt in range(max_attempts):
        if current_df.empty:
            break

        try:
            # Write
            wrote = await write_func(current_df)
            total_written += wrote

            # Verify
            verification = await verify_func(current_df)

            if verification.success:
                if attempt > 0:
                    logger.log_resolution(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=date_range,
                        message=f"InfluxDB write verified successfully after {attempt + 1} attempts",
                        details={'total_written': total_written, 'measurement': measurement}
                    )
                return total_written, True

            # Partial write detected
            missing_df = current_df.iloc[verification.missing_indices].copy()

            if attempt < max_attempts - 1:
                logger.log_retry_attempt(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    attempt=attempt + 1,
                    error_msg=f"InfluxDB partial write: {verification.missing_count}/{verification.total_attempted} missing, retrying",
                    details={
                        'missing_count': verification.missing_count,
                        'total_attempted': verification.total_attempted,
                        'measurement': measurement
                    }
                )

                await asyncio.sleep(retry_policy.delay_seconds)
                current_df = missing_df
            else:
                # Final attempt failed
                logger.log_failure(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    message=f"InfluxDB write failed after {max_attempts} attempts: {verification.missing_count} rows still missing",
                    details={
                        'missing_count': verification.missing_count,
                        'total_written': total_written,
                        'measurement': measurement
                    }
                )
                return total_written, False

        except Exception as e:
            if attempt < max_attempts - 1:
                logger.log_retry_attempt(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    attempt=attempt + 1,
                    error_msg=f"InfluxDB write error: {str(e)}, retrying",
                    details={'error': str(e), 'error_type': type(e).__name__, 'measurement': measurement}
                )
                await asyncio.sleep(retry_policy.delay_seconds)
            else:
                logger.log_failure(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    message=f"InfluxDB write failed after {max_attempts} attempts: {str(e)}",
                    details={'error': str(e), 'error_type': type(e).__name__, 'measurement': measurement}
                )
                return 0, False

    # Should not reach here
    return total_written, False
