"""Data validation utilities for completeness and consistency checks."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

__all__ = ["ValidationResult", "DataValidator"]


@dataclass
class ValidationResult:
    """Result of a data validation check.

    Attributes
    ----------
    valid : bool
        True if validation passed, False otherwise.
    missing_ranges : List[Tuple[str, str]]
        List of missing date/time ranges as (start, end) tuples.
    error_message : Optional[str]
        Human-readable error message if validation failed.
    details : Dict[str, Any]
        Additional details about the validation result.
    """
    valid: bool
    missing_ranges: List[Tuple[str, str]]
    error_message: Optional[str]
    details: Dict[str, Any]


class DataValidator:
    """Stateless validator for data completeness checks on DataFrames."""

    @staticmethod
    def validate_eod_completeness(
        df: pd.DataFrame,
        expected_dates: List[str]
    ) -> ValidationResult:
        """Check that DataFrame contains all expected EOD dates.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with EOD data (must have 'date' column).
        expected_dates : List[str]
            List of expected dates in ISO format (YYYY-MM-DD).

        Returns
        -------
        ValidationResult
            Validation result with missing dates if any.
        """
        # Filter out weekends from expected dates (markets closed on Sat/Sun)
        filtered_dates = []
        for date_str in expected_dates:
            try:
                date_obj = pd.to_datetime(date_str).date()
                # weekday(): Monday=0, Sunday=6
                if date_obj.weekday() < 5:  # Exclude Saturday (5) and Sunday (6)
                    filtered_dates.append(date_str)
            except Exception:
                # If date parsing fails, keep it in the list
                filtered_dates.append(date_str)

        expected_dates = filtered_dates

        if df.empty:
            return ValidationResult(
                valid=False,
                missing_ranges=[(expected_dates[0], expected_dates[-1])] if expected_dates else [],
                error_message="DataFrame is empty",
                details={'expected_count': len(expected_dates), 'actual_count': 0}
            )

        # Extract actual dates
        # Try time column candidates in order of preference
        # timestamp first (used by tick/intraday), then EOD columns (last_trade, created, date)
        time_col = None
        for col in ['timestamp', 'last_trade', 'created', 'date']:
            if col in df.columns:
                time_col = col
                break

        if time_col is None:
            return ValidationResult(
                valid=False,
                missing_ranges=[],
                error_message="No date/time column found (expected: last_trade, created, timestamp, or date)",
                details={'available_columns': list(df.columns)}
            )

        actual_dates = pd.to_datetime(df[time_col], utc=True, errors='coerce').dt.strftime('%Y-%m-%d').unique().tolist()

        # Find missing dates
        missing = sorted(set(expected_dates) - set(actual_dates))

        if not missing:
            return ValidationResult(
                valid=True,
                missing_ranges=[],
                error_message=None,
                details={'expected_count': len(expected_dates), 'actual_count': len(actual_dates)}
            )

        # Compute contiguous ranges
        missing_ranges = DataValidator._compute_date_ranges(missing)

        return ValidationResult(
            valid=False,
            missing_ranges=missing_ranges,
            error_message=f"Missing {len(missing)} days out of {len(expected_dates)}",
            details={
                'missing_dates': missing,
                'expected_count': len(expected_dates),
                'actual_count': len(actual_dates)
            }
        )

    @staticmethod
    def validate_intraday_completeness(
        df: pd.DataFrame,
        interval: str,
        date_iso: str,
        asset: str,
        bucket_tolerance: float = 0.0,
        expected_combo_total: Optional[int] = None
    ) -> ValidationResult:
        """Check that DataFrame contains expected number of intraday candles.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with intraday candle data.
        interval : str
            Time interval (e.g., "5m", "1h").
        date_iso : str
            Date being validated (YYYY-MM-DD).
        asset : str
            Asset type ("stock", "option", "index").
        bucket_tolerance : float
            Fractional tolerance for missing buckets/combos (0 = require all).
        expected_combo_total : Optional[int]
            Expected total combinations (expiration x strike x right) for the day. If provided,
            per-bucket combo validation will compare against this value (adjusted by tolerance);
            otherwise it falls back to combos observed in the df.

        Returns
        -------
        ValidationResult
            Validation result with missing time ranges if any.
        """
        if df.empty:
            return ValidationResult(
                valid=False,
                missing_ranges=[(f"{date_iso} 09:30:00", f"{date_iso} 16:00:00")],
                error_message=f"No data for {date_iso}",
                details={'expected': 'N/A', 'actual': 0}
            )

        # Determine timestamp column
        ts_col = None
        for candidate in ["timestamp", "trade_timestamp", "created", "last_trade"]:
            if candidate in df.columns:
                ts_col = candidate
                break

        if ts_col is None:
            return ValidationResult(
                valid=False,
                missing_ranges=[(f"{date_iso} 09:30:00", f"{date_iso} 16:00:00")],
                error_message="No timestamp column available for intraday validation",
                details={'expected': 'N/A', 'actual': 0}
            )

        ts_series = pd.to_datetime(df[ts_col], errors="coerce", utc=True).dropna()
        # Converti a ET per contare i bucket di mercato, gestendo DST
        ts_series = ts_series.dt.tz_convert(
            "America/New_York"
        ).dropna()

        if ts_series.empty:
            return ValidationResult(
                valid=False,
                missing_ranges=[(f"{date_iso} 09:30:00", f"{date_iso} 16:00:00")],
                error_message=f"No valid timestamps for {date_iso}",
                details={'expected': 'N/A', 'actual': 0}
            )

        def _freq_alias_and_delta(interval: str):
            if interval.endswith('m'):
                minutes = int(interval[:-1])
                return f"{minutes}min", timedelta(minutes=minutes)
            if interval.endswith('h'):
                hours = int(interval[:-1])
                return f"{hours}H", timedelta(hours=hours)
            if interval.endswith('s'):
                seconds = int(interval[:-1])
                return f"{seconds}S", timedelta(seconds=seconds)
            return "5min", timedelta(minutes=5)

        freq_alias, bucket_delta = _freq_alias_and_delta(interval)
        bucket_series = ts_series.dt.floor(freq_alias)
        unique_timestamps = bucket_series.drop_duplicates()
        unique_count = len(unique_timestamps)
        actual_count = len(df)

        # Calcola l'intervallo effettivo coperto dai bucket e l'atteso in base al range osservato
        if unique_timestamps.empty:
            expected_count = 0
        else:
            start_bucket = unique_timestamps.min()
            end_bucket = unique_timestamps.max() + bucket_delta
            span = end_bucket - start_bucket
            expected_count = max(1, int(span / bucket_delta))

        # Coverage check on time buckets
        time_coverage_ok = unique_count >= expected_count * (1 - bucket_tolerance)

        # Expected combos per bucket (only for options where right/strike/expiration exist)
        combo_gaps = []
        total_combos = None
        min_combos = None
        if asset == "option" and all(col in df.columns for col in ["expiration", "strike", "right"]):
            combos = df[["expiration", "strike", "right"]].drop_duplicates()
            observed_combos = len(combos)
            total_combos = expected_combo_total if expected_combo_total is not None else observed_combos
            # Se osserviamo più combo dell'atteso (nuove strike/exp intraday), aggiorna l'atteso al massimo
            if expected_combo_total is not None:
                total_combos = max(total_combos, observed_combos)
            per_bucket = (
                df.groupby(bucket_series)
                .apply(lambda x: len(x[["expiration", "strike", "right"]].drop_duplicates()))
            )
            min_combos = per_bucket.min() if not per_bucket.empty else None
            expected_combos = total_combos * (1 - bucket_tolerance) if total_combos is not None else None
            if expected_combos is not None:
                for bucket_time, count in per_bucket.items():
                    # Log per-bucket atteso vs trovato
                    print(
                        f"[VALIDATION][BUCKET] {date_iso} {bucket_time.strftime('%H:%M:%S')} "
                        f"combos_found={count} expected>={int(expected_combos)} "
                        f"(total_combos={total_combos}, tol={bucket_tolerance:.2%})"
                    )
                    if count < expected_combos:
                        start_str = bucket_time.strftime('%Y-%m-%d %H:%M:%S')
                        end_str = (bucket_time + bucket_delta).strftime('%Y-%m-%d %H:%M:%S')
                        combo_gaps.append((start_str, end_str))

        # Allow no gaps and required coverage
        gaps = DataValidator._find_time_gaps(df, interval)
        missing_ranges = combo_gaps + gaps

        if time_coverage_ok and not missing_ranges:
            return ValidationResult(
                valid=True,
                missing_ranges=[],
                error_message=None,
                details={
                    'expected': expected_count,
                    'actual_rows': actual_count,
                    'actual_buckets': unique_count,
                    'total_combos': total_combos,
                    'min_combos_per_bucket': min_combos,
                    'expected_combo_total': expected_combo_total
                }
            )

        return ValidationResult(
            valid=False,
            missing_ranges=missing_ranges,
            error_message=f"Expected ~{expected_count} buckets, found {unique_count}",
            details={
                'expected': expected_count,
                'actual_rows': actual_count,
                'actual_buckets': unique_count,
                'gaps': missing_ranges,
                'missing_count': expected_count - unique_count,
                'total_combos': total_combos,
                'min_combos_per_bucket': min_combos,
                'expected_combo_total': expected_combo_total
            }
        )

    @staticmethod
    def validate_tick_vs_eod_volume(
        tick_df: pd.DataFrame,
        eod_volume: float,
        date_iso: str,
        asset: str,
        tolerance: float = 0.01,
        eod_volume_call: Optional[float] = None,
        eod_volume_put: Optional[float] = None
    ) -> ValidationResult:
        """Verify tick data volume sum matches EOD volume.

        For options, volumes should be validated separately for calls and puts when the
        EOD data provides separate volumes. If separate call/put volumes are not provided,
        the method falls back to comparing against the total volume.

        Parameters
        ----------
        tick_df : pd.DataFrame
            DataFrame with tick data (must have 'volume' column). For options, should
            also have 'right' column indicating 'C' (call) or 'P' (put).
        eod_volume : float
            Expected total EOD volume. For options without separate call/put volumes,
            this is the combined volume. For stocks/indices, this is the only volume.
        date_iso : str
            Date being validated (YYYY-MM-DD).
        asset : str
            Asset type ("stock", "option", "index").
        tolerance : float, optional
            Default: 0.01

            Allowed tolerance as fraction (default 0.01 = 1%).
        eod_volume_call : Optional[float], optional
            Default: None

            Expected EOD volume for call options only. If provided along with
            eod_volume_put, enables separate call/put validation for options.
        eod_volume_put : Optional[float], optional
            Default: None

            Expected EOD volume for put options only. If provided along with
            eod_volume_call, enables separate call/put validation for options.

        Returns
        -------
        ValidationResult
            Validation result with volume comparison details. For options with separate
            call/put validation, details include breakdowns by option type.
        """
        if tick_df.empty:
            return ValidationResult(
                valid=False,
                missing_ranges=[(date_iso, date_iso)],
                error_message="No tick data available",
                details={'tick_volume': 0, 'eod_volume': eod_volume}
            )

        # Determine correct volume column name based on asset type
        # OPTIONS: trade_quote endpoint returns 'size' (quantity per trade)
        # STOCK/INDEX: returns 'volume'
        volume_col = 'size' if asset == "option" else 'volume'

        # Check for volume/size column
        if volume_col not in tick_df.columns:
            return ValidationResult(
                valid=False,
                missing_ranges=[],
                error_message=f"No '{volume_col}' column in tick data",
                details={'expected_column': volume_col, 'available_columns': list(tick_df.columns)}
            )

        # For options with separate call/put volumes
        if asset == "option" and eod_volume_call is not None and eod_volume_put is not None:
            if 'right' not in tick_df.columns:
                return ValidationResult(
                    valid=False,
                    missing_ranges=[],
                    error_message="Options data missing 'right' column for call/put separation",
                    details={}
                )

            # Normalize right column (handles C/P, call/put, any casing)
            normalized_right = (
                tick_df['right']
                .astype(str)
                .str.strip()
                .str.lower()
                .replace({'c': 'call', 'p': 'put'})
            )

            call_mask = normalized_right == 'call'
            put_mask = normalized_right == 'put'

            # Sum volumes separately for calls and puts using the correct column
            tick_call_volume = tick_df[call_mask][volume_col].sum()
            tick_put_volume = tick_df[put_mask][volume_col].sum()

            # Calculate differences
            if eod_volume_call == 0:
                diff_call_pct = 1.0 if tick_call_volume > 0 else 0.0
            else:
                diff_call_pct = abs(tick_call_volume - eod_volume_call) / eod_volume_call

            if eod_volume_put == 0:
                diff_put_pct = 1.0 if tick_put_volume > 0 else 0.0
            else:
                diff_put_pct = abs(tick_put_volume - eod_volume_put) / eod_volume_put

            # Both must be within tolerance
            is_valid = diff_call_pct <= tolerance and diff_put_pct <= tolerance

            # Log volume validation results (console + structured log)
            print(f"[TICK-VOLUME] {date_iso} Call: sum({volume_col})={int(tick_call_volume)} eod_volume={int(eod_volume_call)} diff={diff_call_pct:.2%}")
            print(f"[TICK-VOLUME] {date_iso} Put: sum({volume_col})={int(tick_put_volume)} eod_volume={int(eod_volume_put)} diff={diff_put_pct:.2%}")
            print(f"[TICK-VOLUME] {date_iso} Total: tick={int(tick_call_volume + tick_put_volume)} eod={int(eod_volume_call + eod_volume_put)} tolerance={tolerance:.2%} {'PASS' if is_valid else 'FAIL'}")

            # Structured log for volume validation statistics
            # Note: DataValidator is stateless and doesn't have access to manager.logger
            # Print statements above already provide detailed output
            logger = None
            if logger:
                logger.log_info(
                    symbol="",
                    asset=asset,
                    interval="tick",
                    date_range=(date_iso, date_iso),
                    message=f"VOLUME_VALIDATION: {'PASS' if is_valid else 'FAIL'}",
                    details={
                        'tick_call_volume': int(tick_call_volume),
                        'tick_put_volume': int(tick_put_volume),
                        'tick_total': int(tick_call_volume + tick_put_volume),
                        'eod_call_volume': int(eod_volume_call),
                        'eod_put_volume': int(eod_volume_put),
                        'eod_total': int(eod_volume_call + eod_volume_put),
                        'diff_call_pct': round(diff_call_pct * 100, 2),
                        'diff_put_pct': round(diff_put_pct * 100, 2),
                        'diff_call_abs': int(abs(tick_call_volume - eod_volume_call)),
                        'diff_put_abs': int(abs(tick_put_volume - eod_volume_put)),
                        'tolerance_pct': round(tolerance * 100, 2),
                        'result': 'PASS' if is_valid else 'FAIL'
                    }
                )

            return ValidationResult(
                valid=is_valid,
                missing_ranges=[] if is_valid else [(date_iso, date_iso)],
                error_message=None if is_valid else (
                    f"Volume mismatch - Call: {diff_call_pct:.2%}, Put: {diff_put_pct:.2%} difference"
                ),
                details={
                    'volume_column_used': volume_col,
                    'tick_call_volume': float(tick_call_volume),
                    'tick_put_volume': float(tick_put_volume),
                    'tick_total_volume': float(tick_call_volume + tick_put_volume),
                    'eod_call_volume': float(eod_volume_call),
                    'eod_put_volume': float(eod_volume_put),
                    'eod_total_volume': float(eod_volume_call + eod_volume_put),
                    'diff_call_pct': float(diff_call_pct),
                    'diff_put_pct': float(diff_put_pct),
                    'diff_call_absolute': float(abs(tick_call_volume - eod_volume_call)),
                    'diff_put_absolute': float(abs(tick_put_volume - eod_volume_put))
                }
            )
        else:
            # Standard validation for stocks/indices or options without separate call/put volumes
            tick_volume = tick_df[volume_col].sum()

            # Calculate difference
            if eod_volume == 0:
                diff_pct = 1.0 if tick_volume > 0 else 0.0
            else:
                diff_pct = abs(tick_volume - eod_volume) / eod_volume

            is_valid = diff_pct <= tolerance

            # Log volume validation results (console + structured log)
            print(f"[TICK-VOLUME] {date_iso} sum({volume_col})={int(tick_volume)} eod_volume={int(eod_volume)} diff={diff_pct:.2%} diff_abs={int(abs(tick_volume - eod_volume))} tolerance={tolerance:.2%} {'PASS' if is_valid else 'FAIL'}")

            # Structured log for volume validation statistics
            # Note: DataValidator is stateless and doesn't have access to manager.logger
            # Print statements above already provide detailed output
            logger = None
            if logger:
                logger.log_info(
                    symbol="",
                    asset=asset,
                    interval="tick",
                    date_range=(date_iso, date_iso),
                    message=f"VOLUME_VALIDATION: {'PASS' if is_valid else 'FAIL'}",
                    details={
                        'tick_volume': int(tick_volume),
                        'eod_volume': int(eod_volume),
                        'diff_pct': round(diff_pct * 100, 2),
                        'diff_abs': int(abs(tick_volume - eod_volume)),
                        'tolerance_pct': round(tolerance * 100, 2),
                        'result': 'PASS' if is_valid else 'FAIL'
                    }
                )

            return ValidationResult(
                valid=is_valid,
                missing_ranges=[] if is_valid else [(date_iso, date_iso)],
                error_message=None if is_valid else f"Volume mismatch: {diff_pct:.2%} difference",
                details={
                    'volume_column_used': volume_col,
                    'tick_volume': float(tick_volume),
                    'eod_volume': float(eod_volume),
                    'diff_pct': float(diff_pct),
                    'diff_absolute': float(abs(tick_volume - eod_volume))
                }
            )

    @staticmethod
    def validate_required_columns(
        df: pd.DataFrame,
        asset: str,
        interval: str,
        enrich_greeks: bool
    ) -> ValidationResult:
        """Check that DataFrame contains all required columns.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to validate.
        asset : str
            Asset type ("stock", "option", "index").
        interval : str
            Time interval.
        enrich_greeks : bool
            Whether Greeks enrichment is expected.

        Returns
        -------
        ValidationResult
            Validation result with missing columns if any.
        """
        # Define required columns by asset and interval
        # For EOD data, prefer 'last_trade' (actual trading date) over 'created' (processing timestamp)
        if interval == "1d":
            # EOD can have 'last_trade', 'created', 'timestamp', or 'date' as time column
            # Prefer last_trade as it represents the actual trading date
            time_col_candidates = ['last_trade', 'created', 'timestamp', 'date']
            has_time_col = any(col in df.columns for col in time_col_candidates)
            if not has_time_col:
                return ValidationResult(
                    valid=False,
                    missing_ranges=[],
                    error_message=f"No date/time column found (expected: last_trade, created, timestamp, or date)",
                    details={'available_columns': list(df.columns)}
                )
            required_base = ['open', 'high', 'low', 'close', 'volume']
        elif interval == "tick":
            # Tick data requires 'timestamp'
            required_base = ['timestamp']
            if asset == "stock":
                required_base.extend(['price', 'size', 'exchange', 'sequence'])
            # Tick data may have different schema
        else:
            # Intraday OHLC requires 'timestamp'
            required_base = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

        if asset == "option":
            required_base.extend(['expiration', 'strike', 'right'])

            if enrich_greeks and interval != "tick":
                # EOD greeks endpoint returns: delta, gamma, theta, vega, rho (NO implied_vol)
                # Intraday greeks endpoint (option_history_all_greeks) also returns implied_vol
                if interval == "1d":
                    # EOD greeks - no implied_vol
                    required_greeks = ['delta', 'gamma', 'theta', 'vega', 'rho']
                else:
                    # Intraday greeks - includes implied_vol (ThetaData V3 API column name)
                    required_greeks = ['delta', 'gamma', 'theta', 'vega', 'rho', 'implied_vol']
                required_base.extend(required_greeks)

        # Check for missing columns
        actual_columns = set(df.columns)
        required_columns = set(required_base)
        missing_columns = required_columns - actual_columns

        if not missing_columns:
            return ValidationResult(
                valid=True,
                missing_ranges=[],
                error_message=None,
                details={'required': list(required_columns), 'actual': list(actual_columns)}
            )

        return ValidationResult(
            valid=False,
            missing_ranges=[],
            error_message=f"Missing required columns: {', '.join(missing_columns)}",
            details={
                'missing_columns': list(missing_columns),
                'required': list(required_columns),
                'actual': list(actual_columns)
            }
        )

    @staticmethod
    def _expected_candles_for_interval(interval: str, asset: str) -> int:
        """Calculate expected number of candles for a given interval.

        Parameters
        ----------
        interval : str
            Time interval (e.g., "5m", "1h").
        asset : str
            Asset type ("stock", "option", "index").

        Returns
        -------
        int
            Expected number of candles for a regular trading day.
        """
        # Regular trading session: 9:30 AM - 4:00 PM ET (6.5 hours = 390 minutes)
        session_minutes = 390

        # Parse interval
        if interval.endswith('m'):
            minutes = int(interval[:-1])
            return session_minutes // minutes
        elif interval.endswith('h'):
            hours = int(interval[:-1])
            return int(session_minutes / (hours * 60))
        elif interval.endswith('s'):
            seconds = int(interval[:-1])
            return session_minutes * 60 // seconds
        else:
            # Unknown interval, return reasonable default
            return 78  # 5-minute candles

    @staticmethod
    def _find_time_gaps(df: pd.DataFrame, interval: str) -> List[Tuple[str, str]]:
        """Find time gaps in intraday data.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with timestamp column.
        interval : str
            Expected time interval.

        Returns
        -------
        List[Tuple[str, str]]
            List of gaps as (start_time, end_time) tuples.
        """
        if df.empty or 'timestamp' not in df.columns:
            return []

        # Sort by timestamp
        df_sorted = df.sort_values('timestamp').copy()
        timestamps = pd.to_datetime(df_sorted['timestamp'], errors="coerce", utc=True).dropna().drop_duplicates()

        # Calculate expected delta
        if interval.endswith('m'):
            minutes = int(interval[:-1])
            expected_delta = timedelta(minutes=minutes)
        elif interval.endswith('h'):
            hours = int(interval[:-1])
            expected_delta = timedelta(hours=hours)
        elif interval.endswith('s'):
            seconds = int(interval[:-1])
            expected_delta = timedelta(seconds=seconds)
        else:
            return []  # Unknown interval

        # Find gaps
        gaps = []
        for i in range(len(timestamps) - 1):
            delta = timestamps.iloc[i + 1] - timestamps.iloc[i]
            if delta > expected_delta * 1.5:  # Allow some tolerance
                gap_start = timestamps.iloc[i].strftime('%Y-%m-%d %H:%M:%S')
                gap_end = timestamps.iloc[i + 1].strftime('%Y-%m-%d %H:%M:%S')
                gaps.append((gap_start, gap_end))

        return gaps

    @staticmethod
    def _compute_date_ranges(dates: List[str]) -> List[Tuple[str, str]]:
        """Compute contiguous date ranges from a list of dates.

        Parameters
        ----------
        dates : List[str]
            Sorted list of dates in ISO format (YYYY-MM-DD).

        Returns
        -------
        List[Tuple[str, str]]
            List of contiguous ranges as (start_date, end_date) tuples.
        """
        if not dates:
            return []

        ranges = []
        start = dates[0]
        prev_date = datetime.fromisoformat(dates[0])

        for date_str in dates[1:]:
            current_date = datetime.fromisoformat(date_str)
            if (current_date - prev_date).days > 1:
                # Gap found, close current range
                ranges.append((start, prev_date.strftime('%Y-%m-%d')))
                start = date_str
            prev_date = current_date

        # Close final range
        ranges.append((start, dates[-1]))

        return ranges
