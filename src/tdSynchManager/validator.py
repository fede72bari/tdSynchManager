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
        if df.empty:
            return ValidationResult(
                valid=False,
                missing_ranges=[(expected_dates[0], expected_dates[-1])] if expected_dates else [],
                error_message="DataFrame is empty",
                details={'expected_count': len(expected_dates), 'actual_count': 0}
            )

        # Extract actual dates
        if 'date' in df.columns:
            actual_dates = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d').unique().tolist()
        elif 'timestamp' in df.columns:
            actual_dates = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d').unique().tolist()
        else:
            return ValidationResult(
                valid=False,
                missing_ranges=[],
                error_message="No 'date' or 'timestamp' column found",
                details={}
            )

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
        asset: str
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

        # Calculate expected candle count
        expected_count = DataValidator._expected_candles_for_interval(interval, asset)

        actual_count = len(df)

        # Allow 5% tolerance for market holidays, early closes, etc.
        tolerance = 0.05
        if actual_count >= expected_count * (1 - tolerance):
            return ValidationResult(
                valid=True,
                missing_ranges=[],
                error_message=None,
                details={'expected': expected_count, 'actual': actual_count}
            )

        # Find time gaps
        gaps = DataValidator._find_time_gaps(df, interval)

        return ValidationResult(
            valid=False,
            missing_ranges=gaps,
            error_message=f"Expected ~{expected_count} candles, found {actual_count}",
            details={
                'expected': expected_count,
                'actual': actual_count,
                'gaps': gaps,
                'missing_count': expected_count - actual_count
            }
        )

    @staticmethod
    def validate_tick_vs_eod_volume(
        tick_df: pd.DataFrame,
        eod_volume: float,
        date_iso: str,
        asset: str,
        tolerance: float = 0.01
    ) -> ValidationResult:
        """Verify tick data volume sum matches EOD volume.

        Parameters
        ----------
        tick_df : pd.DataFrame
            DataFrame with tick data (must have 'volume' column).
        eod_volume : float
            Expected EOD volume.
        date_iso : str
            Date being validated (YYYY-MM-DD).
        asset : str
            Asset type ("stock", "option", "index").
        tolerance : float, optional
            Allowed tolerance as fraction (default 0.01 = 1%).

        Returns
        -------
        ValidationResult
            Validation result with volume comparison details.
        """
        if tick_df.empty:
            return ValidationResult(
                valid=False,
                missing_ranges=[(date_iso, date_iso)],
                error_message="No tick data available",
                details={'tick_volume': 0, 'eod_volume': eod_volume}
            )

        # Sum tick volumes
        # For options: sum across call/put, buy/sell
        if 'volume' in tick_df.columns:
            tick_volume = tick_df['volume'].sum()
        else:
            return ValidationResult(
                valid=False,
                missing_ranges=[],
                error_message="No 'volume' column in tick data",
                details={}
            )

        # Calculate difference
        if eod_volume == 0:
            diff_pct = 1.0 if tick_volume > 0 else 0.0
        else:
            diff_pct = abs(tick_volume - eod_volume) / eod_volume

        is_valid = diff_pct <= tolerance

        return ValidationResult(
            valid=is_valid,
            missing_ranges=[] if is_valid else [(date_iso, date_iso)],
            error_message=None if is_valid else f"Volume mismatch: {diff_pct:.2%} difference",
            details={
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
        required_base = ['timestamp']

        if interval == "1d":
            required_base.extend(['open', 'high', 'low', 'close', 'volume'])
        elif interval == "tick":
            if asset == "stock":
                required_base.extend(['price', 'size', 'exchange', 'sequence'])
            # Tick data may have different schema
        else:
            # Intraday OHLC
            required_base.extend(['open', 'high', 'low', 'close', 'volume'])

        if asset == "option":
            required_base.extend(['expiration', 'strike', 'right'])

            if enrich_greeks and interval != "tick":
                required_greeks = ['delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility']
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
        timestamps = pd.to_datetime(df_sorted['timestamp'])

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
