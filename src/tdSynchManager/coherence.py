"""Data coherence checking and recovery for post-hoc validation.

This module provides tools to check data completeness and consistency between
local storage and the ThetaData source, and to recover missing data.
"""
from console_log import log_console

from contextlib import contextmanager
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from .logger import DataConsistencyLogger
from .validator import DataValidator, ValidationResult

if TYPE_CHECKING:
    from .manager import ThetaSyncManager

__all__ = [
    "CoherenceIssue",
    "CoherenceReport",
    "RecoveryResult",
    "CoherenceChecker",
    "IncoherenceRecovery"
]


@dataclass
class CoherenceIssue:
    """Represents a single coherence issue found during validation.

    Attributes
    ----------
    issue_type : str
        Type of issue (e.g., "MISSING_DAYS", "INTRADAY_GAP", "TICK_VOLUME_MISMATCH").
    severity : str
        Severity level ("ERROR", "WARNING", "INFO").
    date_range : Tuple[str, str]
        Date range affected by the issue (start, end).
    description : str
        Human-readable description of the issue.
    details : Dict
        Additional details about the issue.
    """
    issue_type: str
    severity: str
    date_range: Tuple[str, str]
    description: str
    details: Dict = field(default_factory=dict)


@dataclass
class CoherenceReport:
    """Report of coherence check results.

    Attributes
    ----------
    symbol : str
        Symbol that was checked.
    asset : str
        Asset type ("stock", "option", "index").
    interval : str
        Time interval checked.
    sink : str
        Storage sink ("csv", "parquet", "influx").
    is_coherent : bool
        True if no issues found, False otherwise.
    issues : List[CoherenceIssue]
        List of issues found.
    local_date_range : Optional[Tuple[str, str]]
        Date range available in local storage.
    source_date_range : Optional[Tuple[str, str]]
        Date range available from source.
    missing_days : List[str]
        List of missing days (for EOD data).
    intraday_gaps : List[Tuple[str, str]]
        List of intraday time gaps.
    tick_volume_mismatches : List[str]
        List of dates with tick/EOD volume mismatches.
    missing_enrichment_files : List[str]
        Missing enrichment rows files to recover (option-only).
    """
    symbol: str
    asset: str
    interval: str
    sink: str
    is_coherent: bool = True
    issues: List[CoherenceIssue] = field(default_factory=list)
    local_date_range: Optional[Tuple[str, str]] = None
    source_date_range: Optional[Tuple[str, str]] = None
    missing_days: List[str] = field(default_factory=list)
    intraday_gaps: List[Tuple[str, str]] = field(default_factory=list)
    tick_volume_mismatches: List[str] = field(default_factory=list)
    missing_enrichment_files: List[str] = field(default_factory=list)


@dataclass
class RecoveryResult:
    """Result of recovery operation.

    Attributes
    ----------
    total_issues : int
        Total number of issues attempted to recover.
    successful : int
        Number of successfully recovered issues.
    failed : int
        Number of failed recovery attempts.
    details : Dict
        Detailed results per date/range.
    """
    total_issues: int = 0
    successful: int = 0
    failed: int = 0
    details: Dict[str, bool] = field(default_factory=dict)

    def add_result(self, key: str, success: bool):
        """Add a recovery result.

        Parameters
        ----------
        key : str
            Identifier for the recovered item (e.g., date or time range).
        success : bool
            Whether recovery was successful.
        """
        self.details[key] = success
        self.total_issues += 1
        if success:
            self.successful += 1
        else:
            self.failed += 1


class CoherenceChecker:
    """Check data coherence between local storage and ThetaData source.

    This class performs post-hoc validation to identify missing data,
    gaps in coverage, and inconsistencies between tick and EOD data.
    """

    def __init__(self, manager: 'ThetaSyncManager'):
        """Initialize coherence checker.

        Parameters
        ----------
        manager : ThetaSyncManager
            Reference to ThetaSyncManager instance for accessing storage
            and client operations.
        """
        self.manager = manager
        self.client = manager.client
        self.logger = manager.logger

    async def check(
        self,
        symbol: str,
        asset: str,
        interval: str,
        sink: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> CoherenceReport:
        """Perform complete coherence check.

        Parameters
        ----------
        symbol : str
            Symbol to check (e.g., "AAPL").
        asset : str
            Asset type ("stock", "option", "index").
        interval : str
            Time interval ("1d", "5m", "tick", etc.).
        sink : str
            Storage sink ("csv", "parquet", "influx").
        start_date : Optional[str]
            Start date for check (YYYY-MM-DD). If None, use earliest available.
        end_date : Optional[str]
            End date for check (YYYY-MM-DD). If None, use latest available.

        Returns
        -------
        CoherenceReport
            Report containing all issues found.
        """
        report = CoherenceReport(
            symbol=symbol,
            asset=asset,
            interval=interval,
            sink=sink
        )

        # Get local date range
        # _get_first_last_day_from_sink returns (first_day, last_day)
        local_range_result = self.manager._get_first_last_day_from_sink(
            asset, symbol, interval, sink.lower()
        )

        if not local_range_result or not local_range_result[0]:
            report.is_coherent = False
            report.issues.append(CoherenceIssue(
                issue_type="NO_LOCAL_DATA",
                severity="ERROR",
                date_range=("", ""),
                description=f"No local data found for {symbol} ({asset}/{interval})",
                details={}
            ))
            return report

        # Extract only the date range (first two elements)
        local_range = (local_range_result[0], local_range_result[1])
        report.local_date_range = local_range

        # Use provided date range or default to local range
        check_start = start_date or local_range[0]
        check_end = end_date or local_range[1]

        # Fetch available dates from ThetaData API
        from datetime import datetime as dt
        start_dt = dt.fromisoformat(check_start).date() if check_start else None
        end_dt = dt.fromisoformat(check_end).date() if check_end else None

        log_console(f"[COHERENCE][API-DATES] Fetching available dates for {symbol} ({asset}/{interval})...")
        api_available_dates = await self.manager._fetch_available_dates_from_api(
            asset, symbol, interval, start_dt, end_dt,
            use_api_discovery=True  # Always use API discovery for coherence checks
        )

        if api_available_dates:
            log_console(f"[COHERENCE][API-DATES] Found {len(api_available_dates)} available dates for coherence check")
        else:
            log_console(f"[COHERENCE][API-DATES] API query failed, using fallback date generation")

        # Perform interval-specific checks
        if interval == "1d":
            await self._check_eod_completeness(report, check_start, check_end, api_available_dates)
        elif interval != "tick":
            await self._check_intraday_completeness(report, check_start, check_end, api_available_dates)
        else:
            await self._check_tick_completeness(report, check_start, check_end, api_available_dates)

        if asset == "option":
            await self._check_missing_enrichment_files(report, check_start, check_end)

        # Update coherence status
        report.is_coherent = len(report.issues) == 0

        return report

    async def _check_eod_completeness(
        self,
        report: CoherenceReport,
        start_date: str,
        end_date: str,
        api_available_dates: Optional[set] = None
    ):
        """Check EOD data completeness.

        Parameters
        ----------
        report : CoherenceReport
            Report to populate with issues.
        start_date : str
            Start date (YYYY-MM-DD).
        end_date : str
            End date (YYYY-MM-DD).
        api_available_dates : Optional[set]
            Set of available dates from ThetaData API (if fetched).
        """
        # Get missing days from Manager
        missing_days = self.manager._missing_1d_days_csv(
            report.asset,
            report.symbol,
            report.interval,
            report.sink,
            start_date,
            end_date,
            api_available_dates
        )

        if not missing_days:
            return

        # Filter out weekends
        trading_days = [
            d for d in missing_days
            if datetime.fromisoformat(d).weekday() < 5
        ]

        if trading_days:
            report.missing_days = trading_days
            report.is_coherent = False
            report.issues.append(CoherenceIssue(
                issue_type="MISSING_DAYS",
                severity="ERROR",
                date_range=(trading_days[0], trading_days[-1]),
                description=f"Missing {len(trading_days)} trading days",
                details={'missing_dates': trading_days}
            ))

    async def _check_intraday_completeness(
        self,
        report: CoherenceReport,
        start_date: str,
        end_date: str,
        api_available_dates: Optional[set] = None
    ):
        """Check intraday data completeness.

        This method reads intraday data from local storage for each day in the range
        and validates that all expected candles are present based on the interval and
        market hours. For days with missing candles, it attempts to identify the specific
        time segments with problems using progressive segmentation (e.g., if 5m data has
        gaps, it can identify which 30-minute blocks are affected).

        Parameters
        ----------
        report : CoherenceReport
            Report to populate with issues.
        start_date : str
            Start date (YYYY-MM-DD).
        end_date : str
            End date (YYYY-MM-DD).
        api_available_dates : Optional[set]
            Set of available dates from ThetaData API (if fetched).
        """
        from .validator import DataValidator

        # Determine which dates to check
        if api_available_dates:
            dates = sorted(list(api_available_dates))
        else:
            dates = self._generate_date_range(start_date, end_date)

        for date_iso in dates:
            # Skip weekends (only if using fallback date generation)
            if api_available_dates is None:
                date_obj = datetime.fromisoformat(date_iso)
                if date_obj.weekday() >= 5:  # Saturday or Sunday
                    continue

            # Try to read data for this day from local storage
            # Use Manager's methods to access local data
            try:
                # Get data for this specific day using manager's read methods
                # For CSV/Parquet, we need to read the file
                df = await self._read_intraday_day(report.symbol, report.asset, report.interval, date_iso, report.sink)

                if df is None or df.empty:
                    report.is_coherent = False
                    report.issues.append(CoherenceIssue(
                        issue_type="MISSING_INTRADAY_DATA",
                        severity="ERROR",
                        date_range=(date_iso, date_iso),
                        description=f"No intraday data found for {date_iso}",
                        details={}
                    ))
                    continue

                # Validate candle completeness
                expected_combo_total = None
                if report.asset == "option":
                    try:
                        expected_combo_total = await self.manager._expected_option_combos_for_day(report.symbol, date_iso)
                        if expected_combo_total:
                            log_console(f"[COHERENCE][INFO] {report.symbol} {report.interval} {date_iso} attese_combo={expected_combo_total}")
                    except Exception as e:
                        log_console(f"[COHERENCE][WARN] expected combos fetch failed {report.symbol} {date_iso}: {e}")

                validation_result = DataValidator.validate_intraday_completeness(
                    df=df,
                    date_iso=date_iso,
                    interval=report.interval,
                    asset=report.asset,
                    bucket_tolerance=self.manager.cfg.intraday_bucket_tolerance,
                    expected_combo_total=expected_combo_total
                )

                if not validation_result.valid:
                    # Identify problematic time segments using segmentation
                    problem_segments = await self._segment_intraday_problems(
                        df, date_iso, report.interval, report.asset
                    )
                    combined_segments = problem_segments or validation_result.details.get('gaps', [])
                    for seg in combined_segments:
                        if seg not in report.intraday_gaps:
                            report.intraday_gaps.append(seg)

                    report.is_coherent = False
                    report.issues.append(CoherenceIssue(
                        issue_type="INCOMPLETE_INTRADAY_CANDLES",
                        severity="ERROR",
                        date_range=(date_iso, date_iso),
                        description=validation_result.error_message or f"Missing candles on {date_iso}",
                        details={
                            **validation_result.details,
                            'problem_segments': problem_segments
                        }
                    ))

            except Exception as e:
                self.logger.log_failure(
                    symbol=report.symbol,
                    asset=report.asset,
                    interval=report.interval,
                    date_range=(date_iso, date_iso),
                    message=f"Error checking intraday completeness: {str(e)}",
                    details={'error_type': type(e).__name__}
                )

    async def _check_tick_completeness(
        self,
        report: CoherenceReport,
        start_date: str,
        end_date: str,
        api_available_dates: Optional[set] = None
    ):
        """Check tick data completeness vs EOD volumes.

        This method validates that the sum of tick volumes matches the EOD volume
        for each day. For options, it can validate separately for calls and puts
        if the EOD data provides separate volumes.

        Parameters
        ----------
        report : CoherenceReport
            Report to populate with issues.
        start_date : str
            Start date (YYYY-MM-DD).
        end_date : str
            End date (YYYY-MM-DD).
        api_available_dates : Optional[set]
            Set of available dates from ThetaData API (if fetched).
        """
        from .validator import DataValidator

        # Determine which dates to check
        if api_available_dates:
            dates = sorted(list(api_available_dates))
        else:
            dates = self._generate_date_range(start_date, end_date)

        for date_iso in dates:
            # Skip weekends (only if using fallback date generation)
            if api_available_dates is None:
                date_obj = datetime.fromisoformat(date_iso)
                if date_obj.weekday() >= 5:
                    continue

            try:
                # Read tick data for this day
                tick_df = await self._read_tick_day(report.symbol, report.asset, date_iso, report.sink)

                if tick_df is None or tick_df.empty:
                    report.is_coherent = False
                    report.missing_days.append(date_iso)  # Add to missing_days so recovery can handle it
                    report.issues.append(CoherenceIssue(
                        issue_type="MISSING_TICK_DATA",
                        severity="ERROR",
                        date_range=(date_iso, date_iso),
                        description=f"No tick data found for {date_iso}",
                        details={}
                    ))
                    continue

                # Get EOD volume for comparison
                eod_volume, eod_volume_call, eod_volume_put = await self._get_eod_volumes(
                    report.symbol, report.asset, date_iso, report.sink
                )

                if eod_volume is None:
                    self.logger.log_failure(
                        symbol=report.symbol,
                        asset=report.asset,
                        interval="tick",
                        date_range=(date_iso, date_iso),
                        message=f"Could not retrieve EOD volume for tick validation on {date_iso}",
                        details={}
                    )
                    continue

                # Validate tick vs EOD volume
                validation_result = DataValidator.validate_tick_vs_eod_volume(
                    tick_df=tick_df,
                    eod_volume=eod_volume,
                    date_iso=date_iso,
                    asset=report.asset,
                    tolerance=self.manager.cfg.tick_eod_volume_tolerance,
                    eod_volume_call=eod_volume_call,
                    eod_volume_put=eod_volume_put
                )

                if not validation_result.valid:
                    # If volume mismatch, try to segment the day to find problem hours
                    problem_segments = await self._segment_tick_problems(tick_df, date_iso, report.asset)

                    # Optionally run granular bucket analysis (comparing tick vs intraday bars)
                    bucket_analysis = None
                    if self.manager.cfg.enable_tick_bucket_analysis:
                        from .tick_bucket_analysis import analyze_tick_buckets
                        try:
                            # Determine intraday interval from config (default 30m)
                            interval_map = {30: "30m", 60: "1h", 15: "15m", 5: "5m", 1: "1m"}
                            intraday_interval = interval_map.get(
                                self.manager.cfg.tick_segment_minutes,
                                "30m"
                            )

                            bucket_analysis = await analyze_tick_buckets(
                                manager=self.manager,
                                symbol=report.symbol,
                                asset=report.asset,
                                date_iso=date_iso,
                                sink=report.sink,
                                intraday_interval=intraday_interval,
                                tolerance=self.manager.cfg.tick_eod_volume_tolerance
                            )

                            # Compare EOD delta vs Bucket delta
                            if bucket_analysis:
                                # Calculate tick total from tick_df
                                volume_col = 'size' if report.asset == "option" else 'volume'
                                tick_total = tick_df[volume_col].sum() if volume_col in tick_df.columns else 0

                                eod_total = eod_volume_call + eod_volume_put
                                intraday_total = bucket_analysis.total_intraday_volume

                                if tick_total > 0:
                                    delta_eod = eod_total - tick_total
                                    delta_bucket = intraday_total - tick_total
                                    delta_diff = abs(delta_eod) - abs(delta_bucket)

                                    log_console(f"\n[DELTA-COMPARISON] {date_iso} - EOD vs Bucket Analysis:")
                                    log_console(f"  Tick total:       {int(tick_total)}")
                                    log_console(f"  EOD total:        {int(eod_total)}")
                                    log_console(f"  Intraday total:   {int(intraday_total)}")
                                    log_console(f"  Delta EOD:        {delta_eod:+.0f} ({abs(delta_eod/tick_total)*100:.3f}%)")
                                    log_console(f"  Delta Bucket:     {delta_bucket:+.0f} ({abs(delta_bucket/tick_total)*100:.3f}%)")
                                    log_console(f"  Difference:       {delta_diff:.0f} volume")
                                    if abs(delta_eod) > 0:
                                        ratio = (1 - abs(delta_bucket)/abs(delta_eod)) * 100
                                        log_console(f"  Bucket {ratio:+.1f}% more accurate than EOD")
                        except Exception as e:
                            log_console(f"[BUCKET-ANALYSIS] Failed for {date_iso}: {e}")

                    report.is_coherent = False
                    if date_iso not in report.tick_volume_mismatches:
                        report.tick_volume_mismatches.append(date_iso)

                    issue_details = {
                        **validation_result.details,
                        'problem_segments': problem_segments
                    }

                    # Add bucket analysis to details if available
                    if bucket_analysis:
                        issue_details['bucket_analysis'] = {
                            'intraday_interval': bucket_analysis.intraday_interval,
                            'total_buckets': len(bucket_analysis.buckets),
                            'coherent_buckets': sum(1 for b in bucket_analysis.buckets if b.is_coherent),
                            'problematic_buckets': [
                                {
                                    'start': b.bucket_start,
                                    'end': b.bucket_end,
                                    'tick_volume': b.tick_volume,
                                    'intraday_volume': b.intraday_volume,
                                    'diff_pct': b.diff_pct
                                }
                                for b in bucket_analysis.buckets if not b.is_coherent
                            ]
                        }

                    report.issues.append(CoherenceIssue(
                        issue_type="TICK_VOLUME_MISMATCH",
                        severity="WARNING",
                        date_range=(date_iso, date_iso),
                        description=validation_result.error_message or f"Tick volume mismatch on {date_iso}",
                        details=issue_details
                    ))

            except Exception as e:
                self.logger.log_failure(
                    symbol=report.symbol,
                    asset=report.asset,
                    interval="tick",
                    date_range=(date_iso, date_iso),
                    message=f"Error checking tick completeness: {str(e)}",
                    details={'error_type': type(e).__name__}
                )

    @staticmethod
    def _generate_date_range(start_date: str, end_date: str) -> List[str]:
        """Generate list of dates between start and end.

        Parameters
        ----------
        start_date : str
            Start date (YYYY-MM-DD).
        end_date : str
            End date (YYYY-MM-DD).

        Returns
        -------
        List[str]
            List of dates in ISO format.
        """
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        dates = []

        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)

        return dates

    async def _read_intraday_day(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_iso: str,
        sink: str
    ):
        """Read intraday data for a specific day from local storage.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        date_iso : str
            Date (YYYY-MM-DD).
        sink : str
            Storage sink (csv, parquet, influxdb).

        Returns
        -------
        pd.DataFrame or None
            DataFrame with intraday data, or None if not found.
        """
        import pandas as pd

        # Handle InfluxDB sink by querying the database
        if sink.lower() == "influxdb":
            measurement = f"{symbol}-{asset}-{interval}"
            # Use next day for upper bound to include all timestamps (including nanoseconds)
            # Pattern: time >= start AND time < next_day (exclusive upper bound)
            from datetime import datetime, timedelta
            next_day_iso = (datetime.fromisoformat(date_iso) + timedelta(days=1)).strftime('%Y-%m-%d')
            query = f"""
                SELECT *
                FROM "{measurement}"
                WHERE time >= to_timestamp('{date_iso}T00:00:00Z')
                  AND time < to_timestamp('{next_day_iso}T00:00:00Z')
                ORDER BY time
            """
            try:
                df = self.manager._influx_query_dataframe(query)
                if df.empty:
                    return None
                # Rename 'time' column to 'timestamp' for validator compatibility
                if 'time' in df.columns:
                    df = df.rename(columns={'time': 'timestamp'})
                return df
            except Exception:
                return None

        # Use manager's file listing to find ALL files for this day
        files = self.manager._list_series_files(asset, symbol, interval, sink.lower())

        # Find ALL files for this specific date (there may be multiple _partNN files)
        target_files = []
        for f in files:
            if date_iso in f:
                target_files.append(f)

        if not target_files:
            return None

        # Read and concatenate ALL part files for this day
        try:
            dfs = []
            for target_file in target_files:
                if sink.lower() == "csv":
                    df_part = pd.read_csv(target_file)
                elif sink.lower() == "parquet":
                    df_part = pd.read_parquet(target_file)
                else:
                    continue

                if not df_part.empty:
                    dfs.append(df_part)

            if not dfs:
                return None

            # Concatenate all parts
            df = pd.concat(dfs, ignore_index=True)

            # Remove duplicates if any (use all columns as key for safety)
            df = df.drop_duplicates()

            return df
        except Exception:
            return None

    async def _read_tick_day(
        self,
        symbol: str,
        asset: str,
        date_iso: str,
        sink: str
    ):
        """Read tick data for a specific day from local storage.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        date_iso : str
            Date (YYYY-MM-DD).
        sink : str
            Storage sink.

        Returns
        -------
        pd.DataFrame or None
            DataFrame with tick data, or None if not found.
        """
        # Similar to _read_intraday_day but for tick interval
        return await self._read_intraday_day(symbol, asset, "tick", date_iso, sink)

    async def _get_eod_volumes(
        self,
        symbol: str,
        asset: str,
        date_iso: str,
        sink: str
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Get EOD volumes for a specific date.

        For options, attempts to get separate call and put volumes.
        This method now delegates to the unified function in ThetaSyncManager.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        date_iso : str
            Date (YYYY-MM-DD).
        sink : str
            Storage sink.

        Returns
        -------
        Tuple[Optional[float], Optional[float], Optional[float]]
            (total_volume, call_volume, put_volume). For non-options, call and put volumes are None.
        """
        # Delegate to the unified function in manager (supports CSV/Parquet/InfluxDB + API fallback)
        return await self.manager._get_eod_volume_for_validation(
            symbol=symbol,
            asset=asset,
            date_iso=date_iso,
            sink=sink
        )

    async def _segment_intraday_problems(
        self,
        df,
        date_iso: str,
        interval: str,
        asset: str
    ) -> List[Dict]:
        """Segment intraday data to identify problematic time blocks.

        Divides the trading day into 30-minute segments and checks which segments
        have missing or incomplete data. This helps identify specific hours with problems.

        Parameters
        ----------
        df : pd.DataFrame
            Intraday data for the day.
        date_iso : str
            Date (YYYY-MM-DD).
        interval : str
            Time interval.
        asset : str
            Asset type.

        Returns
        -------
        List[Dict]
            List of problematic segments with details.
        """
        from .validator import DataValidator

        problems = []

        # Ensure we have a timestamp column
        ts_col = None
        for col in ['timestamp', 'created', 'ms_of_day']:
            if col in df.columns:
                ts_col = col
                break

        if ts_col is None:
            return problems

        # Parse timestamps
        import pandas as pd
        df = df.copy()

        try:
            if ts_col == 'ms_of_day':
                # Convert ms_of_day to datetime (ms_of_day represents ET time)
                base_date = pd.to_datetime(date_iso)
                df['_parsed_time'] = (base_date + pd.to_timedelta(df[ts_col], unit='ms')).dt.tz_localize("America/New_York")
            else:
                df['_parsed_time'] = pd.to_datetime(df[ts_col], errors='coerce', utc=True).dt.tz_convert("America/New_York")

            # Market hours: 9:30 AM - 4:00 PM ET
            market_start = pd.Timestamp(f"{date_iso} 09:30:00", tz="America/New_York")
            market_end = pd.Timestamp(f"{date_iso} 16:00:00", tz="America/New_York")

            # Create 30-minute segments
            segment_duration = timedelta(minutes=30)
            current_start = market_start

            while current_start < market_end:
                current_end = current_start + segment_duration

                # Filter data in this segment
                segment_df = df[
                    (df['_parsed_time'] >= current_start) &
                    (df['_parsed_time'] < current_end)
                ]

                # Validate this segment
                # Note: validator uses full day validation, so we just check if segment is empty
                # or has significantly fewer candles than expected
                expected_segment_candles = DataValidator._expected_candles_for_interval(interval, asset) / 13  # ~13 30-min segments in trading day

                if len(segment_df) < expected_segment_candles * 0.5:  # 50% tolerance for segment
                    validation_result = ValidationResult(
                        valid=False,
                        missing_ranges=[],
                        error_message=f"Segment has only {len(segment_df)} candles, expected ~{int(expected_segment_candles)}",
                        details={'expected': int(expected_segment_candles), 'actual': len(segment_df)}
                    )
                else:
                    validation_result = ValidationResult(valid=True, missing_ranges=[], error_message=None, details={})

                if not validation_result.valid:
                    problems.append({
                        'segment_start': current_start.strftime("%H:%M:%S"),
                        'segment_end': current_end.strftime("%H:%M:%S"),
                        'issue': validation_result.error_message,
                        'details': validation_result.details
                    })

                current_start = current_end

        except Exception as e:
            self.logger.log_failure(
                symbol="",
                asset=asset,
                interval=interval,
                date_range=(date_iso, date_iso),
                message=f"Error segmenting intraday problems: {str(e)}",
                details={'error_type': type(e).__name__}
            )

        return problems

    async def _check_missing_enrichment_files(
        self,
        report: CoherenceReport,
        start_date: str,
        end_date: str,
    ):
        files = self.manager._list_missing_enrichment_files(
            report.asset,
            report.symbol,
            report.interval,
            report.sink.lower(),
            start_date=start_date,
            end_date=end_date,
        )
        for day_iso, path in files:
            pending = self.manager._pending_missing_enrichment_rows(path)
            if pending <= 0:
                continue
            self.logger.log_info(
                symbol=report.symbol,
                asset=report.asset,
                interval=report.interval,
                date_range=(day_iso, day_iso),
                message=f"Missing-enrichment file found ({pending} pending): {os.path.basename(path)}",
                details={"file": path, "missing_rows": pending},
            )
            report.missing_enrichment_files.append(path)
            report.issues.append(CoherenceIssue(
                issue_type="MISSING_ENRICHMENT_ROWS",
                severity="WARNING",
                date_range=(day_iso, day_iso),
                description=f"{pending} missing enrichment rows pending on {day_iso}",
                details={"file": path, "missing_rows": pending}
            ))

    async def _segment_tick_problems(
        self,
        tick_df,
        date_iso: str,
        asset: str = "option"
    ) -> List[Dict]:
        """Segment tick data to identify problematic hour blocks.

        Divides the trading day into 1-hour segments and computes volume statistics
        for each segment to identify hours with anomalous volumes.

        Parameters
        ----------
        tick_df : pd.DataFrame
            Tick data for the day.
        date_iso : str
            Date (YYYY-MM-DD).
        asset : str
            Asset type ('option' or 'stock'). Used to determine volume column name.
            Default: 'option'

        Returns
        -------
        List[Dict]
            List of segments with volume statistics.
        """
        import pandas as pd

        segments = []

        # Find timestamp column
        # Prefer quote_timestamp for options (has correct date), fallback to others
        ts_col = None
        for col in ['quote_timestamp', 'timestamp', 'created', 'ms_of_day']:
            if col in tick_df.columns:
                ts_col = col
                break

        # Determine correct volume column based on asset type
        # OPTIONS: 'size' column (quantity per trade)
        # STOCK/INDEX: 'volume' column
        volume_col = 'size' if asset == "option" else 'volume'

        if ts_col is None or volume_col not in tick_df.columns:
            return segments

        try:
            tick_df = tick_df.copy()

            if ts_col == 'ms_of_day':
                # ms_of_day represents ET time, not UTC
                base_date = pd.to_datetime(date_iso)
                tick_df['_parsed_time'] = (base_date + pd.to_timedelta(tick_df[ts_col], unit='ms')).dt.tz_localize("America/New_York")
            else:
                tick_df['_parsed_time'] = pd.to_datetime(tick_df[ts_col], errors='coerce', utc=True).dt.tz_convert("America/New_York")

            # Market hours: 9:30 AM - 4:00 PM ET
            market_start = pd.Timestamp(f"{date_iso} 09:30:00", tz="America/New_York")
            market_end = pd.Timestamp(f"{date_iso} 16:00:00", tz="America/New_York")

            # Create segments (default 30 minutes, configurable)
            # Use manager config if available, otherwise default to 30 minutes
            segment_minutes = getattr(self.manager.cfg, 'tick_segment_minutes', 30)
            segment_duration = timedelta(minutes=segment_minutes)
            current_start = market_start

            while current_start < market_end:
                current_end = current_start + segment_duration

                segment_df = tick_df[
                    (tick_df['_parsed_time'] >= current_start) &
                    (tick_df['_parsed_time'] < current_end)
                ]

                segment_volume = segment_df[volume_col].sum() if not segment_df.empty else 0
                segment_ticks = len(segment_df)

                segments.append({
                    'hour_start': current_start.strftime("%H:%M:%S"),
                    'hour_end': current_end.strftime("%H:%M:%S"),
                    'volume': float(segment_volume),
                    'tick_count': segment_ticks
                })

                current_start = current_end

        except Exception as e:
            # Log exception but don't fail
            log_console(f"[DEBUG] _segment_tick_problems exception: {e}")

        # Log segment summary
        if segments:
            log_console(f"\n[TICK-SEGMENT] {date_iso} - Segmented into {len(segments)} buckets ({segment_minutes}min each):")
            for seg in segments:
                log_console(f"  [{seg['hour_start']}-{seg['hour_end']}] volume={int(seg['volume'])} ticks={seg['tick_count']}")

        return segments


class IncoherenceRecovery:
    """Recover from coherence issues by downloading and saving missing data."""

    def __init__(self, manager: 'ThetaSyncManager'):
        """Initialize recovery handler.

        Parameters
        ----------
        manager : ThetaSyncManager
            Reference to ThetaSyncManager instance.
        """
        self.manager = manager
        self.client = manager.client
        self.logger = manager.logger
        self.retry_policy = manager.cfg.retry_policy

    @contextmanager
    def _suspend_validation(self):
        cfg = self.manager.cfg
        original_enable = cfg.enable_data_validation
        original_strict = cfg.validation_strict_mode
        cfg.enable_data_validation = False
        cfg.validation_strict_mode = False
        try:
            yield
        finally:
            cfg.enable_data_validation = original_enable
            cfg.validation_strict_mode = original_strict

    async def recover(
        self,
        report: CoherenceReport,
        enrich_greeks: bool = False
    ) -> RecoveryResult:
        """Recover from coherence issues.

        Parameters
        ----------
        report : CoherenceReport
            Coherence report with identified issues.
        enrich_greeks : bool, optional
            Whether to enrich with Greeks for options (default False).

        Returns
        -------
        RecoveryResult
            Result of recovery operations.
        """
        result = RecoveryResult()

        # Recover missing enrichment rows (option-only)
        for path in report.missing_enrichment_files:
            success = await self._recover_missing_enrichment_file(
                report.symbol,
                report.asset,
                report.interval,
                report.sink,
                path,
                enrich_greeks
            )
            key = f"missing_enrichment_{os.path.basename(path)}"
            result.add_result(key, success)

        # Recover missing EOD days
        for day in report.missing_days:
            success = await self._recover_eod_day(
                report.symbol,
                report.asset,
                report.interval,
                day,
                report.sink,
                enrich_greeks
            )
            result.add_result(f"eod_{day}", success)

        # Recover intraday gaps
        for gap_start, gap_end in report.intraday_gaps:
            success = await self._recover_intraday_gap(
                report.symbol,
                report.asset,
                report.interval,
                gap_start,
                gap_end,
                report.sink,
                enrich_greeks
            )
            result.add_result(f"intraday_{gap_start}_{gap_end}", success)

        # Recover tick volume mismatches
        for date_iso in report.tick_volume_mismatches:
            success = await self._recover_tick_day(
                report.symbol,
                report.asset,
                date_iso,
                report.sink
            )
            result.add_result(f"tick_{date_iso}", success)

        return result

    async def _recover_eod_day(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_iso: str,
        sink: str,
        enrich_greeks: bool
    ) -> bool:
        """Recover a single missing EOD day.

        This method first checks if local storage already contains data for the specified date.
        If data exists, it skips the download and returns success immediately. Otherwise, it
        downloads the data from ThetaData API with retry logic and saves it to the configured sink.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Interval (should be "1d").
        date_iso : str
            Date to recover (YYYY-MM-DD).
        sink : str
            Storage sink.
        enrich_greeks : bool
            Whether to enrich with Greeks.

        Returns
        -------
        bool
            True if recovery successful or data already exists, False otherwise.
        """
        # First, check if local storage already has data for this date
        # This avoids unnecessary API calls when data exists but wasn't detected during initial check
        # _get_first_last_day_from_sink returns (first_day, last_day)
        local_range_result = self.manager._get_first_last_day_from_sink(
            asset, symbol, interval, sink.lower()
        )

        if local_range_result:
            local_range = (local_range_result[0], local_range_result[1])
        else:
            local_range = None

        if local_range and local_range[0] and local_range[1]:
            # Check if date_iso falls within the local range
            if local_range[0] <= date_iso <= local_range[1]:
                # Further verify by checking missing days
                missing_days = self.manager._missing_1d_days_csv(
                    asset, symbol, interval, sink.lower(),
                    date_iso, date_iso
                )

                if not missing_days:
                    # Data already exists locally, no need to re-download
                    self.logger.log_resolution(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=(date_iso, date_iso),
                        message=f"EOD data for {date_iso} already exists locally, skipped download",
                        details={'recovery_method': 'local_verification'}
                    )
                    return True

        try:
            self.manager._purge_day_files(asset, symbol, interval, sink, date_iso)
            # Download with validation enabled to ensure data quality
            await self.manager._download_and_store_options(
                symbol=symbol,
                interval=interval,
                day_iso=date_iso,
                sink=sink,
                enrich_greeks=enrich_greeks,
                enrich_tick_greeks=False,
                dedupe_influx_against_db=(sink.lower() == "influxdb"),
            )
        except Exception as e:
            self.logger.log_failure(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(date_iso, date_iso),
                message=f"Recovery failed for {date_iso}: {e}",
                details={'error_type': type(e).__name__}
            )
            return False

        self.logger.log_resolution(
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(date_iso, date_iso),
            message=f"Recovered EOD data for {date_iso} via API download",
            details={'recovery_method': 'api_download'}
        )

        return True

    async def _influx_intraday_gap_still_missing(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_iso: str,
    ) -> bool:
        """Return True if intraday gaps still exist in Influx for the given day."""
        base_path = self.manager._make_file_basepath(
            asset, symbol, interval, f"{date_iso}T00-00-00Z", "influxdb"
        )
        measurement = self.manager._influx_measurement_from_base(base_path)

        try:
            if not self.manager._influx_measurement_exists(measurement):
                return True
        except Exception as e:
            log_console(f"[RECOVERY][WARN] influx measurement check failed ({measurement}): {type(e).__name__}: {e}")
            return True

        start_utc, end_utc = self.manager._influx__et_day_bounds_to_utc(date_iso)
        select_cols = ["time"]
        if asset == "option":
            select_cols += ["symbol", "expiration", "strike", "right"]
            if interval == "tick":
                select_cols.append("sequence")
        else:
            select_cols.append("symbol")

        query = (
            f'SELECT {", ".join(select_cols)} FROM "{measurement}" '
            f"WHERE time >= TIMESTAMP '{start_utc}' AND time < TIMESTAMP '{end_utc}'"
        )
        df = self.manager._influx_query_dataframe(query)
        if df is None or df.empty:
            return True

        if "time" in df.columns:
            df = df.rename(columns={"time": "timestamp"})

        expected_combo_total = None
        if asset == "option":
            try:
                expected_combo_total = await self.manager._expected_option_combos_for_day(symbol, date_iso)
            except Exception as e:
                log_console(f"[RECOVERY][WARN] expected combos fetch failed {symbol} {date_iso}: {e}")

        validation_result = DataValidator.validate_intraday_completeness(
            df=df,
            date_iso=date_iso,
            interval=interval,
            asset=asset,
            bucket_tolerance=self.manager.cfg.intraday_bucket_tolerance,
            expected_combo_total=expected_combo_total
        )
        return not validation_result.valid

    async def _recover_intraday_gap(
        self,
        symbol: str,
        asset: str,
        interval: str,
        gap_start: str,
        gap_end: str,
        sink: str,
        enrich_greeks: bool
    ) -> bool:
        """Recover intraday gap.

        Downloads intraday data for the date(s) covering the gap with abundant time margins.
        For example, if the gap is on 2024-01-15, it downloads with a wider date range and
        uses the manager's merge/append logic to integrate the data.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        gap_start : str
            Gap start date (YYYY-MM-DD).
        gap_end : str
            Gap end date (YYYY-MM-DD).
        sink : str
            Storage sink.
        enrich_greeks : bool
            Whether to enrich with Greeks.

        Returns
        -------
        bool
            True if recovery successful, False otherwise.
        """
        start_obj = datetime.fromisoformat(gap_start)
        end_obj = datetime.fromisoformat(gap_end)

        current_date = start_obj
        all_success = True
        sink_lower = sink.lower()

        while current_date <= end_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            if sink_lower == "influxdb":
                try:
                    still_missing = await self._influx_intraday_gap_still_missing(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_iso=date_str,
                    )
                except Exception as e:
                    log_console(f"[RECOVERY][WARN] gap check failed {symbol} {interval} {date_str}: {e}")
                    still_missing = True

                if not still_missing:
                    self.logger.log_resolution(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=(date_str, date_str),
                        message=f"Intraday data for {date_str} already complete in InfluxDB, skipped download",
                        details={'recovery_method': 'influx_check'}
                    )
                    current_date += timedelta(days=1)
                    continue
            try:
                self.manager._purge_day_files(asset, symbol, interval, sink, date_str)
                # Download with validation enabled to ensure data quality
                await self.manager._download_and_store_options(
                    symbol=symbol,
                    interval=interval,
                    day_iso=date_str,
                    sink=sink,
                    enrich_greeks=enrich_greeks,
                    enrich_tick_greeks=False,
                    dedupe_influx_against_db=(sink_lower == "influxdb"),
                    force_full_day=True,
                )
                self.logger.log_resolution(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(date_str, date_str),
                    message=f"Recovered intraday data for {date_str}",
                    details={'recovery_method': 'api_download'}
                )
            except Exception as e:
                all_success = False
                self.logger.log_failure(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(date_str, date_str),
                    message=f"Failed to recover intraday data for {date_str}: {e}",
                    details={'error_type': type(e).__name__}
                )

            current_date += timedelta(days=1)

        return all_success

    async def _recover_tick_day(
        self,
        symbol: str,
        asset: str,
        date_iso: str,
        sink: str
    ) -> bool:
        """Recover tick data for a day with volume mismatch.

        Re-downloads tick data for the specified day. Since tick data can be large and
        have volume mismatches due to incomplete downloads, this method re-downloads
        the entire day's data with retry logic to ensure completeness.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        date_iso : str
            Date to recover (YYYY-MM-DD).
        sink : str
            Storage sink.

        Returns
        -------
        bool
            True if recovery successful, False otherwise.
        """
        try:
            self.manager._purge_day_files(asset, symbol, 'tick', sink, date_iso)
            # Download with validation enabled to ensure data quality
            await self.manager._download_and_store_options(
                symbol=symbol,
                interval='tick',
                day_iso=date_iso,
                sink=sink,
                enrich_greeks=False,
                enrich_tick_greeks=False,
                dedupe_influx_against_db=(sink.lower() == "influxdb"),
            )
        except Exception as e:
            self.logger.log_failure(
                symbol=symbol,
                asset=asset,
                interval='tick',
                date_range=(date_iso, date_iso),
                message=f"Failed to recover tick data for {date_iso}: {e}",
                details={'error_type': type(e).__name__}
            )
            return False

        self.logger.log_resolution(
            symbol=symbol,
            asset=asset,
            interval='tick',
            date_range=(date_iso, date_iso),
            message=f"Recovered tick data for {date_iso}",
            details={'recovery_method': 'api_download'}
        )
        return True

    async def _recover_missing_enrichment_file(
        self,
        symbol: str,
        asset: str,
        interval: str,
        sink: str,
        path: str,
        enrich_greeks: bool,
    ) -> bool:
        if asset != "option":
            return True

        try:
            import pandas as pd
            df = pd.read_csv(path, dtype=str)
        except Exception as e:
            self.logger.log_failure(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=("", ""),
                message=f"Missing-enrichment recovery failed (read): {e}",
                details={"file": path, "error_type": type(e).__name__}
            )
            return False

        if df is None or df.empty:
            return True

        recovered_vals = df["recovered"].astype(str).str.strip().str.lower() if "recovered" in df.columns else None
        pending_mask = ~recovered_vals.isin(["true", "1", "yes"]) if recovered_vals is not None else pd.Series([True] * len(df), index=df.index)
        if not pending_mask.any():
            return True

        day_iso = None
        if "day" in df.columns:
            day_vals = df.loc[pending_mask, "day"].dropna().unique().tolist()
            if day_vals:
                day_iso = day_vals[0]
        if not day_iso:
            m = re.search(r"missing_enrichment-(\d{4}-\d{2}-\d{2})T00-00-00Z", os.path.basename(path))
            day_iso = m.group(1) if m else None
        if not day_iso:
            self.logger.log_failure(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=("", ""),
                message="Missing-enrichment recovery failed (day missing)",
                details={"file": path}
            )
            return False

        pending_count = int(pending_mask.sum())
        self.logger.log_info(
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(day_iso, day_iso),
            message=f"Missing-enrichment recovery start: file={os.path.basename(path)} pending={pending_count}",
            details={"file": path, "pending_rows": pending_count},
        )

        if interval != "1d" and "timestamp" not in df.columns:
            self.logger.log_failure(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(day_iso, day_iso),
                message="Missing-enrichment recovery failed (timestamp missing)",
                details={"file": path}
            )
            return False

        if not enrich_greeks and interval != "tick":
            log_console(f"[RECOVERY][WARN] Forcing enrich_greeks=True for missing rows ({symbol} {interval} {day_iso})")
            enrich_greeks = True

        keys_cols = [c for c in ["timestamp", "symbol", "expiration", "strike", "right", "sequence"] if c in df.columns]
        keys_df = df.loc[pending_mask, keys_cols].copy()
        keys_norm = self.manager._normalize_recovery_keys(keys_df, interval)
        key_cols = self.manager._recovery_key_columns(interval, keys_norm)
        if not key_cols:
            return False
        keys_norm = keys_norm[key_cols].dropna().drop_duplicates()
        attempted_keys = set(tuple(row) for row in keys_norm.itertuples(index=False, name=None))
        if not attempted_keys:
            return True

        self.logger.log_info(
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(day_iso, day_iso),
            message=f"Missing-enrichment recovery: trying to download {len(attempted_keys)} keys",
            details={"file": path, "attempted_keys": len(attempted_keys)},
        )

        sink_lower = sink.lower()
        keys_to_recover = keys_norm
        existing_keys: set[tuple] = set()

        if sink_lower == "influxdb":
            base_path = self.manager._make_file_basepath(
                asset, symbol, interval, f"{day_iso}T00-00-00Z", sink_lower
            )
            measurement = self.manager._influx_measurement_from_base(base_path)
            existing_keys, missing_keys = self.manager._influx_split_recovery_keys(
                keys_norm, measurement, interval
            )
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(day_iso, day_iso),
                message=f"Missing-enrichment key split: existing={len(existing_keys)} missing={len(missing_keys)}",
                details={"file": path, "existing_keys": len(existing_keys), "missing_keys": len(missing_keys)},
            )
            if missing_keys is not None and missing_keys.empty:
                remaining = self.manager._update_missing_enrichment_recovery(
                    path,
                    interval,
                    attempted_keys,
                    existing_keys,
                )
                self.logger.log_info(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message=f"Missing-enrichment recovery: all pending keys already in DB (remaining={remaining})",
                    details={"file": path, "remaining_rows": remaining},
                )
                return remaining == 0
            if missing_keys is not None:
                keys_to_recover = missing_keys

        try:
            with self._suspend_validation():
                df_written = await self.manager._download_and_store_options(
                    symbol=symbol,
                    interval=interval,
                    day_iso=day_iso,
                    sink=sink,
                    enrich_greeks=enrich_greeks,
                    enrich_tick_greeks=(interval == "tick"),
                    recover_only_keys=keys_to_recover,
                    return_df=True,
                    record_missing_rows=False,
                    missing_policy_override="candle_or_tick_row",
                    skip_if_exists=False,
                    dedupe_influx_against_db=(sink_lower == "influxdb"),
                )
        except Exception as e:
            self.logger.log_failure(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(day_iso, day_iso),
                message=f"Missing-enrichment recovery failed: {e}",
                details={"file": path, "error_type": type(e).__name__}
            )
            return False

        recovered_keys = set()
        if df_written is not None and not df_written.empty:
            written_norm = self.manager._normalize_recovery_keys(df_written, interval)
            written_cols = self.manager._recovery_key_columns(interval, written_norm)
            if written_cols:
                written_norm = written_norm[written_cols].dropna().drop_duplicates()
                recovered_keys = set(tuple(row) for row in written_norm.itertuples(index=False, name=None))

        if existing_keys:
            recovered_keys = recovered_keys.union(existing_keys)

        remaining = self.manager._update_missing_enrichment_recovery(
            path,
            interval,
            attempted_keys,
            recovered_keys,
        )

        self.logger.log_info(
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(day_iso, day_iso),
            message=f"Missing-enrichment recovery result: recovered={len(recovered_keys)} remaining={remaining}",
            details={"file": path, "recovered_keys": len(recovered_keys), "remaining_rows": remaining},
        )

        return remaining == 0
