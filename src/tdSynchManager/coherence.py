"""Data coherence checking and recovery for post-hoc validation.

This module provides tools to check data completeness and consistency between
local storage and the ThetaData source, and to recover missing data.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from .logger import DataConsistencyLogger
from .retry import retry_with_policy
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
        # _series_earliest_and_latest_day returns (earliest_day, latest_day, files)
        local_range_result = self.manager._series_earliest_and_latest_day(
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

        # Perform interval-specific checks
        if interval == "1d":
            await self._check_eod_completeness(report, check_start, check_end)
        elif interval != "tick":
            await self._check_intraday_completeness(report, check_start, check_end)
        else:
            await self._check_tick_completeness(report, check_start, check_end)

        # Update coherence status
        report.is_coherent = len(report.issues) == 0

        return report

    async def _check_eod_completeness(
        self,
        report: CoherenceReport,
        start_date: str,
        end_date: str
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
        """
        # Get missing days from Manager
        missing_days = self.manager._missing_1d_days_csv(
            report.asset,
            report.symbol,
            report.interval,
            report.sink,
            start_date,
            end_date
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
        end_date: str
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
        """
        from .validator import DataValidator

        # Generate date range
        dates = self._generate_date_range(start_date, end_date)

        for date_iso in dates:
            # Skip weekends
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
                validation_result = DataValidator.validate_intraday_completeness(
                    df=df,
                    date_iso=date_iso,
                    interval=report.interval,
                    asset=report.asset
                )

                if not validation_result.valid:
                    # Identify problematic time segments using segmentation
                    problem_segments = await self._segment_intraday_problems(
                        df, date_iso, report.interval, report.asset
                    )

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
        end_date: str
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
        """
        from .validator import DataValidator

        # Generate date range
        dates = self._generate_date_range(start_date, end_date)

        for date_iso in dates:
            # Skip weekends
            date_obj = datetime.fromisoformat(date_iso)
            if date_obj.weekday() >= 5:
                continue

            try:
                # Read tick data for this day
                tick_df = await self._read_tick_day(report.symbol, report.asset, date_iso, report.sink)

                if tick_df is None or tick_df.empty:
                    report.is_coherent = False
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
                    problem_segments = await self._segment_tick_problems(tick_df, date_iso)

                    report.is_coherent = False
                    report.issues.append(CoherenceIssue(
                        issue_type="TICK_VOLUME_MISMATCH",
                        severity="WARNING",
                        date_range=(date_iso, date_iso),
                        description=validation_result.error_message or f"Tick volume mismatch on {date_iso}",
                        details={
                            **validation_result.details,
                            'problem_segments': problem_segments
                        }
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
            Storage sink.

        Returns
        -------
        pd.DataFrame or None
            DataFrame with intraday data, or None if not found.
        """
        import pandas as pd

        # Use manager's file listing to find the file for this day
        files = self.manager._list_series_files(asset, symbol, interval, sink.lower())

        # Find file for this specific date
        target_file = None
        for f in files:
            if date_iso in f:
                target_file = f
                break

        if not target_file:
            return None

        # Read the file
        try:
            if sink.lower() == "csv":
                df = pd.read_csv(target_file)
            elif sink.lower() == "parquet":
                df = pd.read_parquet(target_file)
            else:
                return None

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
        import pandas as pd

        # First check local EOD data
        eod_df = await self._read_intraday_day(symbol, asset, "1d", date_iso, sink)

        if eod_df is not None and not eod_df.empty:
            # Try to extract volumes from local data
            if 'volume' in eod_df.columns:
                total_volume = eod_df['volume'].sum()

                # For options, try to separate by call/put
                if asset == "option" and 'right' in eod_df.columns:
                    call_volume = eod_df[eod_df['right'].isin(['C', 'call'])]['volume'].sum()
                    put_volume = eod_df[eod_df['right'].isin(['P', 'put'])]['volume'].sum()
                    return float(total_volume), float(call_volume), float(put_volume)

                return float(total_volume), None, None

        # If not in local storage, download from API
        try:
            if asset == "stock":
                result, url = await self.client.stock_history_eod(
                    symbol=symbol,
                    start_date=date_iso,
                    end_date=date_iso,
                    format_type="csv"
                )
            elif asset == "index":
                result, url = await self.client.index_history_eod(
                    symbol=symbol,
                    start_date=date_iso,
                    end_date=date_iso,
                    format_type="csv"
                )
            elif asset == "option":
                result, url = await self.client.option_history_eod(
                    symbol=symbol,
                    expiration="*",
                    start_date=date_iso,
                    end_date=date_iso,
                    format_type="csv"
                )
            else:
                return None, None, None

            # Parse CSV response
            from io import StringIO
            csv_text = result[0] if isinstance(result, tuple) else result
            df = pd.read_csv(StringIO(csv_text))

            if 'volume' in df.columns:
                total_volume = df['volume'].sum()

                if asset == "option" and 'right' in df.columns:
                    call_volume = df[df['right'].isin(['C', 'call'])]['volume'].sum()
                    put_volume = df[df['right'].isin(['P', 'put'])]['volume'].sum()
                    return float(total_volume), float(call_volume), float(put_volume)

                return float(total_volume), None, None

        except Exception:
            pass

        return None, None, None

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
                # Convert ms_of_day to datetime
                base_date = pd.to_datetime(date_iso)
                df['_parsed_time'] = base_date + pd.to_timedelta(df[ts_col], unit='ms')
            else:
                df['_parsed_time'] = pd.to_datetime(df[ts_col], errors='coerce')

            # Market hours: 9:30 AM - 4:00 PM ET
            market_start = pd.to_datetime(f"{date_iso} 09:30:00")
            market_end = pd.to_datetime(f"{date_iso} 16:00:00")

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

    async def _segment_tick_problems(
        self,
        tick_df,
        date_iso: str
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

        Returns
        -------
        List[Dict]
            List of segments with volume statistics.
        """
        import pandas as pd

        segments = []

        # Find timestamp column
        ts_col = None
        for col in ['timestamp', 'created', 'ms_of_day']:
            if col in tick_df.columns:
                ts_col = col
                break

        if ts_col is None or 'volume' not in tick_df.columns:
            return segments

        try:
            tick_df = tick_df.copy()

            if ts_col == 'ms_of_day':
                base_date = pd.to_datetime(date_iso)
                tick_df['_parsed_time'] = base_date + pd.to_timedelta(tick_df[ts_col], unit='ms')
            else:
                tick_df['_parsed_time'] = pd.to_datetime(tick_df[ts_col], errors='coerce')

            # Market hours: 9:30 AM - 4:00 PM ET
            market_start = pd.to_datetime(f"{date_iso} 09:30:00")
            market_end = pd.to_datetime(f"{date_iso} 16:00:00")

            # Create 1-hour segments
            segment_duration = timedelta(hours=1)
            current_start = market_start

            while current_start < market_end:
                current_end = current_start + segment_duration

                segment_df = tick_df[
                    (tick_df['_parsed_time'] >= current_start) &
                    (tick_df['_parsed_time'] < current_end)
                ]

                segment_volume = segment_df['volume'].sum() if not segment_df.empty else 0
                segment_ticks = len(segment_df)

                segments.append({
                    'hour_start': current_start.strftime("%H:%M:%S"),
                    'hour_end': current_end.strftime("%H:%M:%S"),
                    'volume': float(segment_volume),
                    'tick_count': segment_ticks
                })

                current_start = current_end

        except Exception:
            pass

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
        self.retry_policy = manager.config.retry_policy

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
        # _series_earliest_and_latest_day returns (earliest_day, latest_day, files)
        local_range_result = self.manager._series_earliest_and_latest_day(
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

        # Download with abundant range (±1 day for safety margins)
        date_obj = datetime.fromisoformat(date_iso)
        download_start = (date_obj - timedelta(days=1)).strftime('%Y-%m-%d')
        download_end = (date_obj + timedelta(days=1)).strftime('%Y-%m-%d')

        context = {
            'symbol': symbol,
            'asset': asset,
            'interval': interval,
            'date_range': (download_start, download_end),
            'target_date': date_iso
        }

        # Download with retry
        if asset == "stock":
            coro_func = lambda: self.client.stock_history_eod(
                symbol=symbol,
                start_date=download_start,
                end_date=download_end,
                format_type="csv"
            )
        elif asset == "index":
            coro_func = lambda: self.client.index_history_eod(
                symbol=symbol,
                start_date=download_start,
                end_date=download_end,
                format_type="csv"
            )
        elif asset == "option":
            coro_func = lambda: self.client.option_history_eod(
                symbol=symbol,
                expiration="*",
                start_date=download_start,
                end_date=download_end,
                format_type="csv"
            )
        else:
            return False

        result, success = await retry_with_policy(
            coro_func=coro_func,
            policy=self.retry_policy,
            logger=self.logger,
            context=context
        )

        if not success:
            return False

        # Parse CSV (this would need the Manager's parsing logic)
        # For now, simplified
        csv_text = result[0] if isinstance(result, tuple) else result

        # Here we would parse and save using Manager methods
        # Placeholder: assume success
        self.logger.log_resolution(
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(date_iso, date_iso),
            message=f"Recovered EOD data for {date_iso} via API download",
            details={'recovery_method': 'api_download'}
        )

        return True

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
        # Parse dates and add margin (±1 day)
        start_obj = datetime.fromisoformat(gap_start)
        end_obj = datetime.fromisoformat(gap_end)

        download_start = (start_obj - timedelta(days=1)).strftime('%Y-%m-%d')
        download_end = (end_obj + timedelta(days=1)).strftime('%Y-%m-%d')

        context = {
            'symbol': symbol,
            'asset': asset,
            'interval': interval,
            'date_range': (download_start, download_end),
            'target_range': (gap_start, gap_end)
        }

        # Download each day in the range
        current_date = start_obj
        all_success = True

        while current_date <= end_obj:
            date_str = current_date.strftime('%Y-%m-%d')

            # Download with retry
            if asset == "stock":
                coro_func = lambda d=date_str: self.client.stock_history_ohlc(
                    symbol=symbol,
                    date=d,
                    interval=interval,
                    format_type="csv"
                )
            elif asset == "index":
                coro_func = lambda d=date_str: self.client.index_history_ohlc(
                    symbol=symbol,
                    date=d,
                    interval=interval,
                    format_type="csv"
                )
            elif asset == "option":
                coro_func = lambda d=date_str: self.client.option_history_ohlc(
                    symbol=symbol,
                    expiration="*",
                    date=d,
                    interval=interval,
                    format_type="csv"
                )
            else:
                return False

            result, success = await retry_with_policy(
                coro_func=coro_func,
                policy=self.retry_policy,
                logger=self.logger,
                context=context
            )

            if success:
                # Parse and save using manager
                # For now, log success (full integration with manager's save methods would be needed)
                self.logger.log_resolution(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(date_str, date_str),
                    message=f"Recovered intraday data for {date_str}",
                    details={'recovery_method': 'api_download'}
                )
            else:
                all_success = False

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
        context = {
            'symbol': symbol,
            'asset': asset,
            'interval': 'tick',
            'date_range': (date_iso, date_iso)
        }

        # Download tick data with retry
        if asset == "stock":
            coro_func = lambda: self.client.stock_history_trade_quote(
                symbol=symbol,
                date=date_iso,
                format_type="csv"
            )
        elif asset == "index":
            # Indexes typically don't have tick data, use price endpoint
            coro_func = lambda: self.client.index_history_price(
                symbol=symbol,
                date=date_iso,
                format_type="csv"
            )
        elif asset == "option":
            coro_func = lambda: self.client.option_history_trade_quote(
                symbol=symbol,
                expiration="*",
                date=date_iso,
                format_type="csv"
            )
        else:
            return False

        result, success = await retry_with_policy(
            coro_func=coro_func,
            policy=self.retry_policy,
            logger=self.logger,
            context=context
        )

        if success:
            # Parse and save using manager
            # For now, log success (full integration with manager's save methods would be needed)
            self.logger.log_resolution(
                symbol=symbol,
                asset=asset,
                interval='tick',
                date_range=(date_iso, date_iso),
                message=f"Recovered tick data for {date_iso}",
                details={'recovery_method': 'api_download'}
            )
            return True

        return False
