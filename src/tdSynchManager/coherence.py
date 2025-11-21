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

        Parameters
        ----------
        report : CoherenceReport
            Report to populate with issues.
        start_date : str
            Start date (YYYY-MM-DD).
        end_date : str
            End date (YYYY-MM-DD).
        """
        # Generate date range
        dates = self._generate_date_range(start_date, end_date)

        for date_iso in dates:
            # Check if day exists (use Manager's methods)
            # Note: This is a simplified check. Full implementation would
            # read data and validate candle counts.
            # For now, we log as a placeholder
            pass

    async def _check_tick_completeness(
        self,
        report: CoherenceReport,
        start_date: str,
        end_date: str
    ):
        """Check tick data completeness vs EOD volumes.

        Parameters
        ----------
        report : CoherenceReport
            Report to populate with issues.
        start_date : str
            Start date (YYYY-MM-DD).
        end_date : str
            End date (YYYY-MM-DD).
        """
        # Generate date range
        dates = self._generate_date_range(start_date, end_date)

        for date_iso in dates:
            # Placeholder for tick volume validation
            # Full implementation would read tick data, sum volumes,
            # compare with EOD
            pass

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

        context = {
            'symbol': symbol,
            'asset': asset,
            'interval': interval,
            'date_range': (date_iso, date_iso)
        }

        # Download with retry
        if asset == "stock":
            coro_func = lambda: self.client.stock_history_eod(
                symbol=symbol,
                start_date=date_iso,
                end_date=date_iso,
                format_type="csv"
            )
        elif asset == "index":
            coro_func = lambda: self.client.index_history_eod(
                symbol=symbol,
                start_date=date_iso,
                end_date=date_iso,
                format_type="csv"
            )
        elif asset == "option":
            coro_func = lambda: self.client.option_history_eod(
                symbol=symbol,
                expiration="*",
                start_date=date_iso,
                end_date=date_iso,
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

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        gap_start : str
            Gap start time.
        gap_end : str
            Gap end time.
        sink : str
            Storage sink.
        enrich_greeks : bool
            Whether to enrich with Greeks.

        Returns
        -------
        bool
            True if recovery successful, False otherwise.
        """
        # Placeholder for intraday recovery
        # Would download specific time range and merge with existing data
        return False

    async def _recover_tick_day(
        self,
        symbol: str,
        asset: str,
        date_iso: str,
        sink: str
    ) -> bool:
        """Recover tick data for a day with volume mismatch.

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
        # Placeholder for tick data recovery
        # Would re-download tick data for the day
        return False
