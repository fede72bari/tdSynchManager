"""Structured logging for data consistency and error tracking."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

__all__ = ["DataConsistencyLogger"]


class DataConsistencyLogger:
    """Logger for data consistency issues with persistence to Parquet files.

    Logs are saved to dedicated files following the pattern:
        {root_dir}/data/{asset}/{symbol}/{interval}/logs/
            log_{YYYY-MM-DD}T00-00-00Z-{symbol}-{asset}-{interval}.parquet

    Schema:
        - timestamp: datetime (UTC when error occurred)
        - event_type: str (MISSING_DATA, RETRY_ATTEMPT, RESOLUTION, etc.)
        - severity: str (INFO, WARNING, ERROR, CRITICAL)
        - symbol: str
        - asset: str
        - interval: str
        - date_range_start: str (ISO datetime)
        - date_range_end: str (ISO datetime)
        - error_message: str
        - retry_attempt: int (0 for first attempt, 1+ for retries)
        - resolution_status: str (PENDING, RESOLVED, FAILED)
        - details_json: str (JSON with additional info)
    """

    def __init__(self, root_dir: str, verbose_console: bool = True):
        """Initialize logger.

        Parameters
        ----------
        root_dir : str
            Root directory for data storage.
        verbose_console : bool, optional
            If True, also print logs to console (default True).
        """
        self.root_dir = Path(root_dir)
        self.verbose = verbose_console
        self._log_buffer: Dict[str, list] = {}  # Buffer per batch writes

    def log_missing_data(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_range: Tuple[str, str],
        error_msg: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log missing data detection.

        Parameters
        ----------
        symbol : str
            Symbol name (e.g., "AAPL").
        asset : str
            Asset type ("stock", "option", "index").
        interval : str
            Time interval (e.g., "1d", "5m", "tick").
        date_range : Tuple[str, str]
            Date range (start, end) where data is missing.
        error_msg : str
            Description of the missing data issue.
        details : Dict[str, Any], optional
            Additional details to store as JSON.
        """
        self._log_event(
            event_type="MISSING_DATA",
            severity="ERROR",
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=date_range,
            error_message=error_msg,
            retry_attempt=0,
            resolution_status="PENDING",
            details=details or {}
        )

    def log_retry_attempt(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_range: Tuple[str, str],
        attempt: int,
        error_msg: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log a retry attempt.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        date_range : Tuple[str, str]
            Date range being retried.
        attempt : int
            Retry attempt number (1 for first retry, 2 for second, etc.).
        error_msg : str
            Error that triggered the retry.
        details : Dict[str, Any], optional
            Additional details.
        """
        self._log_event(
            event_type="RETRY_ATTEMPT",
            severity="WARNING",
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=date_range,
            error_message=error_msg,
            retry_attempt=attempt,
            resolution_status="PENDING",
            details=details or {}
        )

    def log_resolution(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_range: Tuple[str, str],
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log successful resolution of a data issue.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        date_range : Tuple[str, str]
            Date range that was resolved.
        message : str
            Resolution description.
        details : Dict[str, Any], optional
            Additional details.
        """
        self._log_event(
            event_type="RESOLUTION",
            severity="INFO",
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=date_range,
            error_message=message,
            retry_attempt=0,
            resolution_status="RESOLVED",
            details=details or {}
        )

    def log_failure(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_range: Tuple[str, str],
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log permanent failure after all retries exhausted.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        date_range : Tuple[str, str]
            Date range that failed.
        message : str
            Failure description.
        details : Dict[str, Any], optional
            Additional details.
        """
        self._log_event(
            event_type="FAILURE",
            severity="CRITICAL",
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=date_range,
            error_message=message,
            retry_attempt=0,
            resolution_status="FAILED",
            details=details or {}
        )

    def log_influx_failure(
        self,
        symbol: str,
        asset: str,
        interval: str,
        failure_type: str,
        message: str,
        fatal: bool = False,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log InfluxDB-specific failures.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        failure_type : str
            Type of failure (e.g., "UNAUTHORIZED", "TRUNCATED_RESPONSE").
        message : str
            Failure description.
        fatal : bool, optional
            If True, marks as CRITICAL (default False).
        details : Dict[str, Any], optional
            Additional details.
        """
        self._log_event(
            event_type=f"INFLUX_{failure_type}",
            severity="CRITICAL" if fatal else "ERROR",
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=("", ""),
            error_message=message,
            retry_attempt=0,
            resolution_status="FAILED" if fatal else "PENDING",
            details=details or {}
        )

    def log_session_closed(
        self,
        symbol: str,
        asset: str,
        interval: str,
        attempt: int,
        fatal: bool = False,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log session closed event.

        Parameters
        ----------
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        attempt : int
            Reconnection attempt number.
        fatal : bool, optional
            If True, reconnection failed (default False).
        details : Dict[str, Any], optional
            Additional details.
        """
        self._log_event(
            event_type="SESSION_CLOSED",
            severity="CRITICAL" if fatal else "WARNING",
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=("", ""),
            error_message=f"Session closed (attempt {attempt})",
            retry_attempt=attempt,
            resolution_status="FAILED" if fatal else "PENDING",
            details=details or {}
        )

    def _log_event(
        self,
        event_type: str,
        severity: str,
        symbol: str,
        asset: str,
        interval: str,
        date_range: Tuple[str, str],
        error_message: str,
        retry_attempt: int,
        resolution_status: str,
        details: Dict[str, Any]
    ):
        """Internal method to log an event.

        Parameters
        ----------
        event_type : str
            Type of event.
        severity : str
            Severity level (INFO, WARNING, ERROR, CRITICAL).
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        date_range : Tuple[str, str]
            Date range (start, end).
        error_message : str
            Error/event message.
        retry_attempt : int
            Retry attempt number.
        resolution_status : str
            Resolution status (PENDING, RESOLVED, FAILED).
        details : Dict[str, Any]
            Additional details as dict.
        """
        timestamp = datetime.now(timezone.utc)

        log_entry = {
            "timestamp": timestamp,
            "event_type": event_type,
            "severity": severity,
            "symbol": symbol,
            "asset": asset,
            "interval": interval,
            "date_range_start": date_range[0] if date_range else "",
            "date_range_end": date_range[1] if date_range else "",
            "error_message": error_message,
            "retry_attempt": retry_attempt,
            "resolution_status": resolution_status,
            "details_json": json.dumps(details)
        }

        # Console output
        if self.verbose:
            range_str = f"{date_range[0]}..{date_range[1]}" if date_range[0] else ""
            print(
                f"[{severity}][{event_type}] {symbol} ({asset}/{interval}) "
                f"{range_str}: {error_message}"
            )

        # Persist to file
        self._persist_log(log_entry, symbol, asset, interval)

    def _persist_log(
        self,
        log_entry: Dict[str, Any],
        symbol: str,
        asset: str,
        interval: str
    ):
        """Persist log entry to Parquet file.

        Parameters
        ----------
        log_entry : Dict[str, Any]
            Log entry to persist.
        symbol : str
            Symbol name.
        asset : str
            Asset type.
        interval : str
            Time interval.
        """
        # Create log directory
        log_dir = self.root_dir / "data" / asset / symbol / interval / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # Log filename: log_YYYY-MM-DD_symbol-asset-interval.parquet
        log_date = log_entry["timestamp"].strftime("%Y-%m-%d")
        log_filename = f"log_{log_date}_{symbol}-{asset}-{interval}.parquet"
        log_path = log_dir / log_filename

        # Append to existing file or create new
        df_new = pd.DataFrame([log_entry])

        if log_path.exists():
            df_existing = pd.read_parquet(log_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new

        # Write
        df_combined.to_parquet(log_path, index=False)
