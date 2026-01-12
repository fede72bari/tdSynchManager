"""Structured logging for data consistency and error tracking."""

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

__all__ = ["DataConsistencyLogger"]


class DataConsistencyLogger:
    """Logger for data consistency issues with persistence to Parquet files.

    Logs are saved to dedicated files following the pattern:
        {root_dir}/data/{asset}/{symbol}/{interval}/logs/
            log_{YYYY-MM-DD}_{symbol}-{asset}-{interval}_part001.parquet
            log_{YYYY-MM-DD}_{symbol}-{asset}-{interval}_part002.parquet
            ...

    File Rotation:
        - Each part file is limited to 300 rows maximum
        - When a part file reaches 300 rows, a new part is created with incremented number
        - Part numbers are zero-padded to 3 digits (001, 002, ..., 999)
        - All parts for the same date are automatically combined when querying logs

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
        """Initialize the DataConsistencyLogger with storage configuration and console output settings.

        This constructor sets up the logger's storage paths and configures whether log messages
        should be echoed to the console in addition to being persisted to Parquet files. The logger
        creates a dedicated logs directory structure under the data directory for each symbol/asset/interval
        combination, ensuring organized and queryable log storage.

        Parameters
        ----------
        root_dir : str
            Root directory for data storage. Log files will be created under
            {root_dir}/data/{asset}/{symbol}/{interval}/logs/ following the project's
            standard directory structure.
        verbose_console : bool, optional
            Default: True

            Controls whether log messages are printed to console in addition to being saved
            to Parquet files. When True, all log events are printed to stdout with formatted
            output showing severity, event type, and details. Set to False for silent operation.
        """
        self.root_dir = Path(root_dir)
        self.verbose = verbose_console
        self._log_buffer: Dict[str, list] = {}  # Buffer for batch writes

    def log_missing_data(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_range: Tuple[str, str],
        error_msg: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Record the detection of missing or incomplete data in the storage system.

        This method logs when data validation identifies missing data points, incomplete time ranges,
        or gaps in expected coverage. The log entry is persisted to Parquet with ERROR severity and
        marked as PENDING resolution status. This creates an audit trail for data quality issues and
        enables post-hoc analysis of data completeness problems.

        Parameters
        ----------
        symbol : str
            The ticker or symbol identifier for which data is missing. Examples include "AAPL"
            for Apple stock, "SPY" for SPDR S&P 500 ETF, or "ES" for E-mini S&P 500 futures.
        asset : str
            The type of financial instrument. Must be one of "stock", "option", or "index".
            This categorization helps organize logs by asset class.
        interval : str
            The time interval or granularity of the missing data. Common values include "1d"
            for daily data, "5m" for 5-minute bars, "1h" for hourly data, or "tick" for
            tick-by-tick data.
        date_range : Tuple[str, str]
            A tuple of (start_date, end_date) in ISO format (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            indicating the time range where data is missing. Both elements should be strings.
        error_msg : str
            Human-readable description of the missing data issue. Should clearly explain what
            data is missing and how it was detected (e.g., "Missing 15 trading days in range").
        details : Dict[str, Any], optional
            Default: None

            Additional contextual information to include in the log entry. This dictionary
            will be serialized to JSON and stored in the details_json column. Useful for
            storing specific missing dates, gap counts, or other diagnostic information.
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
        """Record a retry attempt for a previously failed data operation.

        This method logs when the system automatically retries a failed download or validation
        operation. It tracks the retry attempt number and the error that triggered the retry,
        creating visibility into the system's recovery efforts. Logged with WARNING severity
        and PENDING status to indicate ongoing resolution attempts.

        Parameters
        ----------
        symbol : str
            The ticker or symbol identifier being retried.
        asset : str
            The asset type ("stock", "option", "index").
        interval : str
            The time interval for the data being retried.
        date_range : Tuple[str, str]
            Date range (start, end) being retried in ISO format.
        attempt : int
            The retry attempt number. Value of 1 indicates the first retry (second overall attempt),
            2 indicates the second retry (third overall attempt), and so on. This helps track
            how many retry cycles have been exhausted.
        error_msg : str
            The error message that triggered this retry attempt. Should clearly describe what
            failed and why the retry is being attempted.
        details : Dict[str, Any], optional
            Default: None

            Additional diagnostic information about the retry, such as error types, stack traces,
            or operation-specific context. Serialized to JSON in the log entry.
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

    def log_info(
        self,
        symbol: str,
        asset: str,
        interval: str,
        date_range: Tuple[str, str],
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Record an informational event (non-error) for observability."""
        self._log_event(
            event_type="INFO",
            severity="INFO",
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=date_range,
            error_message=message,
            retry_attempt=0,
            resolution_status="PENDING",
            details=details or {}
        )

    def log_error(
        self,
        asset: str,
        symbol: str,
        interval: str,
        date: str,
        error_type: str,
        error_message: str,
        severity: str = "ERROR",
        details: Optional[Dict[str, Any]] = None
    ):
        """Record a general error event with flexible parameters.

        Parameters
        ----------
        asset : str
            Asset type ("stock", "option", "index").
        symbol : str
            Symbol identifier.
        interval : str
            Time interval.
        date : str
            ISO date or date range affected.
        error_type : str
            Type/category of error (e.g., "GREEKS_DOWNLOAD_FAILED").
        error_message : str
            Detailed error description.
        severity : str, optional
            Severity level ("ERROR", "CRITICAL"). Default: "ERROR".
        details : Dict[str, Any], optional
            Additional context as dictionary.
        """
        self._log_event(
            event_type=error_type,
            severity=severity,
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(date, date),
            error_message=error_message,
            retry_attempt=0,
            resolution_status="PENDING",
            details=details or {}
        )

    def log_warning(
        self,
        asset: str,
        symbol: str,
        interval: str,
        date: str,
        warning_type: str,
        warning_message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Record a warning event.

        Parameters
        ----------
        asset : str
            Asset type ("stock", "option", "index").
        symbol : str
            Symbol identifier.
        interval : str
            Time interval.
        date : str
            ISO date or date range affected.
        warning_type : str
            Type/category of warning (e.g., "OI_DOWNLOAD_EMPTY").
        warning_message : str
            Detailed warning description.
        details : Dict[str, Any], optional
            Additional context as dictionary.
        """
        self._log_event(
            event_type=warning_type,
            severity="WARNING",
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(date, date),
            error_message=warning_message,
            retry_attempt=0,
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
        """Record the successful resolution of a previously identified data issue.

        This method logs when a data consistency problem has been successfully resolved through
        automatic retry, recovery, or manual intervention. It marks the issue as RESOLVED with
        INFO severity, completing the audit trail for the problem. This helps track system
        effectiveness in recovering from failures and maintaining data quality.

        Parameters
        ----------
        symbol : str
            The ticker or symbol identifier for which the issue was resolved.
        asset : str
            The asset type ("stock", "option", "index").
        interval : str
            The time interval of the resolved data.
        date_range : Tuple[str, str]
            Date range (start, end) in ISO format that was successfully recovered or validated.
        message : str
            Human-readable description of the resolution. Should explain how the issue was
            resolved (e.g., "Resolved after 2 retry attempts", "Data successfully recovered").
        details : Dict[str, Any], optional
            Default: None

            Additional information about the resolution process, such as number of attempts,
            recovery method used, or data integrity verification results. Serialized to JSON.
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
        """Record a permanent failure after all retry attempts have been exhausted.

        This method logs when a data operation has failed definitively after all configured
        retry attempts. It marks the issue as FAILED with CRITICAL severity, indicating that
        manual intervention may be required. This creates a clear record of unrecoverable
        data quality issues that need attention.

        Parameters
        ----------
        symbol : str
            The ticker or symbol identifier for which the operation failed.
        asset : str
            The asset type ("stock", "option", "index").
        interval : str
            The time interval of the failed operation.
        date_range : Tuple[str, str]
            Date range (start, end) in ISO format for which the operation failed permanently.
        message : str
            Human-readable description of the failure. Should include information about how
            many retry attempts were made and the final error encountered.
        details : Dict[str, Any], optional
            Default: None

            Additional diagnostic information such as error types, total attempts made,
            or specific failure conditions. This helps in troubleshooting and determining
            whether manual intervention is needed.
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
        """Record InfluxDB-specific failures during data persistence operations.

        This method logs errors specific to InfluxDB operations such as authentication failures,
        connection issues, truncated responses, or write errors. The fatal flag determines whether
        the issue is recoverable (ERROR) or requires immediate attention and process termination
        (CRITICAL).

        Parameters
        ----------
        symbol : str
            The ticker or symbol identifier involved in the failed InfluxDB operation.
        asset : str
            The asset type ("stock", "option", "index").
        interval : str
            The time interval of the data being written to InfluxDB.
        failure_type : str
            Specific type of InfluxDB failure. Common values include "UNAUTHORIZED" for
            authentication failures, "TRUNCATED_RESPONSE" for incomplete server responses,
            "CONNECTION_ERROR" for network issues, or "WRITE_ERROR" for data persistence problems.
        message : str
            Human-readable description of the InfluxDB failure, including any error codes or
            messages returned by the InfluxDB server.
        fatal : bool, optional
            Default: False

            If True, the failure is logged as CRITICAL and marked as FAILED, indicating the
            process should terminate. If False, logged as ERROR with PENDING status, allowing
            for potential retry or recovery.
        details : Dict[str, Any], optional
            Default: None

            Additional diagnostic information such as HTTP status codes, server responses,
            connection parameters (excluding sensitive auth tokens), or retry context.
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
        """Record ThetaData API session closure events and reconnection attempts.

        This method logs when the ThetaData API session is unexpectedly closed and tracks
        automatic reconnection attempts. The fatal flag indicates whether reconnection was
        successful or if the session closure resulted in process termination.

        Parameters
        ----------
        symbol : str
            The ticker or symbol identifier being processed when the session closed. For
            system-level session closures not tied to a specific symbol, use "SYSTEM".
        asset : str
            The asset type ("stock", "option", "index", or "SYSTEM" for general failures).
        interval : str
            The time interval being processed when the session closed, or "SYSTEM" for
            general failures.
        attempt : int
            The reconnection attempt number. Value of 1 indicates the first reconnection
            attempt, 2 indicates the second, and so on. Used to track how many reconnection
            cycles were attempted before success or failure.
        fatal : bool, optional
            Default: False

            If True, the session closure was unrecoverable after all reconnection attempts,
            logged as CRITICAL with FAILED status. If False, indicates a reconnection is
            being attempted, logged as WARNING with PENDING status.
        details : Dict[str, Any], optional
            Default: None

            Additional context such as the method that triggered the session closure,
            error messages from the ThetaData client, or connection parameters.
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
            # Usa default=str per gestire tipi non serializzabili (es. numpy/int64)
            "details_json": json.dumps(details, default=str)
        }

        # Console output
        if self.verbose:
            range_str = f"{date_range[0]}..{date_range[1]}" if date_range[0] else ""
            measurement = ""
            try:
                measurement_val = details.get("measurement")
                if measurement_val:
                    measurement = f" meas={measurement_val}"
            except Exception:
                measurement = ""
            attempt_str = f" attempt={retry_attempt}" if retry_attempt else ""
            status_str = f" status={resolution_status}" if resolution_status else ""
            print(
                f"[{severity}][{event_type}] {symbol} ({asset}/{interval}) "
                f"{range_str}: {error_message}{measurement}{attempt_str}{status_str}"
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
        try:
            # Normalize interval name for filesystem (eod -> 1d to match data directories)
            interval_dir = "1d" if interval == "eod" else interval

            # Create log directory
            log_dir = self.root_dir / "data" / asset / symbol / interval_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)

            # Log filename base: log_YYYY-MM-DD_symbol-asset-interval
            log_date = log_entry["timestamp"].strftime("%Y-%m-%d")
            log_base = f"log_{log_date}_{symbol}-{asset}-{interval}"

            # Max rows per part file
            MAX_ROWS_PER_PART = 300

            # Find all existing part files for this date
            pattern = f"{log_base}_part*.parquet"
            existing_parts = sorted(log_dir.glob(pattern))

            # Determine target file
            target_part_num = 1
            target_path = None

            if existing_parts:
                # Get the last part file
                last_part_path = existing_parts[-1]

                # Extract part number from filename: log_..._part001.parquet
                last_part_name = last_part_path.stem  # removes .parquet
                if "_part" in last_part_name:
                    try:
                        last_num_str = last_part_name.split("_part")[-1]
                        last_part_num = int(last_num_str)
                    except (ValueError, IndexError):
                        last_part_num = 1
                else:
                    last_part_num = 1

                # Check if last part is full (best-effort)
                df_last = self._safe_read_parquet(last_part_path)
                rows_in_last = len(df_last) if df_last is not None else MAX_ROWS_PER_PART

                if rows_in_last < MAX_ROWS_PER_PART:
                    # Append to existing last part
                    target_part_num = last_part_num
                    target_path = last_part_path
                else:
                    # Create new part
                    target_part_num = last_part_num + 1
                    target_path = log_dir / f"{log_base}_part{target_part_num:03d}.parquet"
            else:
                # First log for this date - create part001
                target_part_num = 1
                target_path = log_dir / f"{log_base}_part{target_part_num:03d}.parquet"

            # Append to target file or create new
            df_new = pd.DataFrame([log_entry])

            df_existing = None
            if target_path.exists():
                df_existing = self._safe_read_parquet(target_path)
                if df_existing is None:
                    # Avoid overwriting a corrupt or unreadable file
                    target_part_num += 1
                    target_path = log_dir / f"{log_base}_part{target_part_num:03d}.parquet"

            if df_existing is not None:
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new

            # Atomic write to avoid truncated parquet on crash
            self._atomic_write_parquet(df_combined, target_path)
        except Exception as e:
            if self.verbose:
                print(f"[LOG][WARN] Failed to persist log for {symbol} {asset}/{interval}: {e}")

    def _safe_read_parquet(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            return pd.read_parquet(path)
        except Exception as e:
            self._quarantine_corrupt_parquet(path, e)
            return None

    def _quarantine_corrupt_parquet(self, path: Path, err: Exception) -> None:
        try:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            corrupt_path = path.with_name(f"{path.name}.corrupt.{ts}")
            os.replace(path, corrupt_path)
            if self.verbose:
                print(f"[LOG][WARN] Corrupt log parquet moved to {corrupt_path}: {err}")
        except Exception as move_err:
            if self.verbose:
                print(f"[LOG][WARN] Could not move corrupt log parquet {path}: {move_err}")

    def _atomic_write_parquet(self, df: pd.DataFrame, target_path: Path) -> None:
        tmp = target_path.with_name(f"{target_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            df.to_parquet(tmp, index=False)
            os.replace(tmp, target_path)
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

    # -------- Query/Display Helpers --------
    def get_logs(
        self,
        symbol: str,
        asset: str,
        interval: str,
        start_ts,
        end_ts=None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Return logs for the given series filtered by timestamp range.

        Automatically reads and combines all part files (e.g., _part001, _part002, ...)
        for the requested date range.
        """
        # Normalize interval name for filesystem (eod -> 1d to match data directories)
        interval_dir = "1d" if interval == "eod" else interval
        log_dir = self.root_dir / "data" / asset / symbol / interval_dir / "logs"

        # Pattern matches both legacy files and new part files
        # e.g., log_2024-12-25_AAL-option-1d_part001.parquet
        files = sorted(log_dir.glob("log_*_*.parquet")) if log_dir.exists() else []
        if not files:
            return pd.DataFrame()

        # Read and combine all part files
        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception:
                continue
        if not dfs:
            return pd.DataFrame()
        df = pd.concat(dfs, ignore_index=True)
        df = df[(df["asset"] == asset) & (df["symbol"] == symbol) & (df["interval"] == interval)]
        start_ts = pd.to_datetime(start_ts, utc=True)
        df = df[df["timestamp"] >= start_ts]
        if end_ts is not None:
            end_ts = pd.to_datetime(end_ts, utc=True)
            df = df[df["timestamp"] <= end_ts]
        df = df.sort_values("timestamp").reset_index(drop=True)
        if limit and len(df) > limit:
            df = df.tail(limit)
        return df

    def display_logs(
        self,
        symbol: str,
        asset: str,
        interval: str,
        start_ts,
        end_ts=None,
        limit: int = 50
    ):
        """Return recent logs as DataFrame (caller can print/display as desired)."""
        return self.get_logs(symbol, asset, interval, start_ts, end_ts, limit)

    def print_logs(
        self,
        symbol: str,
        asset: str,
        interval: str,
        start_ts,
        end_ts=None,
        limit: int = 50
    ) -> None:
        """Print recent logs with no truncation (wide columns)."""
        df = self.get_logs(symbol, asset, interval, start_ts, end_ts, limit)
        if df.empty:
            print(f"[LOGGER][INFO] No logs for {symbol} {asset}/{interval} from {start_ts} to {end_ts or 'now'}.")
            return
        cols = ["timestamp", "severity", "event_type", "error_message", "retry_attempt", "resolution_status",
                "date_range_start", "date_range_end"]
        cols = [c for c in cols if c in df.columns]
        with pd.option_context("display.max_colwidth", None, "display.max_rows", None, "display.width", 0):
            print(f"[LOGGER][INFO] Showing last {len(df)} entries for {symbol} {asset}/{interval} from {start_ts} to {end_ts or 'now'}:")
            # Ensure no truncation of cells or rows
            print(df[cols].to_string(index=False, max_colwidth=None))
