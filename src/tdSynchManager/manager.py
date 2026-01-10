from __future__ import annotations

import asyncio
import datetime
import glob
import importlib
import inspect
import io
import json
import math
import os
import re
import requests
import tempfile
import uuid
from datetime import datetime as dt, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, get_args
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import aiohttp
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from clients.ThetaDataV3Client import Interval, ThetaDataV3Client, ResilientThetaClient
from .config import DiscoverPolicy, ManagerConfig, Task, config_from_env
from .logger import DataConsistencyLogger
from .validator import DataValidator, ValidationResult
from .retry import retry_with_policy
from .download_retry import download_with_retry_and_validation
from .influx_retry import InfluxWriteRetry
# Removed influx_verification imports - InfluxDB write() is synchronous and raises exceptions on failure

try:  # optional at runtime
    ipy_display = importlib.import_module("IPython.display").display
except Exception:  # pragma: no cover
    ipy_display = None

try:  # optional dependency, only needed for InfluxDB sinks
    InfluxDBClient3 = importlib.import_module("influxdb_client_3").InfluxDBClient3
except Exception:  # pragma: no cover
    InfluxDBClient3 = None

# EOD TIMESTAMP FIX VERSION - increment on each modification group
EOD_TIMESTAMP_FIX_VERSION = "v1.0.9"  # Dropped legacy infer_datetime_format workaround (pandas 2.x)

# Shell log/version banner (increment when delivering a new patch build)
SHELL_LOG_VERSION = 3
_SHELL_VERSION_PRINTED = False


def _print_shell_version_banner():
    """Emit a single banner line so operators can verify the loaded build."""
    global _SHELL_VERSION_PRINTED
    if not _SHELL_VERSION_PRINTED:
        print(f"[VERSION] tdSynchManager shell v{SHELL_LOG_VERSION} (EOD timestamp fix {EOD_TIMESTAMP_FIX_VERSION})")
        _SHELL_VERSION_PRINTED = True

# -*- coding: utf-8 -*-
"""
ThetaSync Orchestrator (v3.4, no-downloader)

What you get
------------
- Uses your ThetaDataV3Client directly (no external downloader required).
- Fast first-date discovery with safe guards (ONE expiration per request; avoids HTTP 414).
- Binary fallback via /option/list/contracts/{trade|quote}.
- Persistent cache for first-date coverage; selective invalidation.
- Resume logic with overlap, daily file rotation (_partNN) under size cap.
- CSV and Parquet sinks. For Parquet, append by read+concat+dedupe with rotation.
- Clean HTTP error messages and robust JSON normalization.
- Greeks enrichment preserved: saved as a **separate companion file** (same day/part + `.greeks`)
  to avoid schema-merge brittleness across API variants.

Notes
-----
- Option bar download uses /option/history/ohlc per-day windows. If `enrich_bar_greeks=True`,
  we also call /option/history/greeks/all for the same filters and persist a companion file.
- If you want both OHLC+Greeks in a single table, you can post-merge on
  ['timestamp','expiration','strike','right'] (schema may differ by server setup).
"""

# ------------- user client (we assume you've imported it) -------------
# from your_module import ThetaDataV3Client, ThetaDataV3HTTPError
# The client exposes _make_request(endpoint, params) -> (payload, full_url)
# and many typed wrappers we leverage directly. :contentReference[oaicite:1]{index=1}

_ALLOWED_INTERVALS = set(get_args(Interval))


# --- Drop-in: logga gli errori del server per ogni richiesta ThetaData ---
def install_td_server_error_logger(client):
    """Wraps the ThetaData client's _make_request method to log detailed error information when HTTP requests fail.

    This utility function intercepts all requests made by the ThetaData client and logs comprehensive
    error details including status code, full URL, and response body whenever an exception occurs. The
    original exception is always re-raised after logging, ensuring normal error handling continues.

    Parameters
    ----------
    client : ThetaDataV3Client
        The ThetaData client instance whose _make_request method will be wrapped for error logging.
        The client must have a _make_request method available.

    Returns
    -------
    None
        This function modifies the client in-place by replacing its _make_request method.

    Example Usage
    -------------
    # Enable detailed error logging for ThetaData API requests
    from clients.ThetaDataV3Client import ThetaDataV3Client
    client = ThetaDataV3Client(base_url="http://localhost:25503/v3")
    install_td_server_error_logger(client)
    # Now all failed requests will log detailed error information
    """
    orig = getattr(client, "_make_request", None)
    if not callable(orig):
        print("[TD-HTTP-ERROR] _make_request non trovato sul client; impossibile installare il logger.")
        return

    async def _wrapped(endpoint: str, params: dict):
        try:
            return await orig(endpoint, params)
        except Exception as e:
            # Prova a estrarre info utili dall'eccezione
            status = getattr(e, "status", None) or getattr(e, "status_code", None) or getattr(e, "code", None)
            body   = getattr(e, "body", None) or getattr(e, "text", None) or getattr(e, "response_text", None)
            # ricostruisci l’URL completo se il client lo fornisce in uscita:
            try:
                base = getattr(client, "base_url", "http://localhost:25503/v3")
                url  = f"{base}{endpoint}?{urlencode(params or {})}"
            except Exception:
                url  = f"{endpoint}  params={params}"

            print(f"\n[TD-HTTP-ERROR] status={status} url={url}")
            if body is not None:
                if isinstance(body, (bytes, bytearray)):
                    try: body = body.decode("utf-8", "replace")
                    except Exception: body = str(body)
                # stampa max ~4000 char per non inondare la console
                print(f"[TD-HTTP-ERROR-BODY]\n{str(body)[:4000]}\n")
            else:
                print(f"[TD-HTTP-ERROR-EXC] {type(e).__name__}: {e}\n")
            raise  # Rilancia sempre
    client._make_request = _wrapped


# ---------------------------------------------------------------------
# MANAGER
# ---------------------------------------------------------------------

class ThetaSyncManager:
    """High-level orchestrator for symbol synchronization across assets and sinks.

    This class wires together: first-date discovery, robust resume logic, file size capping,
    and direct usage of your ThetaDataV3Client for HTTP I/O.

    Public API
    ----------
    The ThetaSyncManager provides a comprehensive set of public methods organized by functionality:

    **Data Download Methods**
        run(tasks)
            Main entry point for downloading and synchronizing market data. Execute a batch of
            synchronization tasks concurrently for multiple symbols and intervals.

    **Coherence & Validation Methods**
        check_and_recover_coherence(symbol, asset, interval, sink, ...)
            Check data coherence and optionally recover missing data. Performs post-hoc validation
            to identify missing data, gaps in coverage, and inconsistencies.

        generate_duplicate_report(asset, symbol, intervals, sinks, ...)
            Generate a comprehensive duplicate detection report across multiple intervals and sinks.
            Provides a holistic view of data quality across all dimensions.

        check_duplicates_multi_day(asset, symbol, interval, sink, ...)
            Check for duplicate records across multiple trading days in the specified data sink.
            Performs comprehensive duplicate detection analysis across a date range.

        check_duplicates_in_sink(asset, symbol, interval, sink, day_iso, ...)
            Check for duplicate records in a specific sink for a single day or entire dataset.
            Returns detailed statistics about duplication rates and sample duplicate keys.

        duplication_and_strike_checks(asset, symbol, interval, sink, ...)
            Perform detailed per-day auditing: duplicate detection on composite keys and
            strike×expiration accounting. Builds overlap matrices between file parts.

    **Query & Data Access Methods**
        query_local_data(asset, symbol, interval, sink, start_date, ...)
            Query and extract data from local sinks (CSV, Parquet, InfluxDB) within a date/time
            range. Returns both the data DataFrame and a list of warnings/errors encountered.

        list_available_data(asset, symbol, interval, sink)
            List all available data series in local sinks with date range information. Scans
            the local sink directories and returns a summary of all available time series.

        available_expiration_chains(symbol, interval, sink, ...)
            Get available option expiration chains for a given symbol. Returns which expiration
            dates are available in the historical data.

        available_strikes_by_expiration(symbol, interval, sink, expirations)
            Get available strike prices for each expiration chain. Returns a mapping of
            expiration → strikes from local data storage.

    **Storage & Statistics Methods**
        get_storage_stats(asset, symbol, interval, sink)
            Get storage statistics for local databases including file counts, sizes, and date
            ranges. Returns detailed statistics grouped by asset, symbol, interval, and sink.

        get_storage_summary()
            Get aggregated storage summary statistics across all databases. Returns totals
            by sink, asset, interval, and top symbols by storage size.

    **Screening & Analysis Methods**
        screen_option_oi_concentration(symbols, day_iso, threshold_pct, ...)
            Screen for abnormal open interest concentration by symbol and expiration.
            Identifies strikes/contracts with unusually high OI concentration.

        screen_option_volume_concentration(symbols, day_iso, threshold_pct, ...)
            Screen for abnormal daily volume concentration by symbol and expiration.
            Flags rows where volume concentration exceeds specified thresholds.

    **Logging & Utility Methods**
        show_logs(symbol, asset, interval, start_ts, end_ts, ...)
            Convenience wrapper to fetch and print recent logger entries for a data series.
            Useful for debugging and monitoring synchronization progress.

        clear_first_date_cache(asset, symbol, req_type)
            Delete a specific first-date cache entry from memory and persist to disk.
            Forces fresh discovery on the next sync operation.

    Example Usage
    -------------
    Initialize and run basic synchronization:

        >>> from tdSynchManager import ManagerConfig, ThetaSyncManager, Task
        >>> from clients.ThetaDataV3Client import ThetaDataV3Client
        >>>
        >>> cfg = ManagerConfig(root_dir="./data", max_concurrency=5)
        >>> async with ThetaDataV3Client() as client:
        ...     manager = ThetaSyncManager(cfg, client)
        ...     tasks = [
        ...         Task(asset="stock", symbols=["AAPL", "MSFT"], intervals=["1d"], sink="parquet"),
        ...         Task(asset="option", symbols=["SPY"], intervals=["5m"], sink="csv")
        ...     ]
        ...     await manager.run(tasks)

    Check data coherence and recover missing data:

        >>> report = await manager.check_and_recover_coherence(
        ...     symbol="AAPL",
        ...     asset="stock",
        ...     interval="1d",
        ...     sink="parquet",
        ...     start_date="2024-01-01",
        ...     auto_recover=True
        ... )
        >>> if report.is_coherent:
        ...     print("Data is complete!")

    Query local data:

        >>> df, warnings = manager.query_local_data(
        ...     asset="stock",
        ...     symbol="AAPL",
        ...     interval="1d",
        ...     sink="parquet",
        ...     start_date="2024-01-01",
        ...     end_date="2024-12-31"
        ... )

    Notes
    -----
    - All async methods (run, check_and_recover_coherence, screen_*) must be awaited
    - Date parameters typically use ISO format "YYYY-MM-DD"
    - Available assets: "stock", "option", "index"
    - Available sinks: "csv", "parquet", "influxdb"
    - See individual method docstrings for detailed parameter descriptions and examples
    """



    # =========================================================================
    # (BEGIN)
    # INITIALIZATION
    # =========================================================================
    def __init__(self, cfg: ManagerConfig, client: Any, tz_et: str = "America/New_York"):
        """Initializes the ThetaSyncManager with configuration, client, and timezone settings.

        This constructor sets up the manager's core components including concurrency control, caching mechanisms,
        timezone handling, and directory structure. It also loads any existing first-date coverage cache from disk.

        Parameters
        ----------
        cfg : ManagerConfig
            Global configuration object containing settings for concurrency, file size caps, resume behavior,
            cache directory names, and other operational parameters.
        client : ThetaDataV3Client
            An async HTTP client instance for ThetaData API communication. Must support async context manager
            protocol (async with) and provide methods for fetching market data.
        tz_et : str, optional
            Default: "America/New_York"

            The timezone string for Eastern Time, used for aligning market hours and daily boundaries.
            Must be a valid IANA timezone identifier recognized by Python's zoneinfo module.

        Returns
        -------
        None
            This is a constructor method that initializes instance attributes.

        Example Usage
        -------------
        # Initialize the sync manager with configuration and client
        from tdSynchManager import ManagerConfig, ThetaSyncManager
        from clients.ThetaDataV3Client import ThetaDataV3Client

        cfg = ManagerConfig(root_dir="./data", max_concurrency=5)
        async with ThetaDataV3Client(base_url="http://localhost:25503/v3") as client:
            manager = ThetaSyncManager(cfg, client)
        """
        _print_shell_version_banner()
        self.cfg = cfg

        # Initialize logger for data consistency tracking
        self.logger = DataConsistencyLogger(
            root_dir=cfg.root_dir,
            verbose_console=cfg.log_verbose_console
        )

        # Wrap client with resilient layer for session recovery
        self.client = ResilientThetaClient(
            base_client=client,
            logger=self.logger,
            max_reconnect_attempts=cfg.retry_policy.session_closed_max_attempts
        )

        self._sem = asyncio.Semaphore(cfg.max_concurrency)

        self._exp_cache: Dict[str, List[str]] = {}
        self._exp_by_day_cache: Dict[Tuple[str, str], List[str]] = {}

        self.ET = ZoneInfo(tz_et)
        self.UTC = timezone.utc


        # Initialize cache storage
        cache_dir = os.path.join(self.cfg.root_dir, self.cfg.cache_dir_name)
        os.makedirs(cache_dir, exist_ok=True)
        self._cache_path = os.path.join(cache_dir, self.cfg.cache_file_name)
        self._coverage_cache: Dict[str, Dict[str, str]] = self._load_cache_file()

        # Ensure sink root exists
        self._root_sink_dir = os.path.join(self.cfg.root_dir, "data")
        os.makedirs(self._root_sink_dir, exist_ok=True)

        # Initialize InfluxDB retry manager
        failed_batch_dir = os.path.join(cache_dir, "failed_influx_batches")
        self._influx_retry_mgr = InfluxWriteRetry(
            max_retries=getattr(cfg, "influx_max_retries", 3),
            base_delay=getattr(cfg, "influx_retry_delay", 5.0),
            max_delay=60.0,
            failed_batch_dir=failed_batch_dir
        )

    # -------- Log display helper --------
    def show_logs(
        self,
        symbol: str,
        asset: str,
        interval: str,
        start_ts,
        end_ts=None,
        limit: int = 50,
        print_full: bool = False
    ):
        """Fetch and display logger entries for a data series.

        This method retrieves log entries from the data consistency logger for a specific
        symbol, asset type, and interval combination. It provides options for limiting the
        number of results and controlling display formatting.

        Parameters
        ----------
        symbol : str
            The ticker symbol or root symbol to query (e.g., "AAPL", "SPY").
        asset : str
            The asset type: "stock", "option", or "index".
        interval : str
            The time interval (e.g., "tick", "1m", "5m", "1h", "1d").
        start_ts : str or datetime
            Start timestamp for log query. Can be ISO format string or datetime object.
        end_ts : str or datetime, optional
            End timestamp for log query. If None, queries up to the most recent logs.
            Can be ISO format string or datetime object.
        limit : int, optional
            Maximum number of log entries to retrieve (default: 50).
            Applied after date filtering.
        print_full : bool, optional
            Default: False
            If True, displays all rows and columns with unlimited width using IPython display
            (or print if IPython not available) and returns None.
            If False, returns a truncated DataFrame subject to pandas display options.

        Returns
        -------
        pd.DataFrame or None
            If print_full=False: Returns a pandas DataFrame with log entries.
            If print_full=True: Displays the logs and returns None.

        Example Usage
        -------------
        # Get last 50 logs for AAPL stock 1d data
        logs = manager.show_logs(
            symbol="AAPL",
            asset="stock",
            interval="1d",
            start_ts="2024-01-01"
        )

        # Display all logs with full formatting for SPY options
        manager.show_logs(
            symbol="SPY",
            asset="option",
            interval="5m",
            start_ts="2024-11-01",
            end_ts="2024-11-30",
            print_full=True
        )
        """
        df = self.logger.get_logs(
            symbol=symbol,
            asset=asset,
            interval=interval,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=limit,
        )

        if print_full:
            # Display with NO limitations
            import pandas as pd
            try:
                # Try to use IPython display if available
                from IPython.display import display
                with pd.option_context(
                    'display.max_rows', None,           # Show all rows
                    'display.max_columns', None,        # Show all columns
                    'display.max_colwidth', None,       # Show full text in columns
                    'display.width', None,              # No width limit
                    'display.max_seq_items', None       # Show all items in sequences
                ):
                    display(df)
            except ImportError:
                # Fallback to print if IPython not available
                with pd.option_context(
                    'display.max_rows', None,
                    'display.max_columns', None,
                    'display.max_colwidth', None,
                    'display.width', 0
                ):
                    print(df)
            return None

        return df




    # =========================================================================
    # (END)
    # INITIALIZATION
    # =========================================================================

    # =========================================================================
    # DATA VALIDATION (REAL-TIME)
    # =========================================================================

    async def _validate_downloaded_data(
        self,
        df: pd.DataFrame,
        asset: str,
        symbol: str,
        interval: str,
        day_iso: str,
        sink: str,
        enrich_greeks: bool = False,
        enrich_tick_greeks: bool = False
    ) -> bool:
        """Validate downloaded data before persisting to storage.

        This method performs real-time validation on DataFrames in RAM before they are saved.
        It checks for required columns, enrichment columns (if requested), data completeness,
        and volume consistency (for tick data). In strict mode, any validation failure blocks
        the save operation (all-or-nothing). In non-strict mode, warnings are logged but data
        is still saved.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to validate (in RAM).
        asset : str
            Asset type ("stock", "option", "index").
        symbol : str
            Symbol name.
        interval : str
            Time interval ("1d", "5m", "tick", etc.).
        day_iso : str
            Date being validated (YYYY-MM-DD).
        sink : str
            Storage sink ("csv", "parquet", "influx").
        enrich_greeks : bool, optional
            Whether full Greeks enrichment was requested (default False).
        enrich_tick_greeks : bool, optional
            Whether tick-level Greeks enrichment was requested (default False).

        Returns
        -------
        bool
            True if validation passes (or non-strict mode), False if strict mode and validation fails.
        """
        if not self.cfg.enable_data_validation:
            return True

        from .validator import DataValidator

        validation_passed = True
        strict_mode = self.cfg.validation_strict_mode

        # 1. Validate required columns (DataValidator handles requirements internally)
        validation_result = DataValidator.validate_required_columns(
            df=df,
            asset=asset,
            interval=interval,
            enrich_greeks=enrich_greeks
        )

        if not validation_result.valid:
            self.logger.log_missing_data(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(day_iso, day_iso),
                error_msg=validation_result.error_message or "Missing required columns",
                details=validation_result.details
            )
            validation_passed = False

            if strict_mode:
                self.logger.log_failure(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message=f"STRICT MODE: Blocking save due to missing columns",
                    details=validation_result.details
                )
                return False

        # 2. Validate data completeness (EOD or intraday)
        if interval == '1d':
            # EOD: check that we have data for the expected date
            expected_dates = [day_iso]
            validation_result = DataValidator.validate_eod_completeness(
                df=df,
                expected_dates=expected_dates
            )

            if not validation_result.valid:
                self.logger.log_missing_data(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    error_msg=validation_result.error_message or "EOD date missing",
                    details=validation_result.details
                )
                validation_passed = False

                if strict_mode:
                    self.logger.log_failure(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=(day_iso, day_iso),
                        message=f"STRICT MODE: Blocking save due to EOD incompleteness",
                        details=validation_result.details
                    )
                    return False

        elif interval != 'tick':
            expected_combo_total = None
            if asset == "option":
                expected_combo_total = await self._expected_option_combos_for_day(symbol, day_iso)
                if expected_combo_total:
                    print(f"[VALIDATION][INFO] {symbol} {interval} {day_iso}: attese_combo={expected_combo_total} (exp x strike x right)")

            # Intraday OHLC: validate candle completeness
            validation_result = DataValidator.validate_intraday_completeness(
                df=df,
                interval=interval,
                date_iso=day_iso,
                asset=asset,
                bucket_tolerance=self.cfg.intraday_bucket_tolerance,
                expected_combo_total=expected_combo_total
            )
            # Log esito anche quando passa, con expected/actual
            if validation_result.valid:
                det = validation_result.details or {}
                print(f"[VALIDATION][OK] {symbol} {interval} {day_iso} buckets={det.get('actual_buckets')} exp_buckets=~{det.get('expected')} "
                      f"combos_min={det.get('min_combos_per_bucket')} expected_combo_total={det.get('expected_combo_total')}")
                # Log INFO anche nel logger
                self.logger.log_info(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message="VALIDATION_OK",
                    details=det
                )

            if not validation_result.valid:
                self.logger.log_missing_data(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    error_msg=validation_result.error_message or "Missing candles",
                    details=validation_result.details
                )
                validation_passed = False

                if strict_mode:
                    self.logger.log_failure(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=(day_iso, day_iso),
                        message=f"STRICT MODE: Blocking save due to intraday incompleteness",
                        details=validation_result.details
                    )
                    return False

        # 3. Validate tick vs EOD volume (for tick data only)
        if interval == 'tick':
            # Try to get EOD volume for comparison
            eod_volume, eod_volume_call, eod_volume_put = await self._get_eod_volume_for_validation(
                symbol=symbol,
                asset=asset,
                date_iso=day_iso,
                sink=sink
            )

            if eod_volume is not None:
                validation_result = DataValidator.validate_tick_vs_eod_volume(
                    tick_df=df,
                    eod_volume=eod_volume,
                    date_iso=day_iso,
                    asset=asset,
                    tolerance=self.cfg.tick_eod_volume_tolerance,
                    eod_volume_call=eod_volume_call,
                    eod_volume_put=eod_volume_put
                )

                if not validation_result.valid:
                    self.logger.log_missing_data(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=(day_iso, day_iso),
                        error_msg=validation_result.error_message or "Tick volume doesn't match EOD",
                        details=validation_result.details
                    )
                    validation_passed = False

                    if strict_mode:
                        self.logger.log_failure(
                            symbol=symbol,
                            asset=asset,
                            interval=interval,
                            date_range=(day_iso, day_iso),
                            message=f"STRICT MODE: Blocking save due to tick volume mismatch",
                            details=validation_result.details
                        )
                        return False

        # If we got here and validation_passed is still True, or non-strict mode
        if validation_passed:
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(day_iso, day_iso),
                message="VALIDATION_DECISION: All checks passed - data will be saved",
                details={"strict_mode": strict_mode, "decision": "SAVE", "reason": "validation_passed"}
            )
            return True
        else:
            # Non-strict mode: warnings logged but allow save
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(day_iso, day_iso),
                message="VALIDATION_DECISION: Validation warnings - data saved with warnings (non-strict mode)",
                details={"strict_mode": strict_mode, "decision": "SAVE_WITH_WARNINGS", "reason": "non_strict_mode_allows_save"}
            )
            return True

    async def _get_eod_volume_for_validation(
        self,
        symbol: str,
        asset: str,
        date_iso: str,
        sink: str
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Get EOD volumes for tick validation.

        First checks local storage (CSV/Parquet/InfluxDB), then downloads from API if not found.
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
            Storage sink (csv, parquet, influxdb).

        Returns
        -------
        Tuple[Optional[float], Optional[float], Optional[float]]
            (total_volume, call_volume, put_volume). For non-options, call and put are None.
        """
        import io

        def _normalize_right(series: pd.Series) -> pd.Series:
            return (
                series.astype(str)
                .str.strip()
                .str.lower()
                .replace({'c': 'call', 'p': 'put'})
            )

        def _extract_eod_volumes(eod_df: pd.DataFrame) -> Tuple[Optional[float], Optional[float], Optional[float]]:
            if eod_df is None or eod_df.empty:
                return None, None, None

            if 'volume' not in eod_df.columns:
                return None, None, None

            volumes = pd.to_numeric(eod_df['volume'], errors='coerce').fillna(0)
            total_volume = float(volumes.sum())

            if asset == "option" and 'right' in eod_df.columns:
                normalized = _normalize_right(eod_df['right'])
                call_volume = float(volumes[normalized == 'call'].sum())
                put_volume = float(volumes[normalized == 'put'].sum())

                # If normalization failed (both zero) but total > 0, fall back to total-only validation
                if call_volume > 0 or put_volume > 0:
                    return total_volume, call_volume, put_volume

            return total_volume, None, None

        # First, try to read EOD from local storage
        try:
            # Handle InfluxDB sink by querying the database
            if sink.lower() == "influxdb":
                measurement = f"{symbol}-{asset}-1d"
                query = f"""
                    SELECT *
                    FROM "{measurement}"
                    WHERE time >= to_timestamp('{date_iso}T00:00:00Z')
                      AND time <= to_timestamp('{date_iso}T23:59:59Z')
                """
                try:
                    eod_df = self._influx_query_dataframe(query)
                    if not eod_df.empty:
                        total_volume, call_volume, put_volume = _extract_eod_volumes(eod_df)
                        if total_volume is not None:
                            return total_volume, call_volume, put_volume
                except Exception:
                    pass
            else:
                # CSV/Parquet file-based storage
                files = self._list_series_files(asset, symbol, "1d", sink.lower())
                eod_file = None
                for f in files:
                    if date_iso in f:
                        eod_file = f
                        break

                if eod_file:
                    if sink.lower() == "csv":
                        # Avoid pandas auto date parsing (mixed timestamp formats)
                        eod_df = pd.read_csv(eod_file, dtype=str)
                    elif sink.lower() == "parquet":
                        eod_df = pd.read_parquet(eod_file)
                    else:
                        eod_df = None

                    total_volume, call_volume, put_volume = _extract_eod_volumes(eod_df)
                    if total_volume is not None:
                        return total_volume, call_volume, put_volume
        except Exception:
            pass

        # If not in local storage, download from API
        try:
            if asset == "stock":
                csv_txt, _ = await self.client.stock_history_eod(
                    symbol=symbol,
                    start_date=date_iso,
                    end_date=date_iso,
                    format_type="csv"
                )
            elif asset == "index":
                csv_txt, _ = await self.client.index_history_eod(
                    symbol=symbol,
                    start_date=date_iso,
                    end_date=date_iso,
                    format_type="csv"
                )
            elif asset == "option":
                csv_txt, _ = await self.client.option_history_eod(
                    symbol=symbol,
                    expiration="*",
                    start_date=date_iso,
                    end_date=date_iso,
                    format_type="csv"
                )
            else:
                return None, None, None

            if not csv_txt:
                return None, None, None

            # Keep raw strings to avoid mixed-format parse warnings
            eod_df = pd.read_csv(io.StringIO(csv_txt), dtype=str)

            total_volume, call_volume, put_volume = _extract_eod_volumes(eod_df)
            if total_volume is not None:
                return total_volume, call_volume, put_volume

        except Exception:
            pass

        return None, None, None

    # =========================================================================
    # (END)
    # DATA VALIDATION (REAL-TIME)
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # DATA SYNCHRONIZATION
    # =========================================================================
    async def run(self, tasks: List[Task]) -> None:
        """Executes a batch of synchronization tasks concurrently for multiple symbols and intervals.

        This is the main entry point for the ThetaSyncManager. It orchestrates the entire synchronization process
        including first-date discovery, incremental syncing with resume logic, and cache persistence. All tasks
        are executed concurrently up to the configured max_concurrency limit.

        Parameters
        ----------
        tasks : list of Task
            A list of Task objects, where each task specifies the asset type, symbols list, intervals list,
            sink type, and optional configuration like first_date_override or discover_policy.

        Returns
        -------
        None
            This method performs synchronization and saves state to disk but does not return a value.

        Example Usage
        -------------
        # Run synchronization tasks for multiple symbols and intervals
        from tdSynchManager import Task, ThetaSyncManager, ManagerConfig
        from clients.ThetaDataV3Client import ThetaDataV3Client

        cfg = ManagerConfig(root_dir="./data", max_concurrency=5)
        async with ThetaDataV3Client() as client:
            manager = ThetaSyncManager(cfg, client)
            tasks = [
                Task(asset="stock", symbols=["AAPL", "MSFT"], intervals=["1d", "5m"], sink="parquet"),
                Task(asset="option", symbols=["SPY"], intervals=["1d"], sink="csv")
            ]
            await manager.run(tasks)
        """
        jobs = []
        for task in tasks:
            for symbol in task.symbols:
                first_date = await self._resolve_first_date(task, symbol)
                for interval in task.intervals:
                    jobs.append(self._spawn_sync(task, symbol, interval, first_date))

        await asyncio.gather(*jobs)
        self._save_cache_file()


    async def _spawn_sync(self, task: Task, symbol: str, interval: str, first_date: Optional[str]) -> None:
        """Wraps a single symbol sync job with semaphore-based concurrency control.

        This method ensures that the number of concurrent synchronization jobs does not exceed the
        configured max_concurrency limit by acquiring a semaphore before executing the sync.

        Parameters
        ----------
        task : Task
            The task configuration containing asset type, sink, and other settings.
        symbol : str
            The ticker symbol or root symbol to synchronize.
        interval : str
            The bar interval to sync (e.g., '1d', '5m', '1h').
        first_date : str or None
            The first available date for this symbol, or None if not yet determined.

        Returns
        -------
        None
            This method coordinates synchronization but does not return a value.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - run() to spawn each symbol+interval sync job with concurrency control
        """
        async with self._sem:
            await self._sync_symbol(task, symbol, interval, first_date)
            
        
    async def _fetch_available_dates_from_api(
        self,
        asset: str,
        symbol: str,
        interval: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        use_api_discovery: bool = True
    ) -> Optional[set]:
        """Query ThetaData API for available trading dates for a symbol.

        Returns set of ISO date strings ('YYYY-MM-DD') or None if API call fails.
        Automatically filters by date range if start_date/end_date provided.

        Parameters
        ----------
        use_api_discovery : bool
            If False, skip API querying and return None (fallback to day-by-day).
            Useful for fast updates where you don't want to download full historical date list.
        """
        # Early exit if API discovery is disabled
        if not use_api_discovery:
            print(f"[API-DATES] API date discovery disabled for {symbol} - using fallback")
            return None

        print(f"[API-DATES][ENTER] asset={asset} symbol={symbol} interval={interval} start={start_date} end={end_date}")

        try:
            api_dates = None

            if asset == "stock":
                # Don't use asyncio.wait_for to avoid deadlock with ResilientThetaClient wrapper
                api_response, _ = await self.client.stock_list_dates(symbol, data_type="trade", format_type="json")
                api_dates = api_response.get("date", api_response) if isinstance(api_response, dict) else api_response

            elif asset == "index":
                # Don't use asyncio.wait_for to avoid deadlock with ResilientThetaClient wrapper
                api_response, _ = await self.client.index_list_dates(symbol, data_type="ohlc", format_type="json")
                api_dates = api_response.get("date", api_response) if isinstance(api_response, dict) else api_response

            elif asset == "option":
                # OPTIMIZED 2-step process for options:
                # 1. Get all expirations
                # 2. Find ONE expiration >= end_date (or closest if none >= end_date)
                # 3. Get dates for ONLY that expiration (trading dates are same for all expirations)
                print(f"[API-DATES][OPTION] Step 1: Fetching expirations for {symbol}...")
                print(f"[API-DATES][OPTION] DEBUG: About to call option_list_expirations({symbol}, format_type='json')")

                # Build URL for logging
                exp_url = f"http://localhost:25503/v3/option/list/expirations?symbol={symbol}&format=json"
                print(f"[API-DATES][OPTION] DEBUG: URL will be: {exp_url}")

                try:
                    print(f"[API-DATES][OPTION] DEBUG: CALLING await client.option_list_expirations() NOW for {symbol}...")
                    # Don't use asyncio.wait_for to avoid deadlock with ResilientThetaClient wrapper
                    exp_response, exp_url_returned = await self.client.option_list_expirations(symbol, format_type="json")
                    print(f"[API-DATES][OPTION] DEBUG: RETURNED from option_list_expirations for {symbol}")
                    print(f"[API-DATES][OPTION] DEBUG: URL returned: {exp_url_returned}")
                    print(f"[API-DATES][OPTION] DEBUG: Response type={type(exp_response)}")
                except Exception as e:
                    print(f"[API-DATES][OPTION] ERROR: option_list_expirations failed for {symbol}: {e}")
                    import traceback
                    traceback.print_exc()
                    return None

                expirations = exp_response.get("expiration", exp_response) if isinstance(exp_response, dict) else exp_response
                print(f"[API-DATES][OPTION] DEBUG: Extracted {len(expirations) if expirations else 0} expirations")

                if not expirations:
                    print(f"[API-DATES][OPTION] No expirations found for {symbol}")
                    return None

                # ITERATIVE BACKWARD COVERAGE: Query multiple expirations to cover full historical range
                # Convert expirations to dates for comparison
                exp_dates = []
                for exp_str in expirations:
                    try:
                        exp_date = dt.fromisoformat(exp_str).date()
                        exp_dates.append((exp_str, exp_date))
                    except:
                        continue

                if not exp_dates:
                    print(f"[API-DATES][OPTION] No valid expiration dates for {symbol}")
                    return None

                # Sort by expiration date
                exp_dates.sort(key=lambda x: x[1])

                # Step 2: Iteratively query expirations backward until we cover start_date
                all_api_dates = set()  # Use set to auto-deduplicate
                current_end = end_date if end_date else exp_dates[-1][1]
                iteration = 0
                max_iterations = 100  # Safety limit to prevent infinite loops (increased from 10)

                print(f"[API-DATES][OPTION] Starting iterative backward coverage from {current_end} to {start_date}")

                while iteration < max_iterations:
                    iteration += 1

                    # Find expiration >= current_end (or closest if none >= current_end)
                    target_expiration = None
                    for exp_str, exp_date in exp_dates:
                        if exp_date >= current_end:
                            target_expiration = exp_str
                            break

                    # If no expiration >= current_end, use the most recent (last)
                    if not target_expiration:
                        target_expiration = exp_dates[-1][0]

                    print(f"[API-DATES][OPTION] Iteration {iteration}: Fetching dates for expiration {target_expiration}...")

                    try:
                        # Don't use asyncio.wait_for to avoid deadlock with ResilientThetaClient wrapper
                        dates_response, dates_url_returned = await self.client.option_list_dates(
                            symbol=symbol,
                            request_type="quote",
                            expiration=target_expiration,
                            format_type="json"
                        )

                        dates = dates_response.get("date", dates_response) if isinstance(dates_response, dict) else dates_response
                        if not dates:
                            print(f"[API-DATES][OPTION] No dates found for expiration {target_expiration}")
                            break

                        # Convert to date objects and add to set
                        iteration_dates = []
                        for date_str in dates:
                            try:
                                date_obj = dt.fromisoformat(date_str).date()
                                all_api_dates.add(date_obj.isoformat())
                                iteration_dates.append(date_obj)
                            except:
                                continue

                        if not iteration_dates:
                            print(f"[API-DATES][OPTION] No valid dates from expiration {target_expiration}")
                            break

                        # Find earliest date in this batch
                        first_date = min(iteration_dates)
                        print(f"[API-DATES][OPTION] Collected {len(iteration_dates)} dates, earliest={first_date}, total_unique={len(all_api_dates)}")

                        # Check if we've covered the start_date
                        if not start_date or first_date <= start_date:
                            print(f"[API-DATES][OPTION] Coverage complete: first_date={first_date} <= start_date={start_date}")
                            break

                        # Move backward to cover the gap
                        current_end = first_date
                        print(f"[API-DATES][OPTION] Gap remaining, moving backward to {current_end}")

                    except Exception as e:
                        print(f"[API-DATES][OPTION] Error querying expiration {target_expiration}: {e}")
                        import traceback
                        traceback.print_exc()
                        break

                if iteration >= max_iterations:
                    print(f"[API-DATES][OPTION] WARNING: Reached max iterations ({max_iterations}), may not have full coverage")

                # Convert set to list for api_dates
                api_dates = list(all_api_dates)
                print(f"[API-DATES][OPTION] Final: Collected {len(api_dates)} unique dates across {iteration} expiration(s)")

                # Debug: Show all discovered dates and coverage
                if api_dates:
                    sorted_dates = sorted(api_dates)
                    print(f"[API-DATES][DEBUG] Discovered dates - First 10: {sorted_dates[:10]}")
                    print(f"[API-DATES][DEBUG] Discovered dates - Last 10: {sorted_dates[-10:]}")
                    print(f"[API-DATES][DEBUG] Date range: {sorted_dates[0]} to {sorted_dates[-1]}")

                    # Show coverage gaps (weekends/holidays are normal, show gaps > 5 days)
                    from datetime import timedelta
                    gaps = []
                    for i in range(len(sorted_dates) - 1):
                        current = dt.fromisoformat(sorted_dates[i]).date()
                        next_date = dt.fromisoformat(sorted_dates[i+1]).date()
                        gap_days = (next_date - current).days - 1
                        if gap_days > 5:  # Only show gaps > 5 days (weekends are normal)
                            gaps.append(f"{current} to {next_date} ({gap_days} days)")
                    if gaps:
                        print(f"[API-DATES][DEBUG] Large gaps found (>5 days): {len(gaps)} gaps")
                        for gap in gaps[:10]:  # Show first 10 gaps
                            print(f"[API-DATES][DEBUG]   Gap: {gap}")
                        if len(gaps) > 10:
                            print(f"[API-DATES][DEBUG]   ... and {len(gaps) - 10} more gaps")

                    # Show expected vs actual coverage
                    if start_date and end_date:
                        expected_first = start_date.isoformat()
                        expected_last = end_date.isoformat()
                        actual_first = sorted_dates[0]
                        actual_last = sorted_dates[-1]

                        if actual_first > expected_first:
                            print(f"[API-DATES][DEBUG] WARNING: Missing coverage at start - Expected: {expected_first}, Got: {actual_first}")
                        if actual_last < expected_last:
                            print(f"[API-DATES][DEBUG] WARNING: Missing coverage at end - Expected: {expected_last}, Got: {actual_last}")

                        if actual_first <= expected_first and actual_last >= expected_last:
                            print(f"[API-DATES][DEBUG] Full coverage achieved: {actual_first} to {actual_last}")

            if not api_dates:
                return None

            # Filter by date range if specified
            print(f"[API-DATES][FILTER] Filtering {len(api_dates)} dates by range start={start_date} end={end_date}")
            if start_date or end_date:
                valid_dates = set()
                for date_str in api_dates:
                    try:
                        date_obj = dt.fromisoformat(date_str).date()
                        if (not start_date or date_obj >= start_date) and (not end_date or date_obj <= end_date):
                            valid_dates.add(date_obj.isoformat())
                    except:
                        continue
                print(f"[API-DATES][EXIT] Returning {len(valid_dates)} filtered dates for {symbol}")
                return valid_dates if valid_dates else None

            print(f"[API-DATES][EXIT] Returning {len(api_dates)} unfiltered dates for {symbol}")
            return set(api_dates) if api_dates else None

        except Exception as e:
            print(f"[API-DATES][ERROR] Failed to fetch available dates for {symbol} ({asset}): {e}")
            import traceback
            traceback.print_exc()
            return None

    async def _sync_symbol(self, task: Task, symbol: str, interval: str, first_date: Optional[str]) -> None:
        """Incrementally synchronizes a single symbol's time-series data with intelligent resume logic.

        This is the core synchronization method that orchestrates the entire data download process including:
        pre-history filling (retrograde dates), gap backfilling for missing days, and efficient resume from
        the last saved point with configurable overlap. It handles both end-of-day (EOD) batch downloads
        and day-by-day processing for intraday and options data.

        Parameters
        ----------
        task : Task
            The task configuration specifying asset type, sink, discover policy, and other settings.
        symbol : str
            The ticker symbol or root symbol to synchronize.
        interval : str
            The bar interval (e.g., '1d', '5m', '1h', 'tick').
        first_date : str or None
            The first available date for this symbol (discovered or from cache), or None to auto-discover.

        Returns
        -------
        None
            This method performs synchronization and writes data to the configured sink.

        Example Usage
        -------------
        # This is an internal method called by:
        # - _spawn_sync() after acquiring semaphore for concurrency control
        # Coordinates all phases of data synchronization for one symbol+interval combination
        """

        self._validate_interval(interval)
        self._validate_sink(task.sink)
        self._fast_resume_skip_middle = (
            getattr(task, "discover_policy", None) is not None
            and getattr(task.discover_policy, "mode", None) == "skip"
        )

        # Compute resume window suggested by existing logic
        start_dt = await self._compute_resume_start_datetime(task, symbol, interval, first_date)

        # Use end_date_override if provided, otherwise use current time
        if hasattr(task, 'end_date_override') and task.end_date_override:
            end_date_str = self._normalize_date_str(task.end_date_override)
            end_dt = dt.fromisoformat(end_date_str).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
            print(f"[END-DATE-OVERRIDE] Using end_date={end_date_str} instead of today")
        else:
            end_dt = dt.now(timezone.utc)

        # >>> START_DATE_ET — BEGIN
        # Preferisci la data esplicita dell'utente per il retrogrado; altrimenti deriva da start_dt.
        first_date_pref = first_date or getattr(task, "first_date_override", None)
        if first_date_pref:
            # 'YYYY-MM-DD' oppure 'YYYYMMDD' → ET-date
            _fd_norm = self._normalize_date_str(str(first_date_pref))
            start_date_et = dt.fromisoformat(_fd_norm).date()
        else:
            # da start_dt (UTC) converti a ET date
            start_date_et = self._as_utc(start_dt).astimezone(self.ET).date()
        # <<< START_DATE_ET — END
    
        sink_lower = task.sink.lower()
    
        # Normalize first_date to ISO YYYY-MM-DD for comparisons
        def _norm_ymd(s: Optional[str]) -> Optional[str]:
            if not s:
                return None
            return s if "-" in s else f"{s[:4]}-{s[4:6]}-{s[6:]}"

        first_date_iso = _norm_ymd(first_date)
    
        # Discover earliest and latest days across ALL series files (handles rotations / multiple files)
        series_first_iso, series_last_iso = self._get_first_last_day_from_sink(
            task.asset, symbol, interval, sink_lower
        )

        # Compute an effective resume start day:
        resume_start_day = start_dt.date()

        if interval == "1d" and task.asset in ("stock", "index") and series_last_iso:
            try:
                series_last_day = dt.fromisoformat(series_last_iso).date()
                ov = max(0, int(getattr(self.cfg, "eod_resume_overlap_days", 1)))
                # es: last=10-07, ov=1  -> resume=10-07 ; ov=0 -> 10-08
                resume_from = series_last_day - timedelta(days=ov - 1) if ov >= 1 else series_last_day + timedelta(days=1)
                resume_start_day = max(resume_start_day, resume_from)
            except Exception:
                pass

        # ---------------------------
        # Main download loop
        # ---------------------------

        # >>> RESUME-START OVERRIDE (first_date_override) – BEGIN
        try:
            if 'start_date_et' in locals() and start_date_et is not None:
                if start_date_et < resume_start_day:
                    print(f"[RESUME][OVERRIDE] forcing resume_start_day={start_date_et} (was {resume_start_day}) due to first_date/override")
                    resume_start_day = start_date_et
        except Exception as _e:
            print(f"[RESUME][OVERRIDE][WARN] {type(_e).__name__}: {_e}")
        # <<< RESUME-START OVERRIDE – END

        # --- DEFINIZIONE POLICY (deve stare QUI, prima di usarle) ---
        _first_day_db = None
        _last_day_db = None
        _retro_needed = False
        _edge_jump_after_retro = False  
        
        # mild_skip = controlla ogni giorno intermedio
        _mild_skip_policy = bool(
            getattr(task, "discover_policy", None) 
            and getattr(task.discover_policy, "mode", None) == "mild_skip"
        )
        
        # skip = SALTA TUTTO IL BLOCCO INTERMEDIO senza controllare
        _strong_skip_policy = bool(
            getattr(task, "discover_policy", None) 
            and getattr(task.discover_policy, "mode", None) == "skip"
        )
        
        if _mild_skip_policy or _strong_skip_policy:
            _first_day_db, _last_day_db = self._get_first_last_day_from_sink(
                task.asset, symbol, interval, sink_lower
            )
            if _mild_skip_policy:
                print(f"[MILD-SKIP][PRE-LOOP] asset={task.asset} sink={sink_lower} "
                      f"first_db={_first_day_db} last_db={_last_day_db}")
            else:
                print(f"[STRONG-SKIP][PRE-LOOP] asset={task.asset} sink={sink_lower} "
                      f"first_db={_first_day_db} last_db={_last_day_db}")

        # Determina lo start effettivo del loop
        if _strong_skip_policy and _first_day_db and _last_day_db:
            # Con skip: 
            # - Se first_date_override < first_day_db -> fa retrograde fill (gestito dopo)
            # - Altrimenti salta direttamente a last_day_db (ultimo giorno presente)
            user_start = None
            if first_date:
                try:
                    user_start = dt.fromisoformat(self._normalize_date_str(first_date)).date()
                except:
                    pass
            
            first_db_date = dt.fromisoformat(_first_day_db).date()
            last_db_date = dt.fromisoformat(_last_day_db).date()
            
            if user_start and user_start < first_db_date:
                # Retrograde: parte da user_start (verrà gestito dal retrograde fill)
                loop_start_date = user_start
                _edge_jump_after_retro = True 
                print(f"[STRONG-SKIP] retrograde mode: start={loop_start_date} (user < first_db={first_db_date})")
            else:
                # Normal skip: per 1d salta a last_day_db+1, per intraday a last_day_db
                if interval == "1d":
                    loop_start_date = last_db_date + timedelta(days=1)
                    print(f"[STRONG-SKIP] jump to last_db+1 (daily): start={loop_start_date} (skip {first_db_date}..{last_db_date})")
                else:
                    loop_start_date = last_db_date
                    print(f"[STRONG-SKIP] jump to last_db (intraday): start={loop_start_date} (may have incomplete data)")
        else:
            loop_start_date = resume_start_day
        
        cur_date = loop_start_date
        end_date = end_dt.date()

        # EOD batched for stock/index
        if interval == "1d" and task.asset in ("stock", "index"):
            batch_days = getattr(self.cfg, "eod_batch_days", 30)
            if not isinstance(batch_days, int) or batch_days <= 0:
                batch_days = 30

            # STEP 1: RETROGRADE (per skip e mild_skip)
            # Se la data di partenza è prima del primo dato presente, scarica in batch fino a first_db-1
            if (_strong_skip_policy or _mild_skip_policy) and _first_day_db and start_date_et < dt.fromisoformat(_first_day_db).date():
                retro_end = dt.fromisoformat(_first_day_db).date() - timedelta(days=1)
                print(f"[EOD-BATCH][RETRO] Retrograde update: {cur_date.isoformat()}..{retro_end.isoformat()}")

                while cur_date <= retro_end:
                    start_iso = cur_date.isoformat()
                    chunk_end = min(retro_end, cur_date + timedelta(days=batch_days - 1))
                    end_iso = chunk_end.isoformat()

                    try:
                        print(f"[EOD-BATCH][RETRO] {task.asset} {symbol} {start_iso}..{end_iso}")
                        await self._download_and_store_equity_or_index(
                            asset=task.asset,
                            symbol=symbol,
                            interval=interval,
                            day_iso=start_iso,
                            sink=task.sink,
                            range_end_iso=end_iso,
                        )
                    except Exception as e:
                        print(f"[WARN] {task.asset} {symbol} {interval} {start_iso}..{end_iso}: {e}")

                    cur_date = chunk_end + timedelta(days=1)

            # STEP 2: SKIP MODE - salta direttamente all'edge destro
            if _strong_skip_policy and _last_day_db:
                # Per 1d salta a last_db+1 (giorno completo)
                cur_date = dt.fromisoformat(_last_day_db).date() + timedelta(days=1)
                print(f"[SKIP-MODE][EOD-BATCH] jump to last_db+1: {cur_date}")

            # STEP 3: MILD-SKIP MODE - controlla giorni mancanti e scarica solo quelli
            elif _mild_skip_policy and _first_day_db and _last_day_db:
                # Leggi le date effettivamente presenti nei file batch
                existing_dates = self._get_dates_in_eod_batch_files(task.asset, symbol, interval, sink_lower)

                # Trova giorni mancanti tra first_db e last_db
                check_start = dt.fromisoformat(_first_day_db).date()
                check_end = dt.fromisoformat(_last_day_db).date()

                # STRATEGIA: Prima prova a ottenere le date di trading valide dall'API
                # Se l'API fornisce le date, usa quelle (evita weekend/festivi)
                # Se l'API fallisce, controlla ogni giorno (metodo vecchio)
                print(f"[MILD-SKIP][API] Fetching valid trading dates for {symbol} (asset={task.asset})...")
                valid_trading_dates = await self._fetch_available_dates_from_api(
                    task.asset, symbol, interval, check_start, check_end,
                    use_api_discovery=task.use_api_date_discovery
                )

                if valid_trading_dates:
                    print(f"[MILD-SKIP][API] Found {len(valid_trading_dates)} valid trading dates in range {check_start}..{check_end}")
                    if len(valid_trading_dates) > 0:
                        sorted_dates = sorted(valid_trading_dates)
                        print(f"[MILD-SKIP][API-DEBUG] First 3 dates: {sorted_dates[:3] if len(sorted_dates) >= 3 else sorted_dates}")
                else:
                    print(f"[MILD-SKIP][API] Falling back to day-by-day checking")

                # Trova date mancanti
                missing_dates = []

                if valid_trading_dates:
                    # METODO PREFERITO: Usa le date valide dall'API
                    print(f"[MILD-SKIP] Using API-provided valid trading dates")
                    for date_iso in sorted(valid_trading_dates):
                        if date_iso not in existing_dates:
                            missing_dates.append(dt.fromisoformat(date_iso).date())
                else:
                    # FALLBACK: Controlla ogni giorno (include weekend/festivi)
                    print(f"[MILD-SKIP] Using day-by-day checking (may include weekends/holidays)")
                    check_date = check_start
                    while check_date <= check_end:
                        if check_date.isoformat() not in existing_dates:
                            missing_dates.append(check_date)
                        check_date += timedelta(days=1)

                if missing_dates:
                    print(f"[MILD-SKIP][EOD-BATCH] Found {len(missing_dates)} missing dates between {_first_day_db}..{_last_day_db}")

                    # Crea batch per date mancanti adiacenti
                    batch_start = None
                    for i, missing_date in enumerate(missing_dates):
                        if batch_start is None:
                            batch_start = missing_date

                        # Se è l'ultima data o la prossima non è adiacente, scarica il batch
                        is_last = (i == len(missing_dates) - 1)
                        next_not_adjacent = (not is_last) and (missing_dates[i+1] - missing_date).days > 1

                        if is_last or next_not_adjacent:
                            start_iso = batch_start.isoformat()
                            end_iso = missing_date.isoformat()

                            try:
                                print(f"[EOD-BATCH][MILD-SKIP] {task.asset} {symbol} {start_iso}..{end_iso}")
                                await self._download_and_store_equity_or_index(
                                    asset=task.asset,
                                    symbol=symbol,
                                    interval=interval,
                                    day_iso=start_iso,
                                    sink=task.sink,
                                    range_end_iso=end_iso,
                                )
                            except Exception as e:
                                print(f"[WARN] {task.asset} {symbol} {interval} {start_iso}..{end_iso}: {e}")

                            batch_start = None
                else:
                    print(f"[MILD-SKIP][EOD-BATCH] No missing dates between {_first_day_db}..{_last_day_db}")

                # Dopo mild_skip, salta a last_db+1
                cur_date = dt.fromisoformat(_last_day_db).date() + timedelta(days=1)
                print(f"[MILD-SKIP][EOD-BATCH] Jump to last_db+1: {cur_date}")

            # STEP 4: Continua in batch da cur_date a end_date
            while cur_date <= end_date:
                start_iso = cur_date.isoformat()
                chunk_end = min(end_date, cur_date + timedelta(days=batch_days - 1))
                end_iso = chunk_end.isoformat()

                try:
                    print(f"[EOD-BATCH] {task.asset} {symbol} {start_iso}..{end_iso}")
                    await self._download_and_store_equity_or_index(
                        asset=task.asset,
                        symbol=symbol,
                        interval=interval,
                        day_iso=start_iso,
                        sink=task.sink,
                        range_end_iso=end_iso,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[WARN] {task.asset} {symbol} {interval} {start_iso}..{end_iso}: {e}")

                cur_date = chunk_end + timedelta(days=1)

            return  # done for EOD stock/index

        # Day-by-day loop: options and intraday
        # OPTIMIZATION: Skip API date fetch for single-day downloads (real-time intraday)
        # The API call is expensive and unnecessary when downloading only one specific day
        is_single_day = (cur_date == end_date)

        if is_single_day:
            # Single-day download (real-time intraday): skip API fetch, use the specific date
            print(f"[API-DATES] Single-day download ({cur_date.isoformat()}), skipping API date fetch")
            api_available_dates = None
        else:
            # Multi-day range: fetch available dates from ThetaData API to iterate ONLY over real dates
            print(f"[API-DATES] Fetching available dates for {symbol} ({task.asset}/{interval})...")
            api_available_dates = await self._fetch_available_dates_from_api(
                task.asset, symbol, interval, cur_date, end_date,
                use_api_discovery=task.use_api_date_discovery
            )

        # Determine iteration strategy: API dates or fallback to day-by-day
        if api_available_dates:
            print(f"[API-DATES] Found {len(api_available_dates)} available dates, iterating only those")
            # ITERATE ONLY OVER AVAILABLE DATES (no weekends/holidays)
            dates_to_process = sorted(api_available_dates)
        else:
            print(f"[API-DATES] API query failed, using fallback day-by-day iteration")
            # Fallback: generate all days in range (including weekends for filtering)
            dates_to_process = []
            temp_date = cur_date
            while temp_date <= end_date:
                dates_to_process.append(temp_date.isoformat())
                temp_date += timedelta(days=1)

        # MILD-SKIP: reduce work to only missing days (keep edge policies intact)
        if _mild_skip_policy and _first_day_db and _last_day_db:
            existing_days = set()
            if sink_lower == "influxdb":
                prefix = (self.cfg.influx_measure_prefix or "")
                if task.asset == "option":
                    meas = f"{prefix}{symbol}-option-{interval}"
                else:
                    meas = f"{prefix}{symbol}-{task.asset}-{interval}"
                existing_days = self._influx_available_dates_get_all(meas)
            elif sink_lower in ("csv", "parquet"):
                files = self._list_series_files(task.asset, symbol, interval, sink_lower)
                for fpath in files:
                    fname = os.path.basename(fpath)
                    day_part = fname.split("T", 1)[0]
                    if len(day_part) == 10:
                        existing_days.add(day_part)

            if existing_days:
                if api_available_dates:
                    expected_days = set(api_available_dates)
                else:
                    expected_days = set(dates_to_process)

                missing_days = expected_days - existing_days
                edge_days = set()
                if _first_day_db:
                    edge_days.add(_first_day_db)
                if _last_day_db:
                    edge_days.add(_last_day_db)
                edge_days &= expected_days

                dates_to_process = sorted(missing_days.union(edge_days))
                print(f"[MILD-SKIP] missing_days={len(missing_days)} edges_kept={len(edge_days)}")

        # Main iteration loop over dates
        for day_iso in dates_to_process:
            # Skip weekends ONLY if using fallback (API dates already exclude them)
            if api_available_dates is None and dt.fromisoformat(day_iso).weekday() >= 5:
                continue

            # >>> SALTO POST-RETRO (UNIVERSALE) - skip dates before last_db
            if _edge_jump_after_retro and _first_day_db and day_iso >= _first_day_db:
                if _last_day_db and day_iso < _last_day_db:
                    # Skip all dates until we reach last_db
                    print(f"[SKIP-MODE][JUMP] Skip {day_iso} (< last_db={_last_day_db})")
                    continue
                elif _last_day_db and day_iso >= _last_day_db:
                    # Reached last_db, stop skipping
                    _edge_jump_after_retro = False

            # >>> CHECK COMPLETEZZA PRIMO GIORNO (UNIVERSALE) <
            # Only skip first day if ignore_existing=True (otherwise we want to resume)
            if task.ignore_existing and _first_day_db and day_iso == _first_day_db:
                _day_complete = False

                if sink_lower == "influxdb":
                    prefix = (self.cfg.influx_measure_prefix or "")
                    if task.asset == "option":
                        meas = f"{prefix}{symbol}-option-{interval}"
                    else:
                        meas = f"{prefix}{symbol}-{task.asset}-{interval}"
                    _day_complete = self._influx_day_has_any(meas, day_iso)

                elif sink_lower in ("csv", "parquet"):
                    st = self._day_parts_status(task.asset, symbol, interval, sink_lower, day_iso)
                    _day_complete = (
                        (not st.get("missing"))
                        and (not st.get("has_mixed"))
                        and (1 in st.get("parts", []))
                    )

                if _day_complete:
                    print(f"[SKIP-MODE] skip first_day (complete) day={day_iso}")
                    continue

            # >>> EARLY-SKIP MIDDLE-DAY (UNIVERSALE) <
            if self._skip_existing_middle_day(
                asset=task.asset, symbol=symbol, interval=interval, sink=task.sink, day_iso=day_iso,
                first_last_hint=(_first_day_db, _last_day_db)
            ):
                print(f"[SKIP-MODE] skip middle day={day_iso}")
                continue


            # calcola sempre sink_lower nel loop (può non essere definito qui)
            sink_lower = (task.sink or "").lower()

            # >>> MILD-SKIP: salta last_day se interval=1d (giorno completo)
            if _mild_skip_policy and _last_day_db and day_iso == _last_day_db:
                if interval == "1d":
                    print(f"[MILD-SKIP] skip last_day (1d complete): {day_iso}")
                    continue
            
            try:
                if task.asset == "option":
                    await self._download_and_store_options(
                        symbol=symbol,
                        interval=interval,
                        day_iso=day_iso,
                        sink=task.sink,
                        enrich_greeks=task.enrich_bar_greeks,
                        enrich_tick_greeks=getattr(task, "enrich_tick_greeks", False),
                    )
                elif task.asset in ("stock", "index"):
                    await self._download_and_store_equity_or_index(
                        asset=task.asset,
                        symbol=symbol,
                        interval=interval,
                        day_iso=day_iso,
                        sink=task.sink,
                    )
                else:
                    raise ValueError(f"Unsupported asset: {task.asset}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[WARN] {task.asset} {symbol} {interval} {day_iso}: {e}")
                # CRITICAL: Log to persistent storage (not just console)
                self.logger.log_failure(
                    symbol=symbol,
                    asset=task.asset,
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message=f"Download and store failed: {str(e)}",
                    details={'error': str(e), 'error_type': type(e).__name__}
                )

    # --------------------- DATA DOWNLOAD & STORE ---------------------


    async def _download_and_store_options(
        self,
        symbol: str,
        interval: str,
        day_iso: str,
        sink: str,
        enrich_greeks: bool,
        enrich_tick_greeks: bool = False,
    ) -> None:
        """
        Download and persist **option** data for a single trading day, handling both
        end-of-day (EOD) and intraday intervals. The function is idempotent at the
        output-file level and performs lightweight enrichment (IV and prior-day OI)
        by default, with optional full Greeks merge.

        Parameters
        ----------
        symbol : str
            Underlying ticker root (e.g., "AAPL").
        interval : str
            Bar interval. Use "1d" for daily EOD; any other supported interval
            (e.g., "1m", "5m", "30m") triggers the intraday branch.
        day_iso : str
            Trading date in ISO format "YYYY-MM-DD" (UTC). Weekend dates are
            skipped automatically in the intraday branch.
        sink : str
            Output format: "csv", "parquet", or "influxdb".
        enrich_greeks : bool
            If True, fetch and merge the **full** Greeks set in addition to IV:
            - EOD: /option/history/greeks/eod over all expirations.
            - Intraday: /option/history/greeks/all aligned to the requested interval.
        enrich_tick_greeks : bool, optional
            If True, enrich tick data with trade-level Greeks granularity (default False).

        Behavior
        --------
        EOD (interval == "1d")
        • Idempotency & pathing: writes a single daily file at
          {root}/data/option/{symbol}/1d/{YYYY-MM-DD}T00-00-00Z-{symbol}-option-1d.{csv|parquet}.
          If the file already exists and _force_rewrite is False, the function returns early.
        • Data fetch:
          1) OHLC for **all expirations** in one call:
             /option/history/eod (expiration="*", start_date=end_date=day_iso).
          2) (optional) Full Greeks merge:
             /option/history/greeks/eod over the same date range.
          3) Implied Volatility: use 'implied_vol' + 'iv_error' from /option/history/greeks/eod (EOD snapshot).
          4) Prior-day open interest (last_day_OI) merge:
             /option/history/open_interest for the **previous business day** (weekend-safe).
        • Joins: use robust contract keys, preferring "option_symbol" and, when available,
          "timestamp"; fall back to {root|symbol, expiration, strike, right}. Duplicate
          columns from the right side are dropped before merging.
        • Write: atomic CSV write via .tmp + replace, or direct Parquet write.

        Intraday (interval != "1d")
        • Weekend guard: returns early for Saturday/Sunday dates.
        • Expiration discovery: finds expirations that traded that day via
          /option/list/contracts/{trade|quote}.
        • Per-expiration fetch and merge:
          1) OHLC bars: /option/history/ohlc for the given interval.
          2) (optional) Full Greeks: /option/history/greeks/all merged on bar keys.
          3) IV enrichment (always on): /option/history/greeks/implied_volatility
             for the same interval, merged on {option_symbol,timestamp} (or equivalent keys).
        • Chain-level enrichment: after concatenating all expirations, merges prior-day
          open interest as a constant column `last_day_OI` using /option/history/open_interest
          for the previous business day.
        • Output path:
          {root}/data/option/{symbol}/{interval}/{YYYY-MM-DD}T00-00-00Z-{symbol}-option-{interval}.{csv|parquet}.
          If the file already exists and _force_rewrite is False, the function returns early.

        Output
        ------
        None
            The function has side effects only: it writes exactly one file per (symbol, interval, day).
            IV columns (e.g., bid/mid/ask if provided by the API) and `last_day_OI` are included in
            the same table for timeframes shorter than EOD. Optional full Greeks are merged when `enrich_greeks=True`.

        Notes
        -----
        • Endpoint summary:
          EOD:     /option/history/eod, /option/history/greeks/eod,                 /option/history/open_interest (previous day).
          Intraday:/option/list/contracts/{trade|quote}, /option/history/ohlc,
                   /option/history/greeks/all, /option/history/greeks/implied_volatility (interval),
                   /option/history/open_interest (previous day).
        • Enrichment failures are retried; missing OI/Greeks/IV rows are dropped unless skip-day is enabled.
        """

        # Log start of download (console + parquet with ALL parameters)
        print(f"[DOWNLOAD-START] {symbol} option/{interval} day={day_iso} sink={sink} enrich_greeks={enrich_greeks}")
        self.logger.log_info(
            symbol=symbol,
            asset="option",
            interval=interval,
            date_range=(day_iso, day_iso),
            message=f"DOWNLOAD_START: sink={sink} enrich_greeks={enrich_greeks}",
            details={
                "sink": sink,
                "enrich_greeks": enrich_greeks,
                "enrich_tick_greeks": enrich_tick_greeks,
                "validation_enabled": self.cfg.enable_data_validation,
                "validation_strict_mode": self.cfg.validation_strict_mode,
                "tick_eod_volume_tolerance": self.cfg.tick_eod_volume_tolerance,
                "intraday_bucket_tolerance": self.cfg.intraday_bucket_tolerance,
                "max_concurrency": self.cfg.max_concurrency,
                "max_file_mb": self.cfg.max_file_mb,
                "overlap_seconds": self.cfg.overlap_seconds
            }
        )

        sink_lower = sink.lower()
        stop_before_part = None
        skip_day_on_missing = bool(getattr(self.cfg, "skip_day_on_greeks_iv_oi_failure", False))
        retry_passes = int(getattr(self.cfg, "greeks_iv_oi_failure_retry_passes", 0) or 0)
        retry_passes = max(0, retry_passes)
        retry_delay = float(getattr(self.cfg, "greeks_iv_oi_failure_delay_seconds", 0) or 0.0)
        total_passes = 1 + retry_passes
        greeks_version = getattr(self.cfg, "greeks_version", None)
        if greeks_version is not None:
            greeks_version = str(greeks_version).strip() or None

        # -------------------------------
        # EOD (daily) — one shot over all expirations
        # -------------------------------
        if interval == "1d":
            # Early skip per idempotenza "per-file"
            ext = "csv" if sink_lower == "csv" else "parquet"
            out_dir = os.path.join(self.cfg.root_dir, "data", "option", symbol, interval, sink_lower)
            if sink_lower != "influxdb":
                os.makedirs(out_dir, exist_ok=True)
            out_file = os.path.join(out_dir, f"{day_iso}T00-00-00Z-{symbol}-option-{interval}.{ext}")
            if os.path.exists(out_file) and not getattr(self, "_force_rewrite", False):
                print(f"[DOWNLOAD-SKIP] {symbol} option/{interval} day={day_iso} - File already exists")
                self.logger.log_info(
                    symbol=symbol,
                    asset="option",
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message="DOWNLOAD_SKIP: File already exists",
                    details={"reason": "file_exists", "file_path": out_file}
                )
                return

            # ===== DOWNLOAD WITH RETRY AND VALIDATION =====
            # Define download+enrich function
            async def download_and_enrich():
                # 1) EOD OHLC for ALL expirations in one call
                csv_txt, url = await self.client.option_history_eod(
                    symbol=symbol,
                    expiration="*",
                    start_date=day_iso,
                    end_date=day_iso,
                    format_type="csv",
                )
                if not csv_txt:
                    raise ValueError("Empty response from option_history_eod")
                df = pd.read_csv(io.StringIO(csv_txt), dtype=str)
                if df is None or df.empty:
                    raise ValueError("Empty DataFrame from option_history_eod")

                df_base = df

                def _missing_mask_for_cols(df_in, cols):
                    if df_in is None or df_in.empty or not cols:
                        return None
                    mask = None
                    for col in cols:
                        if col not in df_in.columns:
                            return pd.Series([True] * len(df_in), index=df_in.index)
                        s = df_in[col]
                        missing = s.isna()
                        if s.dtype == object:
                            missing |= s.astype(str).str.strip().eq("")
                        mask = missing if mask is None else (mask | missing)
                    return mask

                def _normalize_join_cols(df_in):
                    if df_in is None or df_in.empty:
                        return df_in
                    # Normalize expiration format for robust joins
                    if "expiration" not in df_in.columns:
                        for alt in ("expiration_date", "expirationDate", "exp", "exp_date"):
                            if alt in df_in.columns:
                                df_in = df_in.rename(columns={alt: "expiration"})
                                break
                    if "expiration" in df_in.columns:
                        exp_raw = df_in["expiration"].astype(str).str.strip()
                        exp_raw = exp_raw.map(self._iso_date_only)
                        df_in["expiration"] = exp_raw.map(lambda x: self._normalize_date_str(x) or x)
                    # Normalize strike format (e.g., 30 vs 30.0)
                    if "strike" in df_in.columns:
                        strike_raw = df_in["strike"].astype(str).str.strip()
                        strike_num = pd.to_numeric(strike_raw, errors="coerce")
                        df_in["strike"] = [
                            (f"{n:.10f}".rstrip("0").rstrip(".") if pd.notna(n) else r)
                            for r, n in zip(strike_raw, strike_num)
                        ]
                    if "right" in df_in.columns:
                        df_in["right"] = (
                            df_in["right"].astype(str).str.strip().str.lower()
                            .replace({"c": "call", "p": "put"})
                        )
                    if "symbol" not in df_in.columns and "root" in df_in.columns:
                        df_in["symbol"] = df_in["root"]
                    if "root" not in df_in.columns and "symbol" in df_in.columns:
                        df_in["root"] = df_in["symbol"]
                    return df_in

                df = df_base
                missing_mask = None
                missing_details = {}

                for enrich_pass in range(total_passes):
                    df_pass = df_base.copy()
                    df_pass = _normalize_join_cols(df_pass)
                    greeks_missing_mask = None
                    iv_missing_mask = None
                    oi_missing_mask = None
                    greeks_details = {}
                    iv_details = {}
                    oi_details = {}

                    if enrich_greeks:
                        csv_g, _ = await self._td_get_with_retry(
                            lambda: self.client.option_history_greeks_eod(
                                symbol=symbol,
                                expiration="*",
                                start_date=day_iso,
                                end_date=day_iso,
                                rate_type="sofr",
                                greeks_version=greeks_version,
                                format_type="csv",
                            ),
                            label=f"greeks_eod {symbol} {day_iso}"
                        )
                        if not csv_g:
                            greeks_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                            greeks_details = {"reason": "greeks_empty_response"}
                        else:
                            try:
                                dg = pd.read_csv(io.StringIO(csv_g), dtype=str)
                            except Exception as e:
                                dg = None
                                greeks_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                                greeks_details = {"reason": "greeks_csv_parse_error", "error": str(e)}

                            if dg is None or dg.empty:
                                if greeks_missing_mask is None:
                                    greeks_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                                if not greeks_details:
                                    greeks_details = {"reason": "greeks_csv_empty"}
                            else:
                                if "strike_price" in dg.columns and "strike" not in dg.columns:
                                    dg = dg.rename(columns={"strike_price": "strike"})
                                if "strike_price" in df_pass.columns and "strike" not in df_pass.columns:
                                    df_pass = df_pass.rename(columns={"strike_price": "strike"})

                                dg = _normalize_join_cols(dg)
                                df_pass = _normalize_join_cols(df_pass)

                                # --- >>> DEBUG: Print dataframes before merge — BEGIN
                                print(f"[DEBUG-GREEKS] {symbol} {day_iso}")
                                print(f"  df_pass shape: {df_pass.shape}")
                                print(f"  df_pass columns: {df_pass.columns.tolist()}")
                                print(f"  dg shape: {dg.shape}")
                                print(f"  dg columns: {dg.columns.tolist()}")
                                # --- >>> DEBUG — END

                                candidate_keys = [
                                    ["option_symbol"],
                                    ["root", "expiration", "strike", "right"],
                                    ["symbol", "expiration", "strike", "right"],
                                ]
                                on_cols = next((keys for keys in candidate_keys
                                                if all(k in df_pass.columns for k in keys) and all(k in dg.columns for k in keys)), None)

                                # --- >>> DEBUG: Print merge keys — BEGIN
                                print(f"[DEBUG-GREEKS] Merge keys: {on_cols}")
                                if on_cols:
                                    print(f"  df_pass unique keys: {df_pass[on_cols].drop_duplicates().shape[0]}")
                                    print(f"  dg unique keys: {dg[on_cols].drop_duplicates().shape[0]}")
                                    # Show sample keys from each
                                    print(f"  df_pass sample keys (first 3):")
                                    for idx, row in df_pass[on_cols].head(3).iterrows():
                                        print(f"    {dict(row)}")
                                    print(f"  dg sample keys (first 3):")
                                    for idx, row in dg[on_cols].head(3).iterrows():
                                        print(f"    {dict(row)}")
                                # --- >>> DEBUG — END

                                if on_cols is not None:
                                    dup_cols = [c for c in dg.columns if c in df_pass.columns and c not in on_cols]
                                    dg = dg.drop(columns=dup_cols).drop_duplicates(subset=on_cols)

                                    # --- >>> DEBUG: Merge operation — BEGIN
                                    before_rows = len(df_pass)
                                    # --- >>> DEBUG — END

                                    df_pass = df_pass.merge(dg, on=on_cols, how="left")

                                    # --- >>> DEBUG: After merge stats — BEGIN
                                    after_rows = len(df_pass)
                                    print(f"[DEBUG-GREEKS] Merge result:")
                                    print(f"  Rows before: {before_rows}, after: {after_rows}")
                                    print(f"  df_pass columns after merge: {df_pass.columns.tolist()}")
                                    # --- >>> DEBUG — END

                                    iv_candidates = ["implied_vol", "implied_volatility", "iv"]
                                    iv_col = next((c for c in iv_candidates if c in df_pass.columns), None)
                                    if iv_col and iv_col != "implied_vol":
                                        df_pass = df_pass.rename(columns={iv_col: "implied_vol"})
                                    iv_error_candidates = ["iv_error", "iv_err"]
                                    iv_error_col = next((c for c in iv_error_candidates if c in df_pass.columns), None)
                                    if iv_error_col and iv_error_col != "iv_error":
                                        df_pass = df_pass.rename(columns={iv_error_col: "iv_error"})

                                    # EOD greeks endpoint provides IV; validate Greeks separately.
                                    req_cols = ["delta", "gamma", "theta", "vega", "rho"]
                                    greeks_missing_mask = _missing_mask_for_cols(df_pass, req_cols)

                                    # --- >>> DEBUG: Show which greeks columns are missing — BEGIN
                                    if greeks_missing_mask is not None and greeks_missing_mask.any():
                                        print(f"[DEBUG-GREEKS] Missing greeks values detected:")
                                        print(f"  Total rows: {len(df_pass)}")
                                        print(f"  Rows with missing greeks: {greeks_missing_mask.sum()}")
                                        for col in req_cols:
                                            if col in df_pass.columns:
                                                missing_count = df_pass[col].isna().sum()
                                                print(f"    {col}: {missing_count} missing")
                                            else:
                                                print(f"    {col}: COLUMN NOT FOUND")
                                        # Show sample of missing rows
                                        print(f"  Sample of rows with missing greeks (first 5):")
                                        missing_sample = df_pass[greeks_missing_mask].head(5)
                                        for idx, row in missing_sample.iterrows():
                                            if on_cols:
                                                key_vals = {k: row.get(k) for k in on_cols}
                                                print(f"    {key_vals}")
                                    # --- >>> DEBUG — END

                                    if greeks_missing_mask is not None and greeks_missing_mask.any():
                                        greeks_details = {
                                            "reason": "greeks_missing_values",
                                            "missing_rows": int(greeks_missing_mask.sum())
                                        }
                                else:
                                    greeks_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                                    greeks_details = {"reason": "greeks_merge_keys_missing"}

                    # IV required when greeks requested (EOD)
                    if enrich_greeks:
                        iv_required_cols = ["implied_vol", "iv_error"]
                        missing_cols = [c for c in iv_required_cols if c not in df_pass.columns]
                        if missing_cols:
                            iv_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                            iv_details = {"reason": "iv_columns_missing", "missing_columns": missing_cols}
                        else:
                            iv_missing_mask = _missing_mask_for_cols(df_pass, iv_required_cols)
                            if iv_missing_mask is not None and iv_missing_mask.any():
                                iv_details = {
                                    "reason": "iv_missing_values",
                                    "missing_rows": int(iv_missing_mask.sum())
                                }

                    try:
                        d = dt.fromisoformat(day_iso)
                        prev = d - timedelta(days=1)
                        if prev.weekday() == 5:
                            prev = prev - timedelta(days=1)
                        elif prev.weekday() == 6:
                            prev = prev - timedelta(days=2)

                        cur_ymd = dt.fromisoformat(day_iso).strftime("%Y%m%d")
                        csv_oi, _ = await self._td_get_with_retry(
                            lambda: self.client.option_history_open_interest(
                                symbol=symbol,
                                expiration="*",
                                date=cur_ymd,
                                strike="*",
                                right="both",
                                format_type="csv",
                            ),
                            label=f"oi_eod {symbol} {cur_ymd}"
                        )
                        if not csv_oi:
                            oi_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                            oi_details = {"reason": "oi_empty_response"}
                        else:
                            try:
                                doi = pd.read_csv(io.StringIO(csv_oi), dtype=str)
                            except Exception as e:
                                doi = None
                                oi_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                                oi_details = {"reason": "oi_csv_parse_error", "error": str(e)}

                            if doi is None or doi.empty:
                                if oi_missing_mask is None:
                                    oi_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                                if not oi_details:
                                    oi_details = {"reason": "oi_csv_empty"}
                            else:
                                oi_col = next((c for c in ["open_interest", "oi", "OI"] if c in doi.columns), None)
                                if not oi_col:
                                    oi_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                                    oi_details = {"reason": "oi_column_missing"}
                                else:
                                    doi = doi.rename(columns={oi_col: "last_day_OI"})

                                    ts_col = next((c for c in ["timestamp", "ts", "time"] if c in doi.columns), None)
                                    if ts_col:
                                        doi = doi.rename(columns={ts_col: "timestamp_oi"})
                                    if "timestamp_oi" in doi.columns:
                                        _eff = pd.to_datetime(doi["timestamp_oi"], errors="coerce")
                                        doi["effective_date_oi"] = _eff.dt.tz_localize("UTC").dt.tz_convert("America/New_York").dt.date.astype(str)
                                    else:
                                        doi["effective_date_oi"] = prev.date().isoformat()

                                doi = _normalize_join_cols(doi)
                                df_pass = _normalize_join_cols(df_pass)

                                # --- >>> DEBUG: Print dataframes before OI merge — BEGIN
                                print(f"[DEBUG-OI] {symbol} {day_iso}")
                                print(f"  df_pass shape: {df_pass.shape}")
                                print(f"  df_pass columns: {df_pass.columns.tolist()}")
                                print(f"  doi shape: {doi.shape}")
                                print(f"  doi columns: {doi.columns.tolist()}")
                                # --- >>> DEBUG — END

                                candidate_keys = [
                                    ["option_symbol"],
                                    ["root", "expiration", "strike", "right"],
                                    ["symbol", "expiration", "strike", "right"],
                                ]
                                on_cols = next((keys for keys in candidate_keys
                                                if all(k in df_pass.columns for k in keys) and all(k in doi.columns for k in keys)), None)

                                # --- >>> DEBUG: Print OI merge keys — BEGIN
                                print(f"[DEBUG-OI] Merge keys: {on_cols}")
                                if on_cols:
                                    print(f"  df_pass unique keys: {df_pass[on_cols].drop_duplicates().shape[0]}")
                                    print(f"  doi unique keys: {doi[on_cols].drop_duplicates().shape[0]}")
                                    # Show sample keys from each
                                    print(f"  df_pass sample keys (first 3):")
                                    for idx, row in df_pass[on_cols].head(3).iterrows():
                                        print(f"    {dict(row)}")
                                    print(f"  doi sample keys (first 3):")
                                    for idx, row in doi[on_cols].head(3).iterrows():
                                        print(f"    {dict(row)}")
                                # --- >>> DEBUG — END

                                if on_cols is not None:
                                    keep = on_cols + ["last_day_OI"]
                                    if "timestamp_oi" in doi.columns:
                                        keep += ["timestamp_oi"]
                                    keep += ["effective_date_oi"]
                                    doi = doi[keep].drop_duplicates(subset=on_cols)

                                    # --- >>> DEBUG: OI Merge operation — BEGIN
                                    before_rows = len(df_pass)
                                    # --- >>> DEBUG — END

                                    df_pass = df_pass.merge(doi, on=on_cols, how="left")

                                    # --- >>> DEBUG: After OI merge stats — BEGIN
                                    after_rows = len(df_pass)
                                    print(f"[DEBUG-OI] Merge result:")
                                    print(f"  Rows before: {before_rows}, after: {after_rows}")
                                    print(f"  df_pass columns after merge: {df_pass.columns.tolist()}")
                                    # --- >>> DEBUG — END

                                    oi_missing_mask = _missing_mask_for_cols(df_pass, ["last_day_OI"])
                                    if oi_missing_mask is not None and oi_missing_mask.any():
                                        oi_details = {
                                            "reason": "oi_missing_values",
                                            "missing_rows": int(oi_missing_mask.sum())
                                        }
                                else:
                                    oi_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                                    oi_details = {"reason": "oi_merge_keys_missing"}
                    except Exception as e:
                        oi_missing_mask = pd.Series([True] * len(df_pass), index=df_pass.index)
                        oi_details = {"reason": "oi_exception", "error": str(e)}

                    missing_mask = None
                    missing_details = {"retry_pass": enrich_pass + 1, "retry_total": total_passes}

                    components = []
                    if greeks_missing_mask is not None and greeks_missing_mask.any():
                        missing_mask = greeks_missing_mask if missing_mask is None else (missing_mask | greeks_missing_mask)
                        missing_details["greeks_missing_rows"] = int(greeks_missing_mask.sum())
                        if greeks_details:
                            missing_details["greeks_details"] = greeks_details
                        components.append("greeks")

                        # --- >>> DEBUG: Show which greeks columns are missing — BEGIN
                        print(f"[DEBUG-MISSING] GREEKS missing detected:")
                        print(f"  Total rows with missing greeks: {greeks_missing_mask.sum()}")
                        req_cols = ["delta", "gamma", "theta", "vega", "rho"]
                        for col in req_cols:
                            if col in df_pass.columns:
                                missing_count = df_pass[col].isna().sum()
                                print(f"    {col}: {missing_count} missing ({missing_count/len(df_pass)*100:.1f}%)")
                        # --- >>> DEBUG — END

                    if iv_missing_mask is not None and iv_missing_mask.any():
                        missing_mask = iv_missing_mask if missing_mask is None else (missing_mask | iv_missing_mask)
                        missing_details["iv_missing_rows"] = int(iv_missing_mask.sum())
                        if iv_details:
                            missing_details["iv_details"] = iv_details
                        components.append("iv")

                        # --- >>> DEBUG: Show which IV columns are missing — BEGIN
                        print(f"[DEBUG-MISSING] IV missing detected:")
                        print(f"  Total rows with missing IV: {iv_missing_mask.sum()}")
                        iv_cols = ["implied_vol", "iv_error"]
                        for col in iv_cols:
                            if col in df_pass.columns:
                                missing_count = df_pass[col].isna().sum()
                                print(f"    {col}: {missing_count} missing ({missing_count/len(df_pass)*100:.1f}%)")
                        # --- >>> DEBUG — END

                    if oi_missing_mask is not None and oi_missing_mask.any():
                        missing_mask = oi_missing_mask if missing_mask is None else (missing_mask | oi_missing_mask)
                        missing_details["oi_missing_rows"] = int(oi_missing_mask.sum())
                        if oi_details:
                            missing_details["oi_details"] = oi_details
                        components.append("oi")

                        # --- >>> DEBUG: Show which OI values are missing — BEGIN
                        print(f"[DEBUG-MISSING] OI missing detected:")
                        print(f"  Total rows with missing OI: {oi_missing_mask.sum()}")
                        if "last_day_OI" in df_pass.columns:
                            missing_count = df_pass["last_day_OI"].isna().sum()
                            print(f"    last_day_OI: {missing_count} missing ({missing_count/len(df_pass)*100:.1f}%)")
                            # Show sample of contracts with missing OI
                            print(f"  Sample contracts with missing OI (first 5):")
                            missing_oi_sample = df_pass[oi_missing_mask].head(5)
                            on_cols = ['root', 'expiration', 'strike', 'right']
                            for idx, row in missing_oi_sample.iterrows():
                                key_vals = {k: row.get(k) for k in on_cols if k in row}
                                print(f"    {key_vals}")
                        # --- >>> DEBUG — END

                    if components:
                        missing_details["missing_components"] = components

                    # --- >>> DEBUG: Final combined missing mask summary — BEGIN
                    if missing_mask is not None and missing_mask.any():
                        print(f"[DEBUG-MISSING] COMBINED missing mask summary:")
                        print(f"  Total rows: {len(df_pass)}")
                        print(f"  Rows with ANY missing data: {missing_mask.sum()} ({missing_mask.sum()/len(df_pass)*100:.1f}%)")
                        print(f"  Components missing: {components}")
                    # --- >>> DEBUG — END

                    if missing_mask is None or not missing_mask.any():
                        df = df_pass
                        break

                    df = df_pass
                    if enrich_pass < total_passes - 1 and retry_delay > 0:
                        print(f"[EOD-RETRY] {symbol} {day_iso} pass={enrich_pass+1}/{total_passes} sleeping {retry_delay}s")
                        await asyncio.sleep(retry_delay)

                if missing_mask is not None and missing_mask.any():
                    if "expiration" in df.columns:
                        missing_exps = df.loc[missing_mask, "expiration"].astype(str).dropna().unique().tolist()
                        if missing_exps:
                            missing_details["missing_expirations"] = missing_exps[:25]
                    if self.logger:
                        self.logger.log_error(
                            asset="option",
                            symbol=symbol,
                            interval=interval,
                            date=day_iso,
                            error_type="ENRICHMENT_MISSING_EOD",
                            error_message="Missing OI/Greeks/IV rows after merge",
                            severity="WARNING",
                            details=missing_details
                        )
                    if skip_day_on_missing:
                        error_msg = f"[SKIP-DAY] option EOD {symbol} {day_iso}: missing OI/Greeks/IV rows"
                        print(error_msg)
                        if self.logger:
                            self.logger.log_error(
                                asset="option",
                                symbol=symbol,
                                interval=interval,
                                date=day_iso,
                                error_type="EOD_DAY_ABORTED_ON_MISSING_ENRICHMENT",
                                error_message=error_msg,
                                severity="ERROR",
                                details=missing_details
                            )
                        raise ValueError("EOD missing OI/Greeks/IV rows")

                    df = df.loc[~missing_mask].copy()
                    if df.empty:
                        error_msg = f"[SKIP-DAY] option EOD {symbol} {day_iso}: all rows dropped due to missing OI/Greeks/IV"
                        print(error_msg)
                        if self.logger:
                            self.logger.log_error(
                                asset="option",
                                symbol=symbol,
                                interval=interval,
                                date=day_iso,
                                error_type="EOD_EMPTY_AFTER_ENRICHMENT_DROP",
                                error_message=error_msg,
                                severity="ERROR",
                                details=missing_details
                            )
                        raise ValueError("EOD empty after enrichment drop")

                # 3) Normalize dtypes and dedupe
                if "expiration" in df.columns:
                    df["expiration"] = pd.to_datetime(df["expiration"], errors="coerce").dt.date
                if "strike" in df.columns:
                    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
                if "right" in df.columns:
                    df["right"] = (
                        df["right"].astype(str).str.strip().str.lower()
                        .map({"c":"call","p":"put","call":"call","put":"put"})
                        .fillna(df["right"])
                    )

                _dedupe_keys = ["option_symbol"] if "option_symbol" in df.columns else ["symbol","expiration","strike","right"]
                df = df.drop_duplicates(subset=_dedupe_keys, keep="last")

                return (df, url)

            # Define parse function (identity since download_func already unpacks tuple)
            def parse_result(result):
                return result  # result is already the DataFrame (download_retry unpacks the tuple)

            # Define validation wrapper
            async def validate_result(df):
                return await self._validate_downloaded_data(
                    df=df,
                    asset="option",
                    symbol=symbol,
                    interval=interval,
                    day_iso=day_iso,
                    sink=sink,
                    enrich_greeks=enrich_greeks,
                    enrich_tick_greeks=False
                )

            # Use retry wrapper
            df, validation_ok = await download_with_retry_and_validation(
                download_func=download_and_enrich,
                parse_func=parse_result,
                validate_func=validate_result,
                retry_policy=self.cfg.retry_policy,
                logger=self.logger,
                context={
                    'symbol': symbol,
                    'asset': 'option',
                    'interval': interval,
                    'date_range': (day_iso, day_iso),
                    'sink': sink
                }
            )

            if not validation_ok or df is None:
                # All retry attempts failed or validation failed
                print(f"[VALIDATION] STRICT MODE: Skipping save for option {symbol} EOD {day_iso} - all retry attempts failed")
                return
            # ===== /DOWNLOAD WITH RETRY AND VALIDATION =====

            # Normalizza timestamp a UTC per tutte le colonne temporali
            df = self._normalize_ts_to_utc(df)


            # 3) persist + written rows counting
            st = self._day_parts_status("option", symbol, interval, sink_lower, day_iso)
            base_path = st.get("base_path") or self._make_file_basepath("option", symbol, interval, f"{day_iso}T00-00-00Z", sink_lower)

            wrote = 0
            if sink_lower == "csv":
                df_out = self._format_dt_columns_isoz(df)
                await self._append_csv_text(base_path, df_out.to_csv(index=False), asset="option", interval=interval)
                wrote = len(df_out)
            elif sink_lower == "parquet":
                wrote = self._append_parquet_df(base_path, df, asset="option", interval=interval)
            elif sink_lower == "influxdb":
                first_et = df["timestamp"].min() if "timestamp" in df.columns else None
                last_et  = df["timestamp"].max() if "timestamp" in df.columns else None
                print(f"[INFLUX][ABOUT-TO-WRITE] rows={len(df)} window_ET=({first_et},{last_et})")

                # InfluxDB write with verification and retry
                measurement = self._influx_measurement_from_base(base_path)
                influx_client = self._ensure_influx_client()

                # Define key columns for verification
                key_cols = ['__ts_utc']
                if 'symbol' in df.columns:
                    key_cols.append('symbol')
                if 'expiration' in df.columns:
                    key_cols.append('expiration')
                if 'strike' in df.columns:
                    key_cols.append('strike')
                if 'right' in df.columns:
                    key_cols.append('right')

                df_influx = self._ensure_ts_utc_column(df)

                # Log SAVE_START for InfluxDB write
                self.logger.log_info(
                    symbol=symbol,
                    asset="option",
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message=f"SAVE_START: Writing to InfluxDB, rows={len(df_influx)}",
                    details={"sink": "influxdb", "rows": len(df_influx), "branch": "EOD", "measurement": measurement}
                )

                # InfluxDB write is synchronous - if no exception, write succeeded
                try:
                    wrote = await self._append_influx_df(base_path, df_influx)
                    write_success = True
                except Exception as e:
                    wrote = 0
                    write_success = False
                    print(f"[ALERT] InfluxDB write failed for option {symbol} EOD {day_iso}: {e}")
                    self.logger.log_failure(
                        symbol=symbol,
                        asset="option",
                        interval=interval,
                        date_range=(day_iso, day_iso),
                        message=f"SAVE_FAILURE: InfluxDB write failed: {str(e)}",
                        details={"sink": "influxdb", "rows_attempted": len(df), "error": str(e), "branch": "EOD"}
                    )
                if wrote == 0 and len(df) > 0:
                    print("[ALERT] Influx ha scritto 0 punti a fronte di righe in input. "
                          "Potrebbe essere tutto NaN lato fields o un cutoff troppo aggressivo.")
                    write_success = False
            else:
                raise ValueError(f"Unsupported sink: {sink_lower}")

            print(f"[SUMMARY] option {symbol} 1d day={day_iso} rows={len(df)} wrote={wrote} sink={sink_lower}")

            # Check if write actually succeeded
            if sink_lower == "influxdb" and wrote == 0:
                print(f"[DOWNLOAD-FAILED] {symbol} option/{interval} day={day_iso} EOD branch FAILED - wrote 0 rows to InfluxDB")
                self.logger.log_failure(
                    symbol=symbol,
                    asset="option",
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message=f"DOWNLOAD_FAILURE: Downloaded {len(df)} rows but wrote 0 to InfluxDB",
                    details={"rows": len(df), "wrote": wrote, "sink": sink_lower, "branch": "EOD", "reason": "influx_write_zero"}
                )
                raise RuntimeError(f"InfluxDB write failed: downloaded {len(df)} rows but wrote 0")
            else:
                print(f"[DOWNLOAD-COMPLETE] {symbol} option/{interval} day={day_iso} EOD branch completed successfully")
                self.logger.log_info(
                    symbol=symbol,
                    asset="option",
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message=f"DOWNLOAD_SUCCESS: rows={len(df)} wrote={wrote}",
                    details={"rows": len(df), "wrote": wrote, "sink": sink_lower, "branch": "EOD"}
                )

            return  # end EOD branch

        
        # -------------------------------
        # INTRADAY (interval != "1d")
        # -------------------------------
        ymd = self._td_ymd(day_iso)
        
        ### >>> EDGE-DAY INTRADAY FETCH — window precompute (inclusive ET) [BEGIN]
        # Compute inclusive ET window to avoid re-downloading whole day on resume
        bar_start_et, bar_end_et = self._compute_intraday_window_et(
            "option", symbol, interval, sink, day_iso
        )
        ### <<< EDGE-DAY INTRADAY FETCH — window precompute (inclusive ET) [END]
        
        _sink_lower = sink.lower()
        
        # >>> HEAD-REFILL override: se il primo _partNN esistente è > 1, non passare start_time
        _head_parts = self._list_day_part_files("option", symbol, interval, _sink_lower, day_iso)
        if _head_parts and _head_parts[0][0] > 1:
            bar_start_et = None
        # <<< HEAD-REFILL override
        
        # ### >>> INFLUX RESUME (intraday options) — BEGIN
        if _sink_lower == "influxdb":
            meas = f"{(self.cfg.influx_measure_prefix or '')}{symbol}-option-{interval}"
        
            # Giorno ET -> finestra UTC
            tz_et = "America/New_York"
            day_start_utc = pd.Timestamp(f"{day_iso}T00:00:00", tz=tz_et).tz_convert("UTC")
            day_end_utc   = (pd.Timestamp(f"{day_iso}T00:00:00", tz=tz_et) + pd.Timedelta(days=1)).tz_convert("UTC")
        
            # 1) NEW: se il giorno è VUOTO in DB, forza omissione start_time (niente GET con &start_time=...)
            try:
                has_any = self._influx_day_has_any(meas, day_iso)
                print(f"[RESUME-INFLUX][CHECK] day={day_iso} meas={meas} has_any={has_any}")
            except Exception as _e:
                print(f"[RESUME-INFLUX][WARN] day-check failed: {type(_e).__name__}: {_e}")
                has_any = None  # prosegui con controllo last_ts
        
            if has_any is False:
                bar_start_et = None
                print(f"[RESUME-INFLUX] day={day_iso} empty in DB -> omit start_time (full-day) meas={meas}")
            else:
                # 2) Altrimenti edge-resume: riparti da ultimo-ts - overlap
                last_in_day = self._influx_last_ts_between(meas, day_start_utc, day_end_utc)
                print(f"[RESUME-INFLUX][CHECK] meas={meas} last_ts_between={last_in_day} "
                      f"range=[{day_start_utc.isoformat()}, {day_end_utc.isoformat()})")
        
                if last_in_day is not None:
                    last_et = last_in_day.tz_convert(tz_et)
                    ov = int(getattr(self.cfg, "overlap_seconds", 0) or 0)
                    bar_start_et = (last_et - pd.Timedelta(seconds=ov)).strftime("%H:%M:%S")
                    print(f"[RESUME-INFLUX] edge-day={day_iso} start_time={bar_start_et} meas={meas}")
                else:
                    # giorno VUOTO (non rilevato dal check rapido): nessun start_time
                    bar_start_et = None
                    print(f"[RESUME-INFLUX] day={day_iso} empty in DB -> omit start_time (full-day) meas={meas}")
        # ### >>> INFLUX RESUME (intraday options) — END

                
        # >>> Optional forced start_time (policy set in _sync_symbol) <<<
        _force_key = ("option", symbol, interval, day_iso)
        if hasattr(self, "_force_start_hms") and _force_key in self._force_start_hms:
            _forced = self._force_start_hms[_force_key]
            if _forced is None:
                bar_start_et = None
                print(f"[RESUME-INFLUX] forced OMIT start_time day={day_iso} (retro-fill inclusive)")
            else:
                bar_start_et = _forced
                print(f"[RESUME-INFLUX] forced start_time={bar_start_et} day={day_iso} (applied after resume/head-refill)")
        # <<< Optional forced start_time
        
        _st = "(omitted)" if bar_start_et is None else bar_start_et
        print(f"[INTRADAY-START] day={day_iso} start_time={_st} sink={_sink_lower}")
                    
        # 1) discover expirations of the day
        expirations = await self._expirations_that_traded(symbol, day_iso, req_type="trade")
        if not expirations:
            expirations = await self._expirations_that_traded(symbol, day_iso, req_type="quote")
        if not expirations:
            print(f"[DOWNLOAD-SKIP] {symbol} option/{interval} day={day_iso} - No expirations traded")
            self.logger.log_info(
                symbol=symbol,
                asset="option",
                interval=interval,
                date_range=(day_iso, day_iso),
                message="DOWNLOAD_SKIP: No expirations traded",
                details={"reason": "no_expirations"}
            )
            return
    
        # 2) fetch TICK **oppure** OHLC+enrichment per expiration e concatena
        dfs = []
        greeks_trade_list: List[pd.DataFrame] = []
        iv_trade_list: List[pd.DataFrame] = []
        greeks_bar_list: List[pd.DataFrame] = []
        iv_bar_list: List[pd.DataFrame] = []

        # IMPORTANT: Tick data does NOT support start_time parameter in ThetaData SDK
        # Force bar_start_et to None for tick interval to avoid total_seconds error
        if interval == "tick":
            bar_start_et = None


        if interval == "tick":
            # TICK: uses trade+quote pairing per expiration (v3)
            # /option/history/trade_quote   date=YYYYMMDD, strike="*", right="both"

            async def _fetch_tick_expiration(exp: str, pass_idx: int):
                try:
                    # Build kwargs to avoid passing None to start_time (causes total_seconds error in SDK)
                    tq_kwargs = {
                        "symbol": symbol,
                        "expiration": exp,
                        "date": ymd,
                        "strike": "*",
                        "right": "both",
                        "exclusive": True,
                        "format_type": "csv",
                    }
                    if bar_start_et is not None:
                        tq_kwargs["start_time"] = bar_start_et

                    csv_tq, _ = await self._td_get_with_retry(
                        lambda: self.client.option_history_trade_quote(**tq_kwargs),
                        label=f"tq {symbol} {exp} {ymd}"
                    )
                    if not csv_tq:
                        if self.logger:
                            self.logger.log_error(
                                asset="option",
                                symbol=symbol,
                                interval=interval,
                                date=day_iso,
                                error_type="TQ_DOWNLOAD_FAILED_INTRADAY",
                                error_message=f"Trade/Quote download failed for expiration {exp} - empty response",
                                severity="WARNING",
                                details={"retry_pass": pass_idx + 1, "retry_total": total_passes}
                            )
                        return False, "tq_empty_response", None, None, None

                    df = pd.read_csv(io.StringIO(csv_tq), dtype=str)
                    df = self._normalize_ts_to_utc(df)
                    df = self._normalize_df_types(df)
                    if df is None or df.empty:
                        if self.logger:
                            self.logger.log_error(
                                asset="option",
                                symbol=symbol,
                                interval=interval,
                                date=day_iso,
                                error_type="TQ_CSV_EMPTY_INTRADAY",
                                error_message=f"Trade/Quote CSV empty for expiration {exp}",
                                severity="WARNING",
                                details={"retry_pass": pass_idx + 1, "retry_total": total_passes}
                            )
                        return False, "tq_csv_empty", None, None, None

                    # ### >>> TICK - CANONICAL TIMESTAMP FROM trade_timestamp - BEGIN
                    # Create the canonical 'timestamp' from 'trade_timestamp' for tick data.
                    if "timestamp" not in df.columns:
                        if "trade_timestamp" not in df.columns:
                            raise RuntimeError("Expected 'trade_timestamp' in tick TQ response.")
                        df["timestamp"] = pd.to_datetime(df["trade_timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
                    # ### <<< TICK - CANONICAL TIMESTAMP FROM trade_timestamp - END

                    if not enrich_tick_greeks:
                        return True, "ok", df, None, None

                    greeks_success = True
                    iv_success = True
                    dg = None
                    divt = None

                    exp_version = self._greeks_version_for_expiration(exp, ymd)
                    csv_tg, _ = await self._td_get_with_retry(
                        lambda: self.client.option_history_all_trade_greeks(
                            symbol=symbol,
                            expiration=exp,
                            date=ymd,
                            strike="*",
                            right="both",
                            greeks_version=exp_version,
                            format_type="csv",
                        ),
                        label=f"greeks/trade {symbol} {exp} {ymd}"
                    )
                    if not csv_tg:
                        greeks_success = False
                        if self.logger:
                            self.logger.log_error(
                                asset="option",
                                symbol=symbol,
                                interval=interval,
                                date=day_iso,
                                error_type="GREEKS_DOWNLOAD_FAILED_INTRADAY",
                                error_message=f"Tick greeks download failed for expiration {exp} - skipping expiration",
                                severity="WARNING"
                            )
                    else:
                        dg = pd.read_csv(io.StringIO(csv_tg), dtype=str)
                        dg = self._normalize_ts_to_utc(dg)
                        dg = self._normalize_df_types(dg)
                        if dg is None or dg.empty:
                            greeks_success = False
                            if self.logger:
                                self.logger.log_error(
                                    asset="option",
                                    symbol=symbol,
                                    interval=interval,
                                    date=day_iso,
                                    error_type="GREEKS_CSV_EMPTY_INTRADAY",
                                    error_message=f"Tick greeks CSV empty for expiration {exp} - skipping expiration",
                                    severity="WARNING"
                                )
                        else:
                            if "strike_price" in dg.columns and "strike" not in dg.columns:
                                dg = dg.rename(columns={"strike_price": "strike"})
                            if "strike_price" in df.columns and "strike" not in df.columns:
                                df = df.rename(columns={"strike_price": "strike"})
                            if ("trade_timestamp" in df.columns
                                and "trade_timestamp" not in dg.columns
                                and "timestamp" in dg.columns):
                                dg = dg.rename(columns={"timestamp": "trade_timestamp"})

                    csv_tiv, _ = await self._td_get_with_retry(
                        lambda: self.client.option_history_trade_implied_volatility(
                            symbol=symbol,
                            expiration=exp,
                            date=ymd,
                            strike="*",
                            right="both",
                            greeks_version=exp_version,
                            format_type="csv",
                        ),
                        label=f"iv/trade {symbol} {exp} {ymd}"
                    )
                    if not csv_tiv:
                        iv_success = False
                        if self.logger:
                            self.logger.log_error(
                                asset="option",
                                symbol=symbol,
                                interval=interval,
                                date=day_iso,
                                error_type="IV_DOWNLOAD_FAILED_INTRADAY",
                                error_message=f"Tick IV download failed for expiration {exp} - skipping expiration",
                                severity="WARNING"
                            )
                    else:
                        divt = pd.read_csv(io.StringIO(csv_tiv), dtype=str)
                        divt = self._normalize_ts_to_utc(divt)
                        divt = self._normalize_df_types(divt)
                        if divt is None or divt.empty:
                            iv_success = False
                            if self.logger:
                                self.logger.log_error(
                                    asset="option",
                                    symbol=symbol,
                                    interval=interval,
                                    date=day_iso,
                                    error_type="IV_CSV_EMPTY_INTRADAY",
                                    error_message=f"Tick IV CSV empty for expiration {exp} - skipping expiration",
                                    severity="WARNING"
                                )
                        else:
                            if "implied_volatility" in divt.columns and "trade_iv" not in divt.columns:
                                divt = divt.rename(columns={"implied_volatility": "trade_iv"})
                            if "strike_price" in divt.columns and "strike" not in divt.columns:
                                divt = divt.rename(columns={"strike_price": "strike"})
                            if "strike_price" in df.columns and "strike" not in df.columns:
                                df = df.rename(columns={"strike_price": "strike"})
                            if ("trade_timestamp" in df.columns
                                and "trade_timestamp" not in divt.columns
                                and "timestamp" in divt.columns):
                                divt = divt.rename(columns={"timestamp": "trade_timestamp"})

                    if greeks_success and iv_success:
                        return True, "ok", df, dg, divt

                    return False, "incomplete_greeks_iv", None, None, None

                except Exception as e:
                    if "472" not in str(e) and "No data found" not in str(e):
                        print(f"[WARN] option tick {symbol} {interval} {day_iso} exp={exp}: {e}")
                    return False, f"exception: {e}", None, None, None

            pending_exps = list(expirations)
            failure_reasons: dict[str, list[str]] = {}
            for pass_idx in range(total_passes):
                if not pending_exps:
                    break
                if pass_idx > 0 and retry_delay > 0:
                    print(f"[INTRADAY-RETRY] {symbol} {interval} {day_iso} pass={pass_idx+1}/{total_passes} sleeping {retry_delay}s for {len(pending_exps)} expirations")
                    await asyncio.sleep(retry_delay)

                current_exps = pending_exps
                pending_exps = []
                for exp in current_exps:
                    success, reason, df, dg, divt = await _fetch_tick_expiration(exp, pass_idx)
                    if success:
                        dfs.append(df)
                        if dg is not None:
                            greeks_trade_list.append(dg)
                        if divt is not None:
                            iv_trade_list.append(divt)
                        if exp in failure_reasons:
                            failure_reasons.pop(exp, None)
                    else:
                        pending_exps.append(exp)
                        failure_reasons.setdefault(exp, []).append(reason)

            if pending_exps:
                failed_details = {exp: failure_reasons.get(exp, []) for exp in pending_exps}
                if self.logger:
                    self.logger.log_error(
                        asset="option",
                        symbol=symbol,
                        interval=interval,
                        date=day_iso,
                        error_type="EXPIRATION_RETRY_EXHAUSTED",
                        error_message=f"Expirations failed after {total_passes} passes: {', '.join(pending_exps)}",
                        severity="WARNING",
                        details={"failed_expirations": failed_details}
                    )

                if skip_day_on_missing:
                    error_msg = f"[SKIP-DAY] option intraday {symbol} {interval} {day_iso}: expirations failed after retries"
                    print(error_msg)
                    if self.logger:
                        self.logger.log_error(
                            asset="option",
                            symbol=symbol,
                            interval=interval,
                            date=day_iso,
                            error_type="INTRADAY_DAY_ABORTED_ON_EXPIRATION_FAILURE",
                            error_message=error_msg,
                            severity="ERROR",
                            details={"failed_expirations": failed_details}
                        )
                    return
        else:
            async def _fetch_intraday_expiration(exp: str, pass_idx: int):
                try:
                    # Build kwargs to avoid passing None to start_time (causes total_seconds error in SDK)
                    ohlc_kwargs = {
                        "symbol": symbol,
                        "expiration": exp,
                        "date": ymd,
                        "interval": interval,
                        "format_type": "csv"
                    }
                    if bar_start_et is not None:
                        ohlc_kwargs["start_time"] = bar_start_et

                    csv_ohlc, _ = await self._td_get_with_retry(
                        lambda: self.client.option_history_ohlc(**ohlc_kwargs),
                        label=f"ohlc {symbol} {exp} {ymd} {interval}"
                    )

                    if not csv_ohlc:
                        if self.logger:
                            self.logger.log_error(
                                asset="option",
                                symbol=symbol,
                                interval=interval,
                                date=day_iso,
                                error_type="OHLC_DOWNLOAD_FAILED_INTRADAY",
                                error_message=f"OHLC download failed for expiration {exp} - empty response",
                                severity="WARNING",
                                details={"retry_pass": pass_idx + 1, "retry_total": total_passes}
                            )
                        return False, "ohlc_empty_response", None, None, None

                    df = pd.read_csv(io.StringIO(csv_ohlc), dtype=str)
                    if df is None or df.empty:
                        if self.logger:
                            self.logger.log_error(
                                asset="option",
                                symbol=symbol,
                                interval=interval,
                                date=day_iso,
                                error_type="OHLC_CSV_EMPTY_INTRADAY",
                                error_message=f"OHLC CSV empty for expiration {exp}",
                                severity="WARNING",
                                details={"retry_pass": pass_idx + 1, "retry_total": total_passes}
                            )
                        return False, "ohlc_csv_empty", None, None, None

                    df = self._normalize_ts_to_utc(df)

                    # Track if greeks/IV succeed when enrich_greeks=True
                    greeks_success = True
                    iv_success = True
                    dg = None
                    div = None

                    if enrich_greeks:
                        exp_version = self._greeks_version_for_expiration(exp, ymd)
                        kwargs = {
                            "symbol": symbol,
                            "expiration": exp,
                            "date": ymd,
                            "interval": interval,
                            "strike": "*",
                            "right": "both",
                            "rate_type": "sofr",
                            "format_type": "csv",
                            "greeks_version": exp_version,
                        }
                        if bar_start_et is not None:
                            kwargs["start_time"] = bar_start_et

                        csv_gr_all, _ = await self._td_get_with_retry(
                            lambda: self.client.option_history_all_greeks(**kwargs),
                            label=f"greeks/all {symbol} {exp} {ymd} {interval}"
                        )

                        if not csv_gr_all:
                            greeks_success = False
                            error_msg = f"[SKIP-EXPIRATION] option intraday {symbol} {interval} {day_iso} exp={exp}: greeks download failed"
                            print(error_msg)
                            if self.logger:
                                self.logger.log_error(
                                    asset="option",
                                    symbol=symbol,
                                    interval=interval,
                                    date=day_iso,
                                    error_type="GREEKS_DOWNLOAD_FAILED_INTRADAY",
                                    error_message=f"Greeks download failed for expiration {exp} - skipping expiration",
                                    severity="WARNING"
                                )
                        else:
                            dg = pd.read_csv(io.StringIO(csv_gr_all), dtype=str)
                            if dg is None or dg.empty:
                                greeks_success = False
                                error_msg = f"[SKIP-EXPIRATION] option intraday {symbol} {interval} {day_iso} exp={exp}: greeks CSV empty"
                                print(error_msg)
                                if self.logger:
                                    self.logger.log_error(
                                        asset="option",
                                        symbol=symbol,
                                        interval=interval,
                                        date=day_iso,
                                        error_type="GREEKS_CSV_EMPTY_INTRADAY",
                                        error_message=f"Greeks CSV empty for expiration {exp} - skipping expiration",
                                        severity="WARNING"
                                    )
                            else:
                                dg = self._normalize_ts_to_utc(dg)

                    # IV bars (bid/mid/ask) - collect once, merge later
                    if enrich_greeks:
                        try:
                            # Build kwargs to avoid passing None to start_time (causes total_seconds error in SDK)
                            exp_version = self._greeks_version_for_expiration(exp, ymd)
                            iv_kwargs = {
                                "symbol": symbol,
                                "expiration": exp,
                                "date": ymd,
                                "interval": interval,
                                "rate_type": "sofr",
                                "format_type": "csv",
                                "greeks_version": exp_version,
                            }
                            if bar_start_et is not None:
                                iv_kwargs["start_time"] = bar_start_et

                            csv_iv, _ = await self._td_get_with_retry(
                                lambda: self.client.option_history_implied_volatility(**iv_kwargs),
                                label=f"greeks/iv {symbol} {exp} {ymd} {interval}"
                            )
                            if not csv_iv:
                                iv_success = False
                                error_msg = f"[SKIP-EXPIRATION] option intraday {symbol} {interval} {day_iso} exp={exp}: IV download failed"
                                print(error_msg)
                                if self.logger:
                                    self.logger.log_error(
                                        asset="option",
                                        symbol=symbol,
                                        interval=interval,
                                        date=day_iso,
                                        error_type="IV_DOWNLOAD_FAILED_INTRADAY",
                                        error_message=f"IV download failed for expiration {exp} - skipping expiration",
                                        severity="WARNING"
                                    )
                            else:
                                div = pd.read_csv(io.StringIO(csv_iv), dtype=str)
                                if div is None or div.empty:
                                    iv_success = False
                                    error_msg = f"[SKIP-EXPIRATION] option intraday {symbol} {interval} {day_iso} exp={exp}: IV CSV empty"
                                    print(error_msg)
                                    if self.logger:
                                        self.logger.log_error(
                                            asset="option",
                                            symbol=symbol,
                                            interval=interval,
                                            date=day_iso,
                                            error_type="IV_CSV_EMPTY_INTRADAY",
                                            error_message=f"IV CSV empty for expiration {exp} - skipping expiration",
                                            severity="WARNING"
                                        )
                                else:
                                    div = self._normalize_ts_to_utc(div)
                        except Exception as e:
                            iv_success = False
                            error_msg = f"[SKIP-EXPIRATION] option intraday {symbol} {interval} {day_iso} exp={exp}: IV exception: {e}"
                            print(error_msg)
                            if self.logger:
                                self.logger.log_error(
                                    asset="option",
                                    symbol=symbol,
                                    interval=interval,
                                    date=day_iso,
                                    error_type="IV_EXCEPTION_INTRADAY",
                                    error_message=f"IV download exception for expiration {exp}: {str(e)}",
                                    severity="WARNING"
                                )

                    if not enrich_greeks or (greeks_success and iv_success):
                        return True, "ok", df, dg, div

                    skip_msg = f"[SKIP-EXPIRATION] option intraday {symbol} {interval} {day_iso} exp={exp}: OHLC not saved due to incomplete greeks/IV"
                    print(skip_msg)
                    return False, "incomplete_greeks_iv", None, None, None

                except Exception as e:
                    print(f"[WARN] option intraday {symbol} {interval} {day_iso} exp={exp}: {e}")
                    return False, f"exception: {e}", None, None, None

            pending_exps = list(expirations)
            failure_reasons: dict[str, list[str]] = {}
            for pass_idx in range(total_passes):
                if not pending_exps:
                    break
                if pass_idx > 0 and retry_delay > 0:
                    print(f"[INTRADAY-RETRY] {symbol} {interval} {day_iso} pass={pass_idx+1}/{total_passes} sleeping {retry_delay}s for {len(pending_exps)} expirations")
                    await asyncio.sleep(retry_delay)

                current_exps = pending_exps
                pending_exps = []
                for exp in current_exps:
                    success, reason, df, dg, div = await _fetch_intraday_expiration(exp, pass_idx)
                    if success:
                        dfs.append(df)
                        if dg is not None:
                            greeks_bar_list.append(dg)
                        if div is not None:
                            iv_bar_list.append(div)
                        if exp in failure_reasons:
                            failure_reasons.pop(exp, None)
                    else:
                        pending_exps.append(exp)
                        failure_reasons.setdefault(exp, []).append(reason)

            if pending_exps:
                failed_details = {exp: failure_reasons.get(exp, []) for exp in pending_exps}
                if self.logger:
                    self.logger.log_error(
                        asset="option",
                        symbol=symbol,
                        interval=interval,
                        date=day_iso,
                        error_type="EXPIRATION_RETRY_EXHAUSTED",
                        error_message=f"Expirations failed after {total_passes} passes: {', '.join(pending_exps)}",
                        severity="WARNING",
                        details={"failed_expirations": failed_details}
                    )

                if skip_day_on_missing:
                    error_msg = f"[SKIP-DAY] option intraday {symbol} {interval} {day_iso}: expirations failed after retries"
                    print(error_msg)
                    if self.logger:
                        self.logger.log_error(
                            asset="option",
                            symbol=symbol,
                            interval=interval,
                            date=day_iso,
                            error_type="INTRADAY_DAY_ABORTED_ON_EXPIRATION_FAILURE",
                            error_message=error_msg,
                            severity="ERROR",
                            details={"failed_expirations": failed_details}
                        )
                    return

        if not dfs:
            # STRICT MODE: All expirations failed or were skipped
            error_msg = f"[SKIP-DAY] option intraday {symbol} {interval} {day_iso}: no data saved (all expirations failed/skipped)"
            print(error_msg)
            if self.logger:
                self.logger.log_error(
                    asset="option",
                    symbol=symbol,
                    interval=interval,
                    date=day_iso,
                    error_type="ALL_EXPIRATIONS_FAILED",
                    error_message=f"All expirations failed or skipped - no data saved for entire day (enrich_greeks={enrich_greeks})",
                    severity="ERROR"
                )
            return

        df_all = pd.concat(dfs, ignore_index=True)

        # NOTE: Timestamps are already normalized to UTC by _normalize_ts_to_utc()
        # called on each individual df before concat (line 2239), so NO need to call again here
        # to avoid double conversion (ET->UTC->UTC which would add +5 hours twice)

        # DEBUG: righe totali e conteggio per scadenza
        try:
            n_rows = len(df_all)
            if "expiration" in df_all.columns:
                exp_counts = df_all["expiration"].astype(str).value_counts().sort_index()
                exp_str = ", ".join(f"{k}:{v}" for k, v in exp_counts.items())
            else:
                exp_str = "(no 'expiration' column)"
            print(f"[INTRADAY] rows={n_rows}  expirations={exp_str}")
        except Exception as e:
            print(f"[INTRADAY] log error: {e}")

        # >>> single_merge_bar_greeks >>>
        if interval != "tick" and enrich_greeks:
            # Merge FULL greeks per-bar (all) e IV bar una sola volta
            # 1) Greeks (all)
            if greeks_bar_list:
                dg_all = pd.concat(greeks_bar_list, ignore_index=True)
                # NOTE: Already normalized at line 2296, no need to convert again
                # normalizza colonne per join robusto
                if "strike_price" in dg_all.columns and "strike" not in dg_all.columns:
                    dg_all = dg_all.rename(columns={"strike_price": "strike"})
                # preferisci colonne timestamp coerenti (bar)
                time_candidates = ["timestamp", "bar_timestamp", "datetime"]
                tcol_df = next((c for c in time_candidates if c in df_all.columns), None)
                tcol_dg = next((c for c in time_candidates if c in dg_all.columns), None)
                if tcol_df and tcol_dg and tcol_dg != tcol_df:
                    dg_all = dg_all.rename(columns={tcol_dg: tcol_df})
                candidate_keys = [
                    ["symbol","expiration","strike","right", tcol_df],
                    ["root","expiration","strike","right", tcol_df],
                    ["option_symbol", tcol_df],
                ]
                on_cols = next((keys for keys in candidate_keys
                                if all(k in df_all.columns for k in keys) and all(k in dg_all.columns for k in keys)), None)
                if on_cols is not None:
                    dup_cols = [c for c in dg_all.columns if c in df_all.columns and c not in on_cols]
                    if dup_cols:
                        dg_all = dg_all.drop(columns=dup_cols)
                    df_all = df_all.merge(dg_all, on=on_cols, how="left")
        
            # 2) IV bar (endpoint implied_volatility returns 'implied_vol' column)
            if iv_bar_list:
                div_all = pd.concat(iv_bar_list, ignore_index=True)
                # NOTE: Already normalized at line 2350, no need to convert again
                # ThetaData V3 API returns 'implied_vol' not 'implied_volatility'
                if "implied_vol" in div_all.columns and "bar_iv" not in div_all.columns:
                    div_all = div_all.rename(columns={"implied_vol": "bar_iv"})
                if "strike_price" in div_all.columns and "strike" not in div_all.columns:
                    div_all = div_all.rename(columns={"strike_price": "strike"})
                time_candidates = ["timestamp", "bar_timestamp", "datetime"]
                tcol_df = next((c for c in time_candidates if c in df_all.columns), None)
                tcol_dv = next((c for c in time_candidates if c in div_all.columns), None)
                if tcol_df and tcol_dv and tcol_dv != tcol_df:
                    div_all = div_all.rename(columns={tcol_dv: tcol_df})
                candidate_keys = [
                    ["symbol","expiration","strike","right", tcol_df],
                    ["root","expiration","strike","right", tcol_df],
                    ["option_symbol", tcol_df],
                ]
                on_cols = next((keys for keys in candidate_keys
                                if all(k in df_all.columns for k in keys) and all(k in div_all.columns for k in keys)), None)
                if on_cols is not None:
                    dup_cols = [c for c in div_all.columns if c in df_all.columns and c not in on_cols]
                    if dup_cols:
                        div_all = div_all.drop(columns=dup_cols)
                    df_all = df_all.merge(div_all, on=on_cols, how="left")
        # <<< single_merge_bar_greeks <<<


        # Esegui i merge "per-trade" SOLO nel ramo tick
        if interval == "tick" and enrich_tick_greeks and greeks_trade_list:
            dg_all = pd.concat(greeks_trade_list, ignore_index=True)
            # normalize for robust join
            if "strike_price" in dg_all.columns and "strike" not in dg_all.columns:
                dg_all = dg_all.rename(columns={"strike_price": "strike"})
            if "trade_timestamp" in df_all.columns and "trade_timestamp" not in dg_all.columns and "timestamp" in dg_all.columns:
                dg_all = dg_all.rename(columns={"timestamp": "trade_timestamp"})
            candidate_keys = [
                ["symbol","expiration","strike","right","trade_timestamp","sequence"],
                ["symbol","expiration","strike","right","trade_timestamp"],
                ["root","expiration","strike","right","trade_timestamp","sequence"],
                ["root","expiration","strike","right","trade_timestamp"],
                ["option_symbol","trade_timestamp","sequence"],
                ["option_symbol","trade_timestamp"],
            ]
            on_cols = next((keys for keys in candidate_keys
                            if all(k in df_all.columns for k in keys) and all(k in dg_all.columns for k in keys)), None)
            if on_cols is not None:
                dup_cols = [c for c in dg_all.columns if c in df_all.columns and c not in on_cols]
                if dup_cols:
                    dg_all = dg_all.drop(columns=dup_cols)
                df_all = df_all.merge(dg_all, on=on_cols, how="left")
        
        if interval == "tick" and enrich_tick_greeks and iv_trade_list:
            divt_all = pd.concat(iv_trade_list, ignore_index=True)
            # ThetaData V3 API returns 'implied_vol' not 'implied_volatility'
            if "implied_vol" in divt_all.columns and "trade_iv" not in divt_all.columns:
                divt_all = divt_all.rename(columns={"implied_vol": "trade_iv"})
            if "strike_price" in divt_all.columns and "strike" not in divt_all.columns:
                divt_all = divt_all.rename(columns={"strike_price": "strike"})
            if "trade_timestamp" in df_all.columns and "trade_timestamp" not in divt_all.columns and "timestamp" in divt_all.columns:
                divt_all = divt_all.rename(columns={"timestamp": "trade_timestamp"})
            candidate_keys = [
                ["symbol","expiration","strike","right","trade_timestamp","sequence"],
                ["symbol","expiration","strike","right","trade_timestamp"],
                ["root","expiration","strike","right","trade_timestamp","sequence"],
                ["root","expiration","strike","right","trade_timestamp"],
                ["option_symbol","trade_timestamp","sequence"],
                ["option_symbol","trade_timestamp"],
            ]
            on_cols = next((keys for keys in candidate_keys
                            if all(k in df_all.columns for k in keys) and all(k in divt_all.columns for k in keys)), None)
            if on_cols is not None:
                dup_cols = [c for c in divt_all.columns if c in df_all.columns and c not in on_cols]
                if dup_cols:
                    divt_all = divt_all.drop(columns=dup_cols)
                df_all = df_all.merge(divt_all, on=on_cols, how="left")
                

        # last_day_OI (same value for all rows of the contract)
        def _missing_mask_for_cols(df, cols):
            if df is None or df.empty or not cols:
                return None
            mask = None
            for col in cols:
                if col not in df.columns:
                    return pd.Series([True] * len(df), index=df.index)
                s = df[col]
                missing = s.isna()
                if s.dtype == object:
                    missing |= s.astype(str).str.strip().eq("")
                mask = missing if mask is None else (mask | missing)
            return mask

        def _normalize_join_cols(df):
            if df is None or df.empty:
                return df
            # Normalize expiration format for robust joins
            if "expiration" not in df.columns:
                for alt in ("expiration_date", "expirationDate", "exp", "exp_date"):
                    if alt in df.columns:
                        df = df.rename(columns={alt: "expiration"})
                        break
            if "expiration" in df.columns:
                exp_raw = df["expiration"].astype(str).str.strip()
                exp_raw = exp_raw.map(self._iso_date_only)
                df["expiration"] = exp_raw.map(lambda x: self._normalize_date_str(x) or x)
            # Normalize strike format (e.g., 30 vs 30.0)
            if "strike" in df.columns:
                strike_raw = df["strike"].astype(str).str.strip()
                strike_num = pd.to_numeric(strike_raw, errors="coerce")
                df["strike"] = [
                    (f"{n:.10f}".rstrip("0").rstrip(".") if pd.notna(n) else r)
                    for r, n in zip(strike_raw, strike_num)
                ]
            if "right" in df.columns:
                df["right"] = (
                    df["right"].astype(str).str.strip().str.lower()
                    .replace({"c": "call", "p": "put"})
                )
            if "symbol" not in df.columns and "root" in df.columns:
                df["symbol"] = df["root"]
            if "root" not in df.columns and "symbol" in df.columns:
                df["root"] = df["symbol"]
            return df

        oi_missing_mask = None
        oi_missing_details = {}
        oi_merge_keys = None

        try:
            d = dt.fromisoformat(day_iso)
            prev = d - timedelta(days=1)
            if prev.weekday() == 5:
                prev = prev - timedelta(days=1)
            elif prev.weekday() == 6:
                prev = prev - timedelta(days=2)

            cur_ymd = dt.fromisoformat(day_iso).strftime("%Y%m%d")

            today_utc = dt.now(timezone.utc).date().isoformat()
            is_current_day = (day_iso == today_utc)

            df_oi_base = df_all
            use_cache = bool(self.cfg.enable_oi_caching)

            async def _fetch_oi_remote():
                if is_current_day and expirations:
                    print(f"[OI-FETCH] current-day mode: per-expiration ({len(expirations)} exps)")
                    oi_dfs = []
                    failed_exps = []
                    for exp in expirations:
                        try:
                            csv_oi_exp, _ = await self._td_get_with_retry(
                                lambda: self.client.option_history_open_interest(
                                    symbol=symbol,
                                    expiration=exp,
                                    date=cur_ymd,
                                    strike="*",
                                    right="both",
                                    format_type="csv",
                                ),
                                label=f"oi {symbol} {exp} {cur_ymd}"
                            )
                            if csv_oi_exp:
                                oi_dfs.append(pd.read_csv(io.StringIO(csv_oi_exp), dtype=str))
                            else:
                                failed_exps.append(exp)
                        except Exception as e:
                            failed_exps.append(exp)
                            print(f"[WARN] OI exp={exp} day={day_iso}: {e}")

                    if oi_dfs:
                        return pd.concat(oi_dfs, ignore_index=True), failed_exps
                    return None, failed_exps

                csv_oi, _ = await self._td_get_with_retry(
                    lambda: self.client.option_history_open_interest(
                        symbol=symbol,
                        expiration="*",
                        date=cur_ymd,
                        strike="*",
                        right="both",
                        format_type="csv",
                    ),
                    label=f"oi {symbol} {cur_ymd}"
                )
                if csv_oi:
                    return pd.read_csv(io.StringIO(csv_oi), dtype=str), []
                return None, []

            for oi_pass in range(total_passes):
                doi = None
                oi_from_cache = False
                oi_failed_exps = []

                if use_cache and self.cfg.enable_oi_caching:
                    print(f"[OI-CACHE] Checking local cache for {symbol} date={cur_ymd}...")
                    if self._check_oi_cache(symbol, cur_ymd):
                        print(f"[OI-CACHE] Found current date OI in local cache - retrieving from file")
                        doi = self._load_oi_from_cache(symbol, cur_ymd)
                        if doi is not None and not doi.empty:
                            oi_from_cache = True
                            print(f"[OI-CACHE][SUCCESS] Retrieved {len(doi)} OI records from local cache (no remote API call)")
                        else:
                            print("[OI-CACHE][WARN] Cache file exists but load returned empty - falling back to remote")

                if not oi_from_cache:
                    if self.cfg.enable_oi_caching and use_cache:
                        print("[OI-CACHE] Current date OI not found in local cache - starting remote data source fetching")
                    elif self.cfg.enable_oi_caching and not use_cache:
                        print("[OI-CACHE] Cache bypassed (retry mode) - fetching from remote data source")
                    else:
                        print("[OI-CACHE] OI caching disabled (enable_oi_caching=False) - fetching from remote data source")
                    doi, oi_failed_exps = await _fetch_oi_remote()

                if doi is not None and not doi.empty:
                    if self.cfg.enable_oi_caching and not oi_from_cache:
                        print("[OI-CACHE] Saving current date OI to local cache for future reuse...")
                        doi_cache = doi.copy()
                        doi_cache["request_date"] = cur_ymd
                        saved = self._save_oi_to_cache(symbol, cur_ymd, doi_cache)
                        if saved:
                            cache_path = self._get_oi_cache_path(symbol, cur_ymd)
                            print(f"[OI-CACHE][SAVED] Successfully saved {len(doi_cache)} OI records to local cache: {cache_path}")
                        else:
                            print("[OI-CACHE][WARN] Failed to save OI to local cache - check write permissions")

                    df_norm = _normalize_join_cols(self._normalize_df_types(df_oi_base))
                    doi_norm = _normalize_join_cols(self._normalize_df_types(doi))

                    # ### >>> OI MERGE DIAGNOSTICS — BEGIN
                    try:
                        src = "cache" if oi_from_cache else "remote"
                        cols_hint = [c for c in ["option_symbol", "root", "symbol", "expiration", "strike", "strike_price", "right", "timestamp", "ts", "time", "open_interest", "oi", "OI"] if c in doi_norm.columns]
                        print(f"[OI-MERGE][INPUT] {symbol} {interval} {day_iso} src={src} doi_rows={len(doi_norm)} doi_cols={cols_hint}")
                    except Exception:
                        pass
                    # ### >>> OI MERGE DIAGNOSTICS — END


                    # ### >>> OI JOIN COLUMN NORMALIZATION (strike_price alias) — BEGIN
                    if "strike_price" in doi_norm.columns and "strike" not in doi_norm.columns:
                        doi_norm = doi_norm.rename(columns={"strike_price": "strike"})
                    if "strike_price" in df_norm.columns and "strike" not in df_norm.columns:
                        df_norm = df_norm.rename(columns={"strike_price": "strike"})
                    # ### >>> OI JOIN COLUMN NORMALIZATION (strike_price alias) — END

                    oi_col = next((c for c in ["open_interest", "oi", "OI"] if c in doi_norm.columns), None)
                    if not oi_col:
                        df_all = df_norm
                        oi_missing_mask = pd.Series([True] * len(df_norm), index=df_norm.index)
                        oi_missing_details = {
                            "reason": "oi_column_missing",
                            "retry_pass": oi_pass + 1,
                            "retry_total": total_passes
                        }
                    else:
                        doi_norm = doi_norm.rename(columns={oi_col: "last_day_OI"})
                        ts_col = next((c for c in ["timestamp", "ts", "time"] if c in doi_norm.columns), None)
                        if ts_col:
                            doi_norm = doi_norm.rename(columns={ts_col: "timestamp_oi"})
                        if "timestamp_oi" in doi_norm.columns:
                            _eff = pd.to_datetime(doi_norm["timestamp_oi"], errors="coerce")
                            doi_norm["effective_date_oi"] = _eff.dt.tz_localize("UTC").dt.tz_convert("America/New_York").dt.date.astype(str)
                        else:
                            doi_norm["effective_date_oi"] = prev.date().isoformat()

                        candidate_keys = [
                            ["option_symbol"],
                            ["root", "expiration", "strike", "right"],
                            ["symbol", "expiration", "strike", "right"],
                        ]

                        # ### >>> OI MERGE DIAGNOSTICS (key coverage) — BEGIN
                        avail = []
                        for keys in candidate_keys:
                            if not (all(k in df_norm.columns for k in keys) and all(k in doi_norm.columns for k in keys)):
                                continue
                            try:
                                dfk = df_norm[keys].drop_duplicates()
                                doik = doi_norm[keys].drop_duplicates()
                                chk = dfk.merge(doik, on=keys, how="left", indicator=True)
                                miss = int((chk["_merge"] == "left_only").sum())
                                tot = int(len(chk))
                                avail.append((keys, miss, tot))
                            except Exception as _diag_e:
                                print(f"[OI-MERGE][DIAG][WARN] {symbol} {interval} {day_iso} key={keys} diag_failed={type(_diag_e).__name__}: {_diag_e}")

                        if avail:
                            parts = [f"key={k} unmatched_keys={m}/{t}" for (k, m, t) in avail]
                            print(f"[OI-MERGE][DIAG] {symbol} {interval} {day_iso} " + " | ".join(parts))
                        # ### >>> OI MERGE DIAGNOSTICS (key coverage) — END

                        on_cols = next(
                            (keys for keys in candidate_keys if all(k in df_norm.columns for k in keys) and all(k in doi_norm.columns for k in keys)),
                            None
                        )
                        oi_merge_keys = on_cols
                        print(f"[OI-MERGE][KEY] {symbol} {interval} {day_iso} using={oi_merge_keys}")

                                                
                        if on_cols is not None:
                            keep = on_cols + ["last_day_OI"]
                            if "timestamp_oi" in doi_norm.columns:
                                keep += ["timestamp_oi"]
                            keep += ["effective_date_oi"]
                            doi_norm = doi_norm[keep].drop_duplicates(subset=on_cols)
                            df_all = df_norm.merge(doi_norm, on=on_cols, how="left")
                            oi_missing_mask = _missing_mask_for_cols(df_all, ["last_day_OI"])

                            # ### >>> OI ABSENT-CONTRACTS FILL (NaN->0) — BEGIN
                            try:
                                if oi_missing_mask is not None and oi_missing_mask.any() and oi_merge_keys:
                                    # "Absent from snapshot" means: contract key does NOT exist in the OI snapshot keys.
                                    _snap_keys = doi_norm[oi_merge_keys].drop_duplicates()
                                    _mi_snap = pd.MultiIndex.from_frame(_snap_keys)

                                    _mi_all = pd.MultiIndex.from_frame(df_all[oi_merge_keys])
                                    _absent_contract_mask = ~_mi_all.isin(_mi_snap)

                                    _fill_mask = _absent_contract_mask & df_all["last_day_OI"].isna()
                                    _filled_rows = int(_fill_mask.sum())

                                    if _filled_rows > 0:
                                        _absent_unique = int(df_all.loc[_absent_contract_mask, oi_merge_keys].drop_duplicates().shape[0])

                                        df_all.loc[_fill_mask, "last_day_OI"] = 0

                                        print(
                                            f"[OI-MERGE][FILL0] {symbol} {interval} {day_iso} "
                                            f"absent_contracts={_absent_unique} filled_rows={_filled_rows}"
                                        )

                                        # Recompute missing mask after filling absent contracts
                                        oi_missing_mask = _missing_mask_for_cols(df_all, ["last_day_OI"])
                            except Exception as _fill_e:
                                print(
                                    f"[OI-MERGE][FILL0][WARN] {symbol} {interval} {day_iso} "
                                    f"fill_failed={type(_fill_e).__name__}: {_fill_e}"
                                )
                            # ### >>> OI ABSENT-CONTRACTS FILL (NaN->0) — END

                            try:
                                miss_cnt = int(oi_missing_mask.sum()) if oi_missing_mask is not None else 0
                                tot_cnt = int(len(df_all)) if df_all is not None else 0
                                pct = (100.0 * miss_cnt / max(tot_cnt, 1))

                                uniq_msg = ""
                                if oi_merge_keys:
                                    miss_unique = int(df_all.loc[oi_missing_mask, oi_merge_keys].drop_duplicates().shape[0]) if miss_cnt > 0 else 0
                                    tot_unique = int(df_all[oi_merge_keys].drop_duplicates().shape[0])
                                    uniq_msg = f" missing_unique_contracts={miss_unique}/{tot_unique}"

                                print(
                                    f"[OI-MERGE][RESULT] {symbol} {interval} {day_iso} "
                                    f"pass={oi_pass+1}/{total_passes} keys={oi_merge_keys} "
                                    f"missing_rows={miss_cnt}/{tot_cnt} ({pct:.2f}%){uniq_msg}"
                                )

                                if miss_cnt > 0:
                                    sample_cols = [c for c in ["option_symbol", "root", "expiration", "strike", "right"] if c in df_all.columns]
                                    if sample_cols:
                                        print("[OI-MERGE][SAMPLE-MISS] first 8 missing rows keys:")
                                        print(df_all.loc[oi_missing_mask, sample_cols].head(8).to_string(index=False))

                                    if "expiration" in df_all.columns:
                                        vc = df_all.loc[oi_missing_mask, "expiration"].astype(str).value_counts().head(8)
                                        print("[OI-MERGE][MISS-EXP] top expirations missing OI:")
                                        print(vc.to_string())
                            except Exception as _log_e:
                                print(f"[OI-MERGE][LOG][WARN] {symbol} {interval} {day_iso} log_failed={type(_log_e).__name__}: {_log_e}")


                            if oi_missing_mask is not None and oi_missing_mask.any():
                                oi_missing_details = {
                                    "reason": "oi_missing_values",
                                    "retry_pass": oi_pass + 1,
                                    "retry_total": total_passes,
                                    "missing_rows": int(oi_missing_mask.sum())
                                }
                                if oi_failed_exps:
                                    oi_missing_details["oi_failed_expirations"] = oi_failed_exps
                            else:
                                oi_missing_mask = None
                                break
                        else:
                            df_all = df_norm
                            oi_missing_mask = pd.Series([True] * len(df_norm), index=df_norm.index)
                            oi_missing_details = {
                                "reason": "oi_merge_keys_missing",
                                "retry_pass": oi_pass + 1,
                                "retry_total": total_passes
                            }
                else:
                    df_all = df_oi_base
                    oi_missing_mask = pd.Series([True] * len(df_oi_base), index=df_oi_base.index)
                    oi_missing_details = {
                        "reason": "oi_empty_response",
                        "retry_pass": oi_pass + 1,
                        "retry_total": total_passes
                    }
                    if oi_failed_exps:
                        oi_missing_details["oi_failed_expirations"] = oi_failed_exps

                if oi_missing_mask is None or not oi_missing_mask.any():
                    break

                if oi_pass < total_passes - 1:
                    if oi_from_cache:
                        # Cache data won't change; sleeping is pointless. Retry immediately with remote fetch.
                        print(f"[OI-RETRY] {symbol} {interval} {day_iso} pass={oi_pass+1}/{total_passes} src=cache -> skipping sleep, retrying immediately (remote fetch)")
                        use_cache = False
                    elif retry_delay > 0:
                        print(f"[OI-RETRY] {symbol} {interval} {day_iso} pass={oi_pass+1}/{total_passes} sleeping {retry_delay}s")
                        await asyncio.sleep(retry_delay)
                        use_cache = False
                    else:
                        use_cache = False


        except Exception as e:
            print(f"[WARN] intraday OI merge {symbol} {interval} {day_iso}: {e}")
            if df_all is not None and not df_all.empty:
                oi_missing_mask = pd.Series([True] * len(df_all), index=df_all.index)
                oi_missing_details = {"reason": "oi_exception", "error": str(e)}

        missing_mask = None
        missing_details = {}

        if oi_missing_mask is not None and oi_missing_mask.any():
            missing_mask = oi_missing_mask if missing_mask is None else (missing_mask | oi_missing_mask)
            missing_details["oi_missing_rows"] = int(oi_missing_mask.sum())
            if oi_missing_details:
                missing_details["oi_details"] = oi_missing_details
            if "expiration" in df_all.columns:
                missing_exps = df_all.loc[oi_missing_mask, "expiration"].astype(str).dropna().unique().tolist()
                if missing_exps:
                    missing_details["oi_missing_expirations"] = missing_exps[:25]

        if interval == "tick":
            if enrich_tick_greeks:
                req_cols = ["delta", "gamma", "theta", "vega", "rho", "trade_iv"]
                greek_mask = _missing_mask_for_cols(df_all, req_cols)
                if greek_mask is not None and greek_mask.any():
                    missing_mask = greek_mask if missing_mask is None else (missing_mask | greek_mask)
                    missing_details["greeks_iv_missing_rows"] = int(greek_mask.sum())
        else:
            if enrich_greeks:
                req_cols = ["delta", "gamma", "theta", "vega", "rho", "implied_vol", "bar_iv"]
                greek_mask = _missing_mask_for_cols(df_all, req_cols)
                if greek_mask is not None and greek_mask.any():
                    missing_mask = greek_mask if missing_mask is None else (missing_mask | greek_mask)
                    missing_details["greeks_iv_missing_rows"] = int(greek_mask.sum())

        if missing_mask is not None and missing_mask.any():
            if self.logger:
                self.logger.log_error(
                    asset="option",
                    symbol=symbol,
                    interval=interval,
                    date=day_iso,
                    error_type="ENRICHMENT_MISSING_INTRADAY",
                    error_message="Missing OI/Greeks/IV rows after merge",
                    severity="WARNING",
                    details=missing_details
                )

            if skip_day_on_missing:
                error_msg = f"[SKIP-DAY] option intraday {symbol} {interval} {day_iso}: missing OI/Greeks/IV rows"
                print(error_msg)
                if self.logger:
                    self.logger.log_error(
                        asset="option",
                        symbol=symbol,
                        interval=interval,
                        date=day_iso,
                        error_type="INTRADAY_DAY_ABORTED_ON_MISSING_ENRICHMENT",
                        error_message=error_msg,
                        severity="ERROR",
                        details=missing_details
                    )
                return

            df_all = df_all.loc[~missing_mask].copy()
            if df_all.empty:
                error_msg = f"[SKIP-DAY] option intraday {symbol} {interval} {day_iso}: all rows dropped due to missing OI/Greeks/IV"
                print(error_msg)
                if self.logger:
                    self.logger.log_error(
                        asset="option",
                        symbol=symbol,
                        interval=interval,
                        date=day_iso,
                        error_type="INTRADAY_EMPTY_AFTER_ENRICHMENT_DROP",
                        error_message=error_msg,
                        severity="ERROR",
                        details=missing_details
                    )
                return
        # --- GLOBAL ORDER (stabile per part temporali) ---
        time_candidates = ["trade_timestamp","timestamp","bar_timestamp","datetime","created","last_trade"]
        tcol = next((c for c in time_candidates if c in df_all.columns), None)
        if tcol:
            # Parse as UTC-naive; if any tz-aware values exist, convert to UTC and drop tz.
            tmp = pd.to_datetime(df_all[tcol], errors="coerce", utc=True)
            df_all[tcol] = tmp.dt.tz_convert("UTC").dt.tz_localize(None)
            order_cols = [tcol] + [c for c in ["expiration","right","strike","root","option_symbol","symbol"] if c in df_all.columns]
            # mergesort => stabile: mantiene l’ordine relativo dentro stesso ts
            df_all = df_all.sort_values(order_cols, kind="mergesort").reset_index(drop=True)
        # --- /GLOBAL ORDER ---

        # --- UNIFIED REBUILD (CSV/Parquet) ---
        # Se NON è l'ultimo giorno locale e:
        #   - mancano i part iniziali (part01 assente), oppure
        #   - il giorno è "missing/mixed",
        # allora PURGE di tutto il giorno e riscrittura completa (niente head-only).
        sink_lower = sink.lower()
        series_first_iso, series_last_iso = self._get_first_last_day_from_sink("option", symbol, interval, sink_lower)
        st_now = self._day_parts_status("option", symbol, interval, sink_lower, day_iso)
        parts_now = st_now.get("parts") or []
        head_missing = (parts_now and 1 not in parts_now)
        if (series_last_iso is not None and day_iso < series_last_iso) and (head_missing or st_now.get("missing") or st_now.get("has_mixed")):
            self._purge_day_files("option", symbol, interval, sink_lower, day_iso)
        # --- /UNIFIED REBUILD ---

                
        
                                        
        # --- TAIL-RESUME (>= + boundary dedupe per opzioni intraday) ---
        parts_today = self._list_day_files("option", symbol, interval, sink_lower, day_iso)
        
        # Per le opzioni usiamo SEMPRE la colonna 'timestamp'
        tcol_df = "timestamp"
        if tcol_df not in df_all.columns:
            raise RuntimeError("Per le opzioni manca la colonna 'timestamp' nel df_all.")
        
        # >>> INFLUX-AWARE CUTOFF (no file scan) 
        cutoff = None
        if _sink_lower == "influxdb":
            meas = f"{(self.cfg.influx_measure_prefix or '')}{symbol}-option-{interval}"
            
            day_start_utc = pd.Timestamp(f"{day_iso}T00:00:00", tz=tz_et).tz_convert("UTC")
            day_end_utc = (pd.Timestamp(f"{day_iso}T00:00:00", tz=tz_et) + pd.Timedelta(days=1)).tz_convert("UTC")
            
            try:
                has_any = self._influx_day_has_any(meas, day_iso)
                if has_any is False:
                    bar_start_et = None
                    print(f"[RESUME-INFLUX] day={day_iso} empty in DB -> omit start_time (full-day) meas={meas}")
                else:
                    last_in_day = self._influx_last_ts_between(meas, day_start_utc, day_end_utc)
                    print(f"[RESUME-INFLUX][CHECK] meas={meas} last_ts_between={last_in_day} "
                          f"range=[{day_start_utc.isoformat()}, {day_end_utc.isoformat()})")
                    
                    if last_in_day is not None:
                        last_et = last_in_day.tz_convert(tz_et)
                        ov = int(getattr(self.cfg, "overlap_seconds", 0) or 0)
                        
                        # >>> FIX: per interval != "tick", allinea al bucket DOPO l'overlap
                        iv = (interval or "").strip().lower()
                        if iv.endswith("m") and iv[:-1].isdigit():
                            m = int(iv[:-1])
                            resume_et = last_et - pd.Timedelta(seconds=ov)
                            floored = resume_et.replace(second=0, microsecond=0)
                            floored = floored.replace(minute=(floored.minute // m) * m)
                            bar_start_et = floored.strftime("%H:%M:%S")
                        elif iv.endswith("h") and iv[:-1].isdigit():
                            h = int(iv[:-1])
                            resume_et = last_et - pd.Timedelta(seconds=ov)
                            floored = resume_et.replace(minute=0, second=0, microsecond=0)
                            floored = floored.replace(hour=(floored.hour // h) * h)
                            bar_start_et = floored.strftime("%H:%M:%S")
                        else:
                            # tick/secondi: usa solo overlap
                            bar_start_et = (last_et - pd.Timedelta(seconds=ov)).strftime("%H:%M:%S")
                        
                        print(f"[RESUME-INFLUX] edge-day={day_iso} start_time={bar_start_et} meas={meas}")
                    else:
                        bar_start_et = None
                        print(f"[RESUME-INFLUX] day={day_iso} empty in DB -> omit start_time (full-day) meas={meas}")
            except Exception as e:
                print(f"[RESUME-INFLUX][WARN] query failed: {e}")
                bar_start_et = None
        
        elif sink_lower in ("csv", "parquet") and parts_today:
            # File-based: helper esistente
            def _read_minimal(path: str, cols: list[str]):
                use = list(dict.fromkeys([c for c in cols if c]))
                if sink_lower == "csv":
                    head = pd.read_csv(path, nrows=0, dtype=str)
                    keep = [c for c in use if c in head.columns]
                    df = pd.read_csv(path, usecols=keep, dtype=str) if keep else pd.read_csv(path, dtype=str)
                else:
                    try:
                        df = pd.read_parquet(path, columns=use)
                    except Exception:
                        df = pd.read_parquet(path)
                        df = df[[c for c in use if c in df.columns]]
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
                return df
            
            def _max_file_ts(path: str, tcol: str):
                try:
                    d = _read_minimal(path, [tcol])
                    if tcol not in d.columns:
                        return None
                    ts = pd.to_datetime(d[tcol], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
                    return ts.max() if ts.notna().any() else None
                except Exception:
                    return None
            
            max_list = []
            for p in parts_today:
                mt = _max_file_ts(p, tcol_df)
                if mt is not None:
                    max_list.append(mt)
            if max_list:
                cutoff = max(max_list)
                print(f"[TAIL-RESUME][FILE] cutoff={cutoff} (from {len(parts_today)} files)")
        
        # >>> FILTER + BOUNDARY DEDUP 
        if cutoff is not None:
            # Normalizza left side (df_all) a UTC-naive
            ts_all = pd.to_datetime(df_all[tcol_df], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
            
            cutoff_naive = pd.to_datetime(cutoff, utc=True).tz_convert("UTC").tz_localize(None) if hasattr(cutoff, 'tz') else cutoff
            
            # Filtro > cutoff (mantiene solo nuovi dati)
            df_tail = df_all.loc[ts_all > cutoff_naive].copy()
            
            if df_tail.empty:
                print(f"[TAIL-RESUME] no new data after cutoff={cutoff}")
                return 0
            
            print(f"[TAIL-RESUME] kept {len(df_tail)}/{len(df_all)} rows (>{cutoff})")
            
            # >>> BOUNDARY DEDUP: rimuovi eventuali duplicati al confine
            if sink_lower in ("csv", "parquet") and parts_today:
                try:
                    # Leggi ultime 1000 righe del file più recente
                    latest = max(parts_today, key=os.path.getmtime)
                    last_chunk = _read_minimal(latest, [tcol_df, "symbol", "expiration", "strike", "right", "sequence"])
                    
                    if not last_chunk.empty and tcol_df in last_chunk.columns:
                        # Normalizza timestamp chunk
                        ts_chunk = pd.to_datetime(last_chunk[tcol_df], errors="coerce")
                        if getattr(ts_chunk.dtype, "tz", None) is not None:
                            ts_chunk = ts_chunk.dt.tz_convert("UTC").dt.tz_localize(None)
                        last_chunk[tcol_df] = ts_chunk
                        
                        # Prendi solo ultime 1000 righe
                        last_chunk = last_chunk.tail(1000)
                        
                        # Chiave logica
                        key_cols = [tcol_df, "symbol", "expiration", "strike"]
                        if "right" in df_tail.columns and "right" in last_chunk.columns:
                            key_cols.append("right")
                        if interval == "tick" and "sequence" in df_tail.columns and "sequence" in last_chunk.columns:
                            key_cols.append("sequence")
                        
                        # Normalizza tipi per confronto
                        for col in ["expiration", "strike", "right"]:
                            if col in key_cols:
                                if col == "expiration" and col in last_chunk.columns:
                                    last_chunk[col] = pd.to_datetime(last_chunk[col], errors="coerce").dt.date
                                elif col == "strike" and col in last_chunk.columns:
                                    last_chunk[col] = pd.to_numeric(last_chunk[col], errors="coerce")
                                elif col == "right" and col in last_chunk.columns:
                                    last_chunk[col] = last_chunk[col].astype(str).str.strip().str.lower()
                        
                        if all(c in last_chunk.columns for c in key_cols):
                            existing_keys = set(map(tuple, last_chunk[key_cols].dropna().to_numpy()))
                            if existing_keys:
                                new_keys = df_tail[key_cols].apply(tuple, axis=1)
                                before = len(df_tail)
                                df_tail = df_tail[~new_keys.isin(existing_keys)]
                                removed = before - len(df_tail)
                                if removed > 0:
                                    print(f"[TAIL-RESUME][BOUNDARY-DEDUP] removed {removed} boundary duplicates")
                except Exception as e:
                    print(f"[TAIL-RESUME][WARN] boundary dedup failed: {e}")
            
            # Ordine stabile
            order_cols = ["timestamp"] + [c for c in ["expiration", "right", "strike", "symbol"] if c in df_tail.columns]
            df_tail = df_tail.sort_values(order_cols, kind="mergesort").reset_index(drop=True)
            df_all = df_tail
        
        # --- /TAIL-RESUME ---

        # ORDINA: prima per 'timestamp' (obbligatorio), poi per chiavi contratto
        if "timestamp" not in df_all.columns:
            raise RuntimeError("Manca la colonna 'timestamp' nel df_all (opzioni intraday).")
        
        ts = pd.to_datetime(df_all["timestamp"], errors="coerce", utc=True)
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
        df_all["timestamp"] = ts
        order_cols = ["timestamp"] + [c for c in ["expiration","right","strike","root","option_symbol","symbol"] if c in df_all.columns]
        df_all = df_all.sort_values(order_cols, kind="mergesort").reset_index(drop=True)


        # --- HARD DEDUPE su chiave logica prima del writer (intraday options) ---
        # Normalizzazione tipi per coerenza della chiave (evita string vs date ecc.)
        if "expiration" in df_all.columns:
            df_all["expiration"] = pd.to_datetime(df_all["expiration"], errors="coerce").dt.date
        if "strike" in df_all.columns:
            df_all["strike"] = pd.to_numeric(df_all["strike"], errors="coerce")
        if "right" in df_all.columns:
            df_all["right"] = (
                df_all["right"].astype(str).str.strip().str.lower()
                .map({"c": "call", "p": "put", "call": "call", "put": "put"})
                .fillna(df_all["right"])
            )
        
        _key_cols = ["timestamp", "symbol", "expiration", "strike"] + (["right"] if "right" in df_all.columns else [])
        if interval == "tick" and "sequence" in df_all.columns:
            _key_cols.append("sequence")

        if all(c in df_all.columns for c in _key_cols):
            df_all = (
                df_all
                .dropna(subset=_key_cols)
                .drop_duplicates(subset=_key_cols, keep="last")
            )
        # --- /HARD DEDUPE ---

        # ===== VALIDATION (Real-Time) =====
        # Validate data before persisting
        validation_ok = await self._validate_downloaded_data(
            df=df_all,
            asset="option",
            symbol=symbol,
            interval=interval,
            day_iso=day_iso,
            sink=sink,
            enrich_greeks=enrich_greeks,
            enrich_tick_greeks=enrich_tick_greeks
        )

        if not validation_ok:
            # Validation failed in strict mode - do not save
            print(f"[VALIDATION] STRICT MODE: Skipping save for option {symbol} {interval} {day_iso} due to validation failure")
            return
        # ===== /VALIDATION =====


        st = self._day_parts_status("option", symbol, interval, sink_lower, day_iso)

        # --- STATUS & BASE PATH (post tail-resume) ---
        base_path = st.get("base_path") or self._make_file_basepath(
            "option", symbol, interval, f"{day_iso}T00-00-00Z", sink_lower
        )
        

        # --- scrittura "normale" ---
        wrote = 0
        if sink_lower == "csv":
            df_out = self._format_dt_columns_isoz(df_all)
            print(f"[WRITE-CSV] About to write {len(df_out)} rows to base_path={base_path}")
            csv_text = df_out.to_csv(index=False)
            print(f"[WRITE-CSV] CSV text size: {len(csv_text)} bytes, first 200 chars: {csv_text[:200]}")
            await self._append_csv_text(base_path, csv_text, asset="option", interval=interval)
            print(f"[WRITE-CSV] _append_csv_text() completed")
            wrote = len(df_out)
        elif sink_lower == "parquet":
            wrote = self._append_parquet_df(base_path, df_all, asset="option", interval=interval)
        elif sink_lower == "influxdb":
            # >>> PATCH: log finestra prima di scrivere (INTRADAY)
            first_et = df_all["timestamp"].min() if "timestamp" in df_all.columns else None
            last_et  = df_all["timestamp"].max() if "timestamp" in df_all.columns else None
            print(f"[INFLUX][ABOUT-TO-WRITE] rows={len(df_all)} window_ET=({first_et},{last_et})")

            # InfluxDB write with verification and retry
            measurement = self._influx_measurement_from_base(base_path)
            influx_client = self._ensure_influx_client()

            # Define key columns for verification
            key_cols = ['__ts_utc']
            if 'symbol' in df_all.columns:
                key_cols.append('symbol')
            if 'expiration' in df_all.columns:
                key_cols.append('expiration')
            if 'strike' in df_all.columns:
                key_cols.append('strike')
            if 'right' in df_all.columns:
                key_cols.append('right')
            if interval == 'tick' and 'sequence' in df_all.columns:
                key_cols.append('sequence')

            df_influx = self._ensure_ts_utc_column(df_all)

            # Log SAVE_START for InfluxDB write
            self.logger.log_info(
                symbol=symbol,
                asset="option",
                interval=interval,
                date_range=(day_iso, day_iso),
                message=f"SAVE_START: Writing to InfluxDB, rows={len(df_influx)}",
                details={"sink": "influxdb", "rows": len(df_influx), "branch": "intraday", "measurement": measurement, "expirations_count": len(expirations)}
            )

            # InfluxDB write is synchronous - if no exception, write succeeded
            try:
                wrote = await self._append_influx_df(base_path, df_influx)
                write_success = True
            except Exception as e:
                wrote = 0
                write_success = False
                print(f"[ALERT] InfluxDB write failed for option {symbol} {interval} {day_iso}: {e}")
                self.logger.log_failure(
                    symbol=symbol,
                    asset="option",
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message=f"SAVE_FAILURE: InfluxDB write failed: {str(e)}",
                    details={"sink": "influxdb", "rows_attempted": len(df_all), "error": str(e), "branch": "intraday"}
                )
            if wrote == 0 and len(df_all) > 0:
                print("[ALERT] Influx ha scritto 0 punti a fronte di righe in input. "
                      "Potrebbe essere tutto NaN lato fields o un cutoff troppo aggressivo.")
                write_success = False

        else:
            raise ValueError(f"Unsupported sink: {sink_lower}")

        print(f"[SUMMARY] option {symbol} {interval} day={day_iso} rows={len(df_all)} wrote={wrote} sink={sink_lower}")

        # Check if write actually succeeded
        if sink_lower == "influxdb" and wrote == 0:
            print(f"[DOWNLOAD-FAILED] {symbol} option/{interval} day={day_iso} intraday branch FAILED - wrote 0 rows to InfluxDB")
            self.logger.log_failure(
                symbol=symbol,
                asset="option",
                interval=interval,
                date_range=(day_iso, day_iso),
                message=f"DOWNLOAD_FAILURE: Downloaded {len(df_all)} rows but wrote 0 to InfluxDB",
                details={"rows": len(df_all), "wrote": wrote, "sink": sink_lower, "branch": "intraday", "expirations_count": len(expirations), "reason": "influx_write_zero"}
            )
            raise RuntimeError(f"InfluxDB write failed: downloaded {len(df_all)} rows but wrote 0")
        else:
            print(f"[DOWNLOAD-COMPLETE] {symbol} option/{interval} day={day_iso} intraday branch completed successfully")
            self.logger.log_info(
                symbol=symbol,
                asset="option",
                interval=interval,
                date_range=(day_iso, day_iso),
                message=f"DOWNLOAD_SUCCESS: rows={len(df_all)} wrote={wrote}",
                details={"rows": len(df_all), "wrote": wrote, "sink": sink_lower, "branch": "intraday", "expirations_count": len(expirations)}
            )




    
    async def _download_and_store_equity_or_index(
        self,
        asset: str,
        symbol: str,
        interval: str,
        day_iso: str,
        sink: str,
        base_path_override: Optional[str] = None, 
        range_end_iso: Optional[str] = None,
    ) -> None:
        """
        Download and persist equity or index OHLC data for a single trading day or date range.

        This method handles both end-of-day (EOD) and intraday data downloads for stocks and indices,
        with intelligent resume capabilities and idempotent behavior. For EOD intervals, it can fetch
        multiple days in a single API call and append only new data. For intraday intervals, it performs
        overlap-safe appends by filtering out already-persisted timestamps.

        Parameters
        ----------
        asset : str
            Asset type: "stock" or "index".
        symbol : str
            Ticker symbol (e.g., "AAPL", "$SPX").
        interval : str
            Bar interval. Use "1d" for daily EOD data. For intraday, use intervals like
            "1m", "5m", "30m", "1h", or "tick" for tick-level data.
        day_iso : str
            Start date in ISO format "YYYY-MM-DD". For EOD, this is the beginning of the
            date range. For intraday, this is the single trading day to fetch.
        sink : str
            Output format: "csv", "parquet", or "influxdb".
        base_path_override : str, optional
            Default: None
            If provided, overrides the default file path construction.
        range_end_iso : str, optional
            Default: None (same as day_iso)
            For EOD intervals only: inclusive end date for the range. Allows fetching multiple
            days in a single API call. Automatically clamped to yesterday to avoid API errors
            on current-day requests.

        Returns
        -------
        None
            Data is written directly to disk (CSV/Parquet) or InfluxDB. No return value.

        Example Usage
        -------------
        # Called by public download_equity and download_index methods
        # Single-day intraday download:
        await manager._download_and_store_equity_or_index(
            asset="stock", symbol="AAPL", interval="5m",
            day_iso="2024-03-15", sink="parquet"
        )

        # Multi-day EOD batch download:
        await manager._download_and_store_equity_or_index(
            asset="stock", symbol="AAPL", interval="1d",
            day_iso="2024-01-01", sink="csv", range_end_iso="2024-03-15"
        )

        Notes
        -----
        - EOD behavior: Calls /stock/history/eod or /index/history/eod with date range.
          Automatically clamps end date to yesterday. Drops already-persisted days for idempotency.
        - Intraday behavior: Calls /stock/history/ohlc, /index/history/ohlc, or tick endpoints.
          Computes resume window based on last timestamp. Filters new data > last saved timestamp.
        - For InfluxDB: uses measurement naming "{prefix}{symbol}-{asset}-{interval}".
        - Uses client methods: stock_history_eod, stock_history_ohlc, stock_history_trade_quote,
          index_history_eod, index_history_ohlc, index_history_price.
        - Integrates with: _compute_intraday_window_et, _append_csv_text, _write_parquet_from_csv,
          _csv_has_day, _influx_day_has_any, _influx_last_ts_between, _touch_cache.
        """
    
        sink_lower = sink.lower()

        # >>> EARLY-SKIP (intraday only) <<<
        # Skip logic è ora gestita nel loop principale per uniformità
        # Non serve qui perché viene già controllato prima di chiamare questa funzione
    
        # -------------------------------
        # 1) FETCH from ThetaData
        # -------------------------------
        if interval == "1d":
            start_iso = day_iso
            end_iso = range_end_iso or day_iso  # inclusive end for EOD batch

            # clamp end to "yesterday" to avoid 500 on current day
            today_utc = dt.now(timezone.utc).date().isoformat()
            if end_iso >= today_utc:
                # se l'end cade oggi o nel futuro, retrocedi a ieri
                end_iso = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
                if end_iso < start_iso:
                    # niente da fare (range vuoto dopo il clamp)
                    return
                
            if asset == "stock":
                csv_txt, _ = await self.client.stock_history_eod(
                    symbol=symbol,
                    start_date=start_iso,  # "YYYY-MM-DD"
                    end_date=end_iso,      # "YYYY-MM-DD"
                    format_type="csv",
                )
            elif asset == "index":
                csv_txt, _ = await self.client.index_history_eod(
                    symbol=symbol,
                    start_date=start_iso,  # "YYYY-MM-DD"
                    end_date=end_iso,      # "YYYY-MM-DD"
                    format_type="csv",
                )
            else:
                raise ValueError(f"Unsupported asset for EOD: {asset}")
        else:
            # Calcola finestra resume (universale)
            bar_start_et = None
            if sink_lower == "influxdb":
                prefix = (self.cfg.influx_measure_prefix or "")
                meas = f"{prefix}{symbol}-{asset}-{interval}"
                
                day_start_utc = pd.Timestamp(f"{day_iso}T00:00:00", tz="America/New_York").tz_convert("UTC")
                day_end_utc = (pd.Timestamp(f"{day_iso}T00:00:00", tz="America/New_York") + pd.Timedelta(days=1)).tz_convert("UTC")
                
                try:
                    has_any = self._influx_day_has_any(meas, day_iso)
                    if has_any is False:
                        bar_start_et = None
                    else:
                        last_in_day = self._influx_last_ts_between(meas, day_start_utc, day_end_utc)
                        if last_in_day is not None:
                            last_et = last_in_day.tz_convert("America/New_York")
                            ov = int(getattr(self.cfg, "overlap_seconds", 0) or 0)
                            bar_start_et = (last_et - pd.Timedelta(seconds=ov)).strftime("%H:%M:%S")
                except Exception:
                    pass
            
            elif sink_lower in ("csv", "parquet"):
                bar_start_et, _ = self._compute_intraday_window_et(
                    asset, symbol, interval, sink_lower, day_iso
                )
            
            # Intraday: if interval == "tick" use *tick* endpoints, otherwise OHLC bars
            if interval == "tick":
                if asset == "stock":
                    # Trade+Quote tick pairing for stocks (v3)
                    # /stock/history/trade_quote  date=YYYYMMDD
                    ymd = self._td_ymd(day_iso)
                    csv_txt, _ = await self.client.stock_history_trade_quote(
                        symbol=symbol,
                        date=ymd,
                        start_time=None,
                        end_time=None,
                        format_type="csv",
                    )  # stock tick T+Q, v3. :contentReference[oaicite:0]{index=0}
                elif asset == "index":
                    # Index price ticks (v3). L’endpoint richiede un 'interval'; mappiamo 'tick' -> '1s'
                    ymd = self._td_ymd(day_iso)
                    csv_txt, _ = await self.client.index_history_price(
                        symbol=symbol,
                        date=ymd,
                        interval="1s",
                        start_time=None,
                        end_time=None,
                        format_type="csv",
                    )  # index price ticks, v3. :contentReference[oaicite:1]{index=1}
                else:
                    raise ValueError(f"Unsupported asset for tick: {asset}")
            else:
                # Intraday OHLC: stock vs index signatures differ
                if asset == "stock":
                    # stock OHLC accepts YYYY-MM-DD for 'date'
                    csv_txt, _ = await self.client.stock_history_ohlc(
                        symbol=symbol,
                        date=day_iso,       # "YYYY-MM-DD"
                        interval=interval,
                        start_time=bar_start_et,
                        end_time=None,
                        format_type="csv",
                    )  # :contentReference[oaicite:2]{index=2}
                elif asset == "index":
                    # index OHLC uses YYYYMMDD range
                    ymd = self._td_ymd(day_iso)
                    csv_txt, _ = await self.client.index_history_ohlc(
                        symbol=symbol,
                        start_date=ymd,     # "YYYYMMDD"
                        end_date=ymd,       # "YYYYMMDD"
                        interval=interval,
                        sstart_time=bar_start_et,
                        end_time=None,
                        format_type="csv",
                    )  # :contentReference[oaicite:3]{index=3}
                else:
                    raise ValueError(f"Unsupported asset for intraday: {asset}")

    
        # Nothing to do if empty payload
        if not csv_txt:
            return
    
        # -------------------------------
        # 2) PARSE
        # -------------------------------
        # Parse raw text as strings to avoid pandas auto date parsing errors on mixed ISO formats
        df_day = pd.read_csv(io.StringIO(csv_txt), dtype=str)
        if df_day is None or df_day.empty:
            return

        # Normalize timestamps to UTC (naive) right after download
        df_day = self._normalize_ts_to_utc(df_day)

        # -------------------------------
        # 3) RESOLVE SERIES BASE PATH
        # -------------------------------
        if base_path_override:
            base_path = base_path_override
        else:
            existing = self._find_existing_series_base(asset, symbol, interval, sink_lower)
            if existing:
                base_path = existing
            else:
                # First write for the series: derive from minimum timestamp present
                series_start_iso = self._min_ts_from_df(df_day) or f"{day_iso}T00-00-00Z"
                base_path = self._make_file_basepath(asset, symbol, interval, series_start_iso, sink_lower)
    
        # -------------------------------
        # 4) EOD IDEMPOTENCY: DROP DAYS ALREADY ON DISK (handles multi-day ranges)
        # -------------------------------
        if interval == "1d":
            # Pick a date/time column in order of preference
            # Prefer last_trade (actual trading date) over created (processing timestamp)
            tcol = None
            for c in ("last_trade", "created", "timestamp", "date"):
                if c in df_day.columns:
                    tcol = c
                    break

            # Build list of 'YYYY-MM-DD' for each row
            if tcol in ("last_trade", "created", "timestamp"):
                # Parse timestamps - pandas handles mixed ISO8601 formats correctly (pandas 2.x strict)
                # ThetaData API returns: '2024-01-29T17:10:35.602' and '2024-02-01T16:46:45'
                ts = pd.to_datetime(df_day[tcol], utc=True, errors='coerce')
                day_list = ts.dt.date.astype(str)
            elif tcol == "date":
                # If 'date' exists but not parsed, coerce to string 'YYYY-MM-DD'
                day_list = pd.to_datetime(df_day["date"], utc=True, errors='coerce').dt.date.astype(str)
            else:
                # Fallback: treat as single day (day_iso) if no recognizable column
                day_list = pd.Series([day_iso] * len(df_day), dtype=str)
    
            keep_mask = []
            # For each row, keep it only if that day is NOT already present on disk
            for d in day_list:
                keep_mask.append(not self._csv_has_day(base_path, d))
    
            if keep_mask:
                df_day = df_day.loc[keep_mask]
    
            if df_day.empty:
                # Entire batch already present -> nothing to append
                return
    
            # Rebuild CSV from filtered rows (applies to both CSV and Parquet sinks)
            csv_txt = df_day.to_csv(index=False)
    
        # -------------------------------
        # 5) INTRADAY CSV OVERLAP FILTER (timestamp > last saved) + BOUNDARY DEDUP
        # -------------------------------
        if interval != "1d" and sink_lower == "csv":
            # Determine existing series file
            target_path = base_path if os.path.exists(base_path) else self._find_existing_series_base(
                asset, symbol, interval, sink_lower
            )
            tcol = "timestamp" if "timestamp" in df_day.columns else ("created" if "created" in df_day.columns else None)
            if target_path and os.path.exists(target_path) and tcol:
                last_iso = self._last_csv_timestamp(target_path)
                if last_iso:
                    ts = pd.to_datetime(df_day[tcol], errors="coerce", utc=True)
                    cutoff = pd.to_datetime(last_iso.replace("Z", "+00:00"), errors="coerce", utc=True)
                    df_day = df_day.loc[ts > cutoff]
                    if df_day.empty:
                        return

                    # >>> BOUNDARY DEDUP: leggi ultime 100 righe e rimuovi duplicati su timestamp
                    try:
                        last_chunk = pd.read_csv(target_path, dtype=str).tail(100)
                        if tcol in last_chunk.columns:
                            existing_ts = set(pd.to_datetime(last_chunk[tcol], errors="coerce", utc=True).dropna())
                            if existing_ts:
                                new_ts = pd.to_datetime(df_day[tcol], errors="coerce", utc=True)
                                before = len(df_day)
                                df_day = df_day[~new_ts.isin(existing_ts)]
                                removed = before - len(df_day)
                                if removed > 0:
                                    print(f"[INTRADAY-CSV][BOUNDARY-DEDUP] removed {removed} stock/index duplicates")
                    except Exception as e:
                        print(f"[INTRADAY-CSV][WARN] boundary dedup failed: {e}")
                    
                    # Rebuild CSV from filtered rows
                    csv_txt = df_day.to_csv(index=False)

        # -------------------------------
        # 5b) HARD DEDUPE (UNIVERSALE stock/index intraday)
        # -------------------------------
        if interval != "1d":
            tcol = "timestamp" if "timestamp" in df_day.columns else (
                "created" if "created" in df_day.columns else None
            )
            if tcol:
                # Normalizza timestamp (ET-naive)
                ts = pd.to_datetime(df_day[tcol], errors="coerce", utc=True)
                ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
                df_day[tcol] = ts

                dedupe_cols = [tcol]
                if interval == "tick" and "sequence" in df_day.columns:
                    dedupe_cols.append("sequence")
                
                df_day = df_day.dropna(subset=dedupe_cols).drop_duplicates(subset=dedupe_cols, keep="last")

                # Rebuild CSV
                if sink_lower in ("csv", "parquet"):
                    csv_txt = df_day.to_csv(index=False)

        # ===== VALIDATION (Real-Time) =====
        # Validate data before persisting
        validation_ok = await self._validate_downloaded_data(
            df=df_day,
            asset=asset,
            symbol=symbol,
            interval=interval,
            day_iso=day_iso,
            sink=sink,
            enrich_greeks=False,  # Stocks/indices don't have Greeks enrichment
            enrich_tick_greeks=False
        )

        if not validation_ok:
            # Validation failed in strict mode - do not save
            print(f"[VALIDATION] STRICT MODE: Skipping save for {asset} {symbol} {interval} {day_iso} due to validation failure")
            return
        # ===== /VALIDATION =====

        # -------------------------------
        # 6) PERSIST
        # -------------------------------
        wrote = 0
        if sink_lower == "csv":
            df_out = self._format_dt_columns_isoz(df_day)
            await self._append_csv_text(base_path, df_out.to_csv(index=False), asset=asset, interval=interval)
            wrote = len(df_out)
        elif sink_lower == "parquet":
            wrote = self._append_parquet_df(base_path, df_day, asset=asset, interval=interval)
        elif sink_lower == "influxdb":
            # InfluxDB write with verification and retry
            measurement = self._influx_measurement_from_base(base_path)
            influx_client = self._ensure_influx_client()

            # Define key columns for verification (stock/index have simpler keys than options)
            key_cols = ['__ts_utc']
            if 'symbol' in df_day.columns:
                key_cols.append('symbol')
            if interval == 'tick' and 'sequence' in df_day.columns:
                key_cols.append('sequence')

            df_influx = self._ensure_ts_utc_column(df_day)

            # Log SAVE_START for InfluxDB write
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(day_iso, day_iso),
                message=f"SAVE_START: Writing to InfluxDB, rows={len(df_influx)}",
                details={"sink": "influxdb", "rows": len(df_influx), "branch": f"{asset}", "measurement": measurement}
            )

            # InfluxDB write is synchronous - if no exception, write succeeded
            try:
                wrote = await self._append_influx_df(base_path, df_influx)
                write_success = True
            except Exception as e:
                wrote = 0
                write_success = False
                print(f"[ALERT] InfluxDB write failed for {asset} {symbol} {interval} {day_iso}: {e}")
                self.logger.log_failure(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=(day_iso, day_iso),
                    message=f"SAVE_FAILURE: InfluxDB write failed: {str(e)}",
                    details={"sink": "influxdb", "rows_attempted": len(df_day), "error": str(e), "branch": f"{asset}"}
                )
        else:
            raise ValueError(f"Unsupported sink: {sink_lower}")

            print(f"[SUMMARY] {asset} {symbol} {interval} day={day_iso} rows={len(df_day)} wrote={wrote} sink={sink_lower}")
            # Log INFO write summary
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(day_iso, day_iso),
                message="WRITE_OK",
                details={"rows": len(df_day), "wrote": wrote, "sink": sink_lower}
            )

        
        # -------------------------------
        # 7) UPDATE COVERAGE CACHE
        # -------------------------------
        # Determine first/last day in the df we just wrote
        # (handles both EOD multi-day batch and intraday)
        
        first_day_written, last_day_written = self._extract_days_from_df(df_day, day_iso)
        self._touch_cache(asset, symbol, interval, sink, first_day=first_day_written, last_day=last_day_written)

        
        



            

    # ---------------------- START DATE RESOLUTION --------------------


    async def _expirations_that_traded(self, symbol: str, day_iso: str, req_type: str = "trade") -> list[str]:
        """Fetches and caches the list of option expiration dates that had activity on a specific trading day.

        This method queries the ThetaData API for all option contracts that traded or were quoted on the given
        day, extracts their expiration dates, and caches the result for reuse. It handles multiple response
        formats from the API including columnar dictionaries and lists of contract objects.

        Parameters
        ----------
        symbol : str
            The underlying ticker root symbol (e.g., 'SPY', 'AAPL').
        day_iso : str
            The trading day in 'YYYY-MM-DD' format to query for active expirations.
        req_type : str, optional
            Default: "trade"
            Possible values: ["trade", "quote"]

            The type of activity to query - either trades or quotes.

        Returns
        -------
        list of str
            A list of expiration dates in 'YYYYMMDD' format that had activity on the specified day.
            Returns an empty list if no contracts traded/quoted or if the API call fails.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _download_and_store_options() to determine which expirations to download for a given day
        # - First-date discovery methods to identify which expirations were active
        """
        cache_key = (symbol, day_iso, req_type)
        if cache_key in self._exp_by_day_cache:
            return self._exp_by_day_cache[cache_key]
    
        day_ymd = self._td_ymd(day_iso)
        try:
            payload, _ = await self.client.option_list_contracts(
                request_type=req_type, date=day_ymd, symbol=symbol, format_type="json"
            )
        except Exception:
            self._exp_by_day_cache[cache_key] = []
            return []
    
        def _norm8(s: str) -> str | None:
            s = (s or "").replace("-", "")
            if len(s) == 8 and s.isdigit():
                return s
            if len(s) == 6 and s.isdigit():  # YYMMDD -> YYYYMMDD
                yy = int(s[:2]); year = 2000 + yy
                return f"{year:04d}{s[2:4]}{s[4:6]}"
            return None
    
        exps: set[str] = set()
    
        # --- Fast path: COLUMNAR dict-of-arrays ---
        if isinstance(payload, dict):
            # Common column names used by ThetaData for this endpoint
            for col in ("expiration", "expirationDate", "exp"):
                colv = payload.get(col)
                if isinstance(colv, list) and colv:
                    for e in colv:
                        e8 = _norm8(str(e))
                        if e8:
                            exps.add(e8)
            if exps:
                out = sorted(exps)
                self._exp_by_day_cache[cache_key] = out
                # debug opzionale
                # print(f"[INFO] {symbol} {day_iso} ({req_type}) → {len(out)} expirations (columnar)")
                return out
    
        # --- Fallback: LISTA di dict ---
        def _dig_to_list(x):
            if isinstance(x, list):
                return x
            if isinstance(x, dict):
                for k in ("contracts", "items", "results", "data"):
                    if k in x:
                        r = _dig_to_list(x[k])
                        if r:
                            return r
            return []
    
        items = _dig_to_list(payload) or []
        for it in items:
            if not isinstance(it, dict):
                continue
            exp = it.get("expiration") or it.get("expirationDate") or it.get("exp")
            if not exp:
                # parse da OCC-style option symbol, se presente
                sym = it.get("option_symbol") or it.get("symbol") or it.get("contractSymbol") or it.get("s")
                if isinstance(sym, str):
                    m8 = re.search(r"(\d{8})[CP]", sym)
                    m6 = re.search(r"(\d{6})[CP]", sym)
                    if m8:
                        exp = m8.group(1)
                    elif m6:
                        exp = m6.group(1)
            e8 = _norm8(str(exp)) if exp else None
            if e8:
                exps.add(e8)
    
        out = sorted(exps)
        self._exp_by_day_cache[cache_key] = out
    
        if not out:
            # Qui ora vedrai la chiave 'expiration' quando è columnar (come nel tuo file)
            print(f"[DEBUG] No expirations parsed for {symbol} {day_iso} ({req_type}). "
                  f"Payload keys sample: {list(payload.keys()) if isinstance(payload, dict) else type(payload)}")
    
        return out



    async def _fetch_option_all_greeks_by_date(
        self,
        symbol: str,
        day_iso: str,
        interval: str,
        strike: str = "*",
        right: str = "both",
        rate_type: str = "sofr",
        annual_dividend: Optional[float] = None,
        rate_value: Optional[float] = None,
        expiration: str = "*",           # <— aggiunto: "*" per tutta la chain
        fmt: str = "csv",
    ) -> Tuple[str, str]:
        """Fetches historical Greeks data for options from the ThetaData API for a single trading day.

        This method retrieves Greeks (delta, gamma, theta, vega, rho, etc.) for option contracts on a
        specific day using the /option/history/greeks/all endpoint. It supports filtering by strike,
        right (call/put), and expiration, with wildcard support for fetching entire chains.

        Parameters
        ----------
        symbol : str
            The underlying ticker root symbol (e.g., 'SPY', 'AAPL').
        day_iso : str
            The trading day in 'YYYY-MM-DD' format.
        interval : str
            The bar interval for Greeks data (e.g., '1d', '5m', '1h').
        strike : str, optional
            Default: "*"

            The strike price filter. Use "*" for all strikes or specify a specific strike.
        right : str, optional
            Default: "both"
            Possible values: ["call", "put", "both"]

            Filter for call options, put options, or both.
        rate_type : str, optional
            Default: "sofr"

            The interest rate type to use for Greeks calculations.
        annual_dividend : float or None, optional
            Default: None

            Annual dividend amount for the underlying. If None, the API uses its default.
        rate_value : float or None, optional
            Default: None

            Specific interest rate value. If None, the API uses current market rates.
        expiration : str, optional
            Default: "*"

            Expiration date filter in 'YYYYMMDD' format, or "*" for all expirations.
        fmt : str, optional
            Default: "csv"

            Response format from the API ('csv' or 'json').

        Returns
        -------
        tuple of (str, str)
            A tuple containing (response_text, full_url). The response_text is the raw CSV or JSON data
            from the API, and full_url is the complete request URL for debugging.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _download_and_store_options() when enrich_greeks=True to fetch companion Greeks data
        # - Methods that need historical Greeks enrichment for options analysis
        """
        ymd = self._td_ymd(day_iso)
        # Use conditional kwargs to avoid passing None (causes total_seconds error in SDK)
        kwargs = {
            "symbol": symbol,
            "expiration": expiration,
            "date": ymd,
            "interval": interval,
            "strike": strike,
            "right": right,
            "rate_type": rate_type,
            "format_type": fmt,
        }
        if annual_dividend is not None:
            kwargs["annual_dividend"] = annual_dividend
        if rate_value is not None:
            kwargs["rate_value"] = rate_value
        greeks_version = getattr(self.cfg, "greeks_version", None)
        if greeks_version is not None:
            greeks_version = str(greeks_version).strip() or None
            if greeks_version is not None:
                kwargs["greeks_version"] = greeks_version

        return await self.client.option_history_all_greeks(**kwargs)




    async def _td_get_with_retry(self, coro_factory, label: str, retries: int = 1):
        """
        Esegue la richiesta TD con 1 piccolo retry se fallisce per errori transitori
        (timeout, disconnessione, cancel durante I/O). Ritorna (text, meta) o (None, None).
        """
    
        for attempt in range(retries + 1):
            try:
                return await coro_factory()
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                print(f"[TD-HTTP][RETRY] {label} attempt={attempt+1} error={e}")
                if attempt >= retries:
                    print(f"[TD-HTTP][GIVEUP] {label}")
                    return None, None
                await asyncio.sleep(0.75)
            except asyncio.CancelledError as e:
                # a volte arriva da stream/flight; proviamo un solo retry "soft"
                print(f"[TD-HTTP][CANCELLED] {label} attempt={attempt+1}")
                if attempt >= retries:
                    print(f"[TD-HTTP][GIVEUP] {label} cancelled")
                    return None, None
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"[TD-HTTP][ERROR] {label}: {e}")
                return None, None


    # ──────────────────────────────────────────────────────────────────────────
    # DUPLICATE CHECK UTILITIES
    # ──────────────────────────────────────────────────────────────────────────
    

    async def _resolve_first_date(self, task: Task, symbol: str) -> Optional[str]:
        """Resolve the initial coverage date for this (asset, symbol) according to policy."""
        pol = task.discover_policy or DiscoverPolicy()
        req_type = pol.request_type or self.cfg.discovery_request_type

        if task.first_date_override:
            return self._normalize_date_str(task.first_date_override)

        # Both 'skip' and 'mild_skip' bypass discovery and rely on resume/overrides.
        if pol.mode in ("skip", "mild_skip"):
            return None


        cached = self._get_cached_first_date(task.asset, symbol, req_type)
        if pol.mode == "auto" and cached:
            return cached

        if task.asset in ("stock", "index"):
            fd = await self._discover_equity_first_date(symbol, req_type=req_type, asset=task.asset)
        elif task.asset == "option":
            k = pol.check_first_k_expirations if pol.check_first_k_expirations is not None else self.cfg.option_check_first_k_expirations
            fd = await self._discover_option_first_date(
                symbol,
                req_type=req_type,
                check_first_k=max(1, int(k) if isinstance(k, (int, str)) and str(k).isdigit() else 1),
                fallback_binary=pol.enable_option_binary_fallback,
                bin_start=pol.binary_search_start,
                bin_end=pol.binary_search_end,
            )
        else:
            fd = None

        if fd:
            self._set_cached_first_date(task.asset, symbol, req_type, fd)
        return fd



        

    async def _discover_equity_first_date(
        self,
        symbol: str,
        req_type: str = "trade",
        asset: str = "stock",
    ) -> Optional[str]:
        """
        Discover the earliest available date for stock or index data using ThetaData list endpoints.

        This method queries the appropriate endpoint based on asset type and data type to find the
        first date for which data is available. It handles the semantic differences between stock
        and index data types (stocks have "trade"/"quote", indices have "price"/"ohlc").

        Parameters
        ----------
        symbol : str
            Ticker symbol (e.g., "AAPL" for stocks, "$SPX" for indices).
        req_type : str, optional
            Default: "trade"
            Possible values: ["trade", "quote", "price", "ohlc"]
            Type of data to query. For stocks: "trade" or "quote". For indices: "price" or "ohlc".
            If "trade"/"quote" is specified for an index, automatically maps to "price".
        asset : str, optional
            Default: "stock"
            Possible values: ["stock", "index"]
            Asset class. Determines which API endpoint to use.

        Returns
        -------
        str or None
            Earliest available date in ISO format "YYYY-MM-DD", or None if no data is available
            or the API request fails.

        Example Usage
        -------------
        # Called by download_equity and download_index when determining date range
        first_date = await manager._discover_equity_first_date(
            symbol="AAPL", req_type="trade", asset="stock"
        )
        # Returns: "2010-01-04" (or earliest available date)

        # For index data:
        first_date = await manager._discover_equity_first_date(
            symbol="$SPX", req_type="price", asset="index"
        )

        Notes
        -----
        - For stocks: uses /stock/list/dates endpoint with data_type="trade" or "quote".
        - For indices: uses /index/list/dates endpoint with data_type="price" or "ohlc".
        - Automatically maps "trade"/"quote" → "price" when asset is "index" since indices
          don't have trade/quote semantics.
        - Uses helper methods: _ensure_list, _extract_first_date_from_any for robust JSON parsing.
        - Searches nested JSON keys: ("data", "results", "items", "dates").
        """
        # Normalizza l'asset
        asset = (asset or "stock").lower()
        if asset not in ("stock", "index"):
            asset = "stock"
    
        # Mappa req_type in base all'asset
        if asset == "stock":
            data_type = "trade" if req_type not in ("trade", "quote") else req_type
            data, _ = await self.client.stock_list_dates(
                symbol=symbol,
                data_type=data_type,
                format_type="json",
            )
        else:  # asset == "index"
            # Gli indici espongono "price"/"ohlc": se ci arriva "trade"/"quote", usa "price"
            if req_type in ("price", "ohlc"):
                idx_type = req_type
            else:
                idx_type = "price"
            data, _ = await self.client.index_list_dates(
                symbol=symbol,
                data_type=idx_type,
                format_type="json",
            )
    
        # Estrarre la lista di date con i tuoi helper robusti
        dlist = self._ensure_list(data, keys=("data", "results", "items", "dates"))
        return self._extract_first_date_from_any(dlist)

    async def _discover_option_first_date(
        self,
        symbol: str,
        req_type: str = "trade",
        check_first_k: int = 3,
        fallback_binary: bool = True,
        bin_start: str = "2010-01-01",
        bin_end: Optional[str] = None,
    ) -> Optional[str]:
        """
        Discover the earliest available date for option data for a given underlying symbol.

        This method uses a multi-strategy approach: first checking the oldest expirations via
        /option/list/dates, then falling back to binary search via /option/list/contracts if
        needed. The approach avoids HTTP 414 errors by querying one expiration at a time.

        Parameters
        ----------
        symbol : str
            Underlying root symbol (e.g., "AAPL", "SPX").
        req_type : str, optional
            Default: "trade"
            Possible values: ["trade", "quote"]
            Type of option data to query.
        check_first_k : int, optional
            Default: 3
            Number of oldest expirations to check via /option/list/dates. Checking multiple
            expirations increases reliability but uses more API calls.
        fallback_binary : bool, optional
            Default: True
            If True and no dates are found via expiration checking, performs a binary search
            over the date range [bin_start, bin_end] using /option/list/contracts.
        bin_start : str, optional
            Default: "2010-01-01"
            Start date for binary search fallback (ISO format "YYYY-MM-DD").
        bin_end : str, optional
            Default: None (uses current date)
            End date for binary search fallback (ISO format "YYYY-MM-DD").

        Returns
        -------
        str or None
            Earliest available date in ISO format "YYYY-MM-DD", or None if no data is found.

        Example Usage
        -------------
        # Called by download_options when determining historical start date
        first_date = await manager._discover_option_first_date(
            symbol="AAPL", req_type="trade", check_first_k=3
        )
        # Returns: "2010-01-04" (or earliest date with option data)

        # With custom binary search range:
        first_date = await manager._discover_option_first_date(
            symbol="SPY", req_type="trade",
            fallback_binary=True, bin_start="2015-01-01"
        )

        Notes
        -----
        Strategy:
        1. Fetches all expirations via /option/list/expirations?symbol=ROOT.
        2. For the K oldest expirations, queries /option/list/dates/{req_type} with ONE
           expiration per request (strike="*", right="both") to avoid HTTP 414 errors.
        3. Returns the minimum date found across checked expirations.
        4. If no dates found and fallback_binary=True, performs binary search using
           /option/list/contracts/{req_type}?date=YYYYMMDD&symbol=ROOT.

        - Uses expiration cache (_exp_cache) to avoid repeated API calls.
        - Helper methods: _ensure_list, _extract_expirations_as_dates, _extract_first_date_from_any.
        - Fallback method: _binary_search_first_date_option.
        """
        # 1) expirations
        if symbol in self._exp_cache:
            exps_list = self._exp_cache[symbol]
        else:
            exps, _ = await self.client.option_list_expirations(symbol=symbol, format_type="json")
            exps_list = self._ensure_list(exps, keys=("data", "results", "items", "expirations"))
            self._exp_cache[symbol] = exps_list

        exp_dates = sorted(dict.fromkeys(self._extract_expirations_as_dates(exps_list)))
        if not exp_dates:
            return None

        # 2) dates per-expiration (ONE expiration per request => avoids 414)
        first_dates: List[str] = []
        for exp_iso in exp_dates[:check_first_k]:
            exp_iso = self._normalize_date_str(exp_iso) or ""
            if not exp_iso:
                continue
            exp_param = exp_iso.replace("-", "")
            if not (len(exp_param) == 8 and exp_param.isdigit()):
                continue
                
            payload, _ = await self.client.option_list_dates(
                symbol=symbol,
                request_type=req_type,
                expiration=exp_param,
                strike="*",
                right="both",
                format_type="json",
            )
            
            dlist = self._ensure_list(payload, keys=("data", "results", "items", "dates"))
            f = self._extract_first_date_from_any(dlist)
            if f:
                first_dates.append(f)

        if first_dates:
            return min(first_dates)

        # 3) binary fallback via contracts listing
        if fallback_binary:
            if not bin_end:
                bin_end = datetime.now(timezone.utc).date().isoformat()
            return await self._binary_search_first_date_option(symbol, req_type, bin_start, bin_end)

        return None

    async def _binary_search_first_date_option(self, symbol: str, req_type: str,
                                               start_date: str, end_date: str) -> Optional[str]:
        """
        Perform binary search to find the earliest date with option contract data.

        This method is used as a fallback when standard expiration-based discovery fails. It uses
        /option/list/contracts endpoint to check for data availability on specific dates, narrowing
        the search range via binary search until the first date with data is found.

        Parameters
        ----------
        symbol : str
            Underlying root symbol (e.g., "AAPL", "SPX").
        req_type : str
            Type of data to check: "trade" or "quote".
        start_date : str
            Beginning of search range in ISO format "YYYY-MM-DD".
        end_date : str
            End of search range in ISO format "YYYY-MM-DD".

        Returns
        -------
        str or None
            Earliest date in ISO format "YYYY-MM-DD" with option data, or None if no data
            is found in the range.

        Example Usage
        -------------
        # Called internally by _discover_option_first_date as fallback
        first_date = await manager._binary_search_first_date_option(
            symbol="AAPL", req_type="trade",
            start_date="2010-01-01", end_date="2024-12-31"
        )

        Notes
        -----
        - Uses /option/list/contracts endpoint to check data availability for each candidate date.
        - Implements classic binary search algorithm: O(log N) where N is days in range.
        - Returns the first date (earliest) where has_data_async returns True.
        - Automatically swaps start/end if provided in wrong order.
        """
        sd = dt.fromisoformat(start_date).date()
        ed = dt.fromisoformat(end_date).date()
        if sd > ed:
            sd, ed = ed, sd

        async def has_data_async(d: date) -> bool:
            ymd = d.strftime("%Y%m%d")
            try:
                payload, _ = await self.client.option_list_contracts(
                    request_type=req_type, date=ymd, symbol=symbol, format_type="json"
                )
                data = self._ensure_list(payload, keys=("data", "results", "items", "contracts"))
                return bool(data)
            except Exception:
                return False

        lo, hi = sd, ed
        found: Optional[date] = None
        while lo <= hi:
            mid = lo + (hi - lo) // 2
            if await has_data_async(mid):
                found = mid
                hi = mid - timedelta(days=1)
            else:
                lo = mid + timedelta(days=1)
        return found.isoformat() if found else None

    # ------------------------- RESUME LOGIC --------------------------

             


    # -------------------------- HELPERS -------------------------

    def _extract_first_date_from_any(self, seq: List[Any]) -> Optional[str]:
        dates: List[str] = []
        for x in seq:
            if isinstance(x, str):
                ds = self._normalize_date_str(x)
                if ds:
                    dates.append(ds)
            elif isinstance(x, dict):
                for k in ("date", "Date", "trade_date", "tradingDay"):
                    if k in x and x[k]:
                        ds = self._normalize_date_str(str(x[k]))
                        if ds:
                            dates.append(ds)
                        break
        return min(dates) if dates else None

    def _extract_expirations_as_dates(self, seq: List[Any]) -> List[str]:
        out: List[str] = []
        for x in seq:
            if isinstance(x, str):
                ds = self._normalize_date_str(x)
                if ds:
                    out.append(ds)
            elif isinstance(x, dict):
                val = x.get("expiration") or x.get("date") or x.get("expirationDate")
                if val:
                    ds = self._normalize_date_str(str(val))
                    if ds:
                        out.append(ds)
        out.sort()
        return out


    # ------------- SCREENING FUNCTIONS -------------------------------



    async def _compute_resume_start_datetime(self, task: Task, symbol: str, interval: str, first_date: Optional[str]) -> datetime:
        """
        Decide the starting datetime for a task, honoring:
          - existing data (resume),
          - daily-file series for OPTION intraday (EOD-like filenames),
          - user-provided backfill (retrograde) first_date,
          - overlap_seconds for non-1d intervals.
        """
        last_ts = None
        last_path = None
    
        # 1) Probe existing "last timestamp" using the original source-specific logic
        if not getattr(task, "ignore_existing", False):
            res = self._probe_existing_last_ts_with_source(task, symbol, interval, task.sink.lower())
            if inspect.isawaitable(res):
                last_ts, last_path = await res
            else:
                last_ts, last_path = res
    
            # Option intraday file-based resume logic
            try:
                if getattr(task, "asset", None) == "option" and interval != "1d":
                    sink_lower = task.sink.lower()
                    resume_pinned = False
    
                    series_first_iso, series_last_iso = self._get_first_last_day_from_sink(
                        task.asset, symbol, interval, sink_lower
                    )
                    series_files = []
                    if sink_lower in ("csv", "parquet"):
                        series_files = self._list_series_files(task.asset, symbol, interval, sink_lower)
    
                    # Check for incomplete days
                    try:
                        if series_first_iso and series_last_iso:
                            cur = dt.fromisoformat(series_first_iso)
                            end = dt.fromisoformat(series_last_iso)
                            forced_day = None
                            while cur <= end:
                                di = cur.date().isoformat()
                                st = self._day_parts_status(task.asset, symbol, interval, sink_lower, di)
                                if st.get("missing") or st.get("has_mixed"):
                                    forced_day = di
                                    resume_pinned = True
                                    break
                                cur += timedelta(days=1)
                            if forced_day:
                                forced_day_iso = forced_day
                        else:
                            forced_day_iso = None
                    except Exception:
                        pass
    
                    # Normalize user first_date
                    user_fd = None
                    if first_date:
                        if isinstance(first_date, (list, tuple)):
                            first_date = self._extract_first_date_from_any(list(first_date))
                        elif isinstance(first_date, dict):
                            first_date = self._extract_first_date_from_any([first_date])
                        if isinstance(first_date, str):
                            user_fd = self._normalize_date_str(first_date)
    
                    # **FIX: Se user chiede retrogrado, NON sovrascrivere con series_last**
                    if not (user_fd and series_first_iso and user_fd < series_first_iso):
                        if (not resume_pinned) and series_last_iso:
                            next_day = (dt.fromisoformat(series_last_iso).date() + timedelta(days=1))
                            last_ts = _dt.combine(next_day, _dt.min.time(), tzinfo=_tz.utc)
                            try:
                                last_path = None
                                for p in reversed(series_files):
                                    if os.path.basename(p).startswith(series_last_iso):
                                        last_path = p
                                        break
                            except Exception:
                                last_path = None
            except Exception:
                pass
    
        # 2) Compute start_dt from last_ts / first_date / defaults
        candidates = []
        
        # 2.a) Resume anchor from last_ts (with overlap only for intraday)
        if last_ts:
            if interval == "1d":
                nd = (last_ts.astimezone(timezone.utc).date() + timedelta(days=1))
                candidates.append(dt.combine(nd, dt.min.time(), tzinfo=timezone.utc))
            else:
                # >>> BUCKET ALIGNMENT FOR M/H FRAMES <
                overlap = max(0, self.cfg.overlap_seconds)
                resume_dt = last_ts - timedelta(seconds=overlap)
                
                iv = (interval or "").strip().lower()
                if iv.endswith("m") or iv.endswith("h"):
                    m = int(iv[:-1]) * (60 if iv.endswith("h") else 1)
                    # Converti a ET-aware, floor, torna a UTC
                    resume_et = resume_dt.astimezone(self.ET)
                    floored_utc = self._floor_to_interval_et(resume_et, m)
                    resume_dt = floored_utc
                
                candidates.append(resume_dt)
        
        # 2.b) User first_date (midnight UTC) - **SEMPRE prioritario se presente**
        if first_date:
            if isinstance(first_date, (list, tuple)):
                first_date = self._extract_first_date_from_any(list(first_date))
            elif isinstance(first_date, dict):
                first_date = self._extract_first_date_from_any([first_date])
            if isinstance(first_date, str):
                first_date = self._normalize_date_str(first_date)
            if first_date:
                fd_utc = self._as_utc(f"{first_date}T00:00:00Z")
                candidates.append(fd_utc)
        
        # 2.c) forced_day (if present)
        try:
            if forced_day_iso:
                candidates.append(self._as_utc(f"{forced_day_iso}T00:00:00Z"))
        except NameError:
            pass
        
        # **FIX: Prendiamo SEMPRE il MIN dei candidates (retrogrado prioritario)**
        if candidates:
            start_dt = min(candidates)
        else:
            start_dt = dt.now(timezone.utc) - timedelta(days=1)
    
        # Debug log
        try:
            requested_start_input = first_date
            requested_from_task = (
                getattr(task, "first_date_override", None)
                or getattr(task, "first_date", None)
                or getattr(task, "start_date", None)
            )
            requested_start_s = str(requested_start_input) if requested_start_input is not None else "None"
            requested_task_s  = str(requested_from_task)   if requested_from_task  is not None else "None"
    
            latest_file_s = last_path or "None"
            last_saved_s = last_ts.isoformat() if last_ts else "None"
    
            should_start_dt = None
            if last_ts is not None:
                if interval == "1d":
                    nd = (last_ts.astimezone(timezone.utc).date() + timedelta(days=1))
                    should_start_dt = dt.combine(nd, dt.min.time(), tzinfo=timezone.utc)
                else:
                    should_start_dt = last_ts - timedelta(seconds=max(0, self.cfg.overlap_seconds))
            should_start_s = should_start_dt.isoformat() if should_start_dt else "None"
    
            print(f"[RESUME-DEBUG] asset={getattr(task,'asset',None)} symbol={symbol} interval={interval} sink={task.sink.lower()}")
            print(f"[RESUME-DEBUG] requested_start(arg)={requested_start_s}  requested_start(task)={requested_task_s}  latest_file={latest_file_s}")
            print(f"[RESUME-DEBUG] resume_anchor={last_saved_s}  should_start_from={should_start_s}  computed_start={start_dt.isoformat()}")
    
            if should_start_dt is not None and start_dt < should_start_dt:
                print(f"[RESUME-ALERT] computed_start ({start_dt.isoformat()}) < should_start ({should_start_s})")
        except Exception:
            pass
    
        return start_dt
    
    
    def _skip_existing_middle_day(
        self, *,
        asset: str, symbol: str, interval: str, sink: str, day_iso: str,
        first_last_hint: Optional[tuple[Optional[str], Optional[str]]] = None,
    ) -> bool:
        """Determines whether to skip downloading data for a day because it's already complete and is a middle day.

        This method implements intelligent resume logic by skipping re-downloads of complete middle days (days
        that fall strictly between the earliest and latest existing files). It always processes edge days (first
        and last) to ensure they are complete, and handles retrograde start dates properly. For options, it also
        checks whether daily parts are complete before deciding to skip.

        Parameters
        ----------
        asset : str
            The asset type (e.g., 'option', 'stock', 'index').
        symbol : str
            The ticker symbol or root symbol.
        interval : str
            The bar interval (e.g., '1d', '5m', '1h', 'tick').
        sink : str
            The output sink type. Only 'csv' and 'parquet' are supported; other sinks return False.
        day_iso : str
            The day being evaluated in 'YYYY-MM-DD' format.
        first_last_hint : tuple of (str or None, str or None), optional
            Default: None

            A hint containing (first_existing_day, last_existing_day) to avoid re-scanning files.
            If None, the method will scan files to determine the earliest and latest days.

        Returns
        -------
        bool
            True if the day should be skipped (complete middle day), False if it should be processed.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _sync_symbol() to decide whether to skip downloading for each day in the sync range
        # - Download methods to optimize resume behavior and avoid redundant downloads
        """

        # ### >>> FAST RESUME — MIDDLE-DAY BYPASS (only strong 'skip') — BEGIN
        # In strong 'skip' we bypass *checks*, ma NON dobbiamo saltare tutto alla cieca:
        # Skippiamo soltanto i *middle day* veri (cioè day_iso strettamente tra first e last),
        # lasciando elaborare edge day e futuro. Per confronto usiamo 'YYYY-MM-DD' lexicografico.
        if getattr(self, "_fast_resume_skip_middle", False):
            if first_last_hint and first_last_hint[0] and first_last_hint[1]:
                first_day, last_day = first_last_hint
                if first_day < day_iso < last_day:
                    return True  # solo i middle-day
            return False  # edge/futuro: NON skippare
        # ### >>> FAST RESUME — MIDDLE-DAY BYPASS (only strong 'skip') — END

            
        if sink not in ("csv", "parquet"):
            return False
    
   
        base_dir = Path(self.cfg.root_dir) / "data" / asset / symbol / interval / sink
        base_dir.mkdir(parents=True, exist_ok=True)
    
        out_name = f"{day_iso}T00-00-00Z-{symbol}-{asset}-{interval}.{sink}"
        out_path = base_dir / out_name
        day_has_any = bool(self._list_day_files(asset, symbol, interval, sink, day_iso))
        print(f"[RESUME-DEBUG] day={day_iso} has_any={day_has_any}")

        sink_lower = sink.lower()
        if first_last_hint is not None:
            first_existing, last_existing = first_last_hint
        else:
            first_existing, last_existing = self._get_first_last_day_from_sink(
                asset, symbol, interval, sink_lower
            )
        
        if day_has_any:
            # --- Edge-day policy (earliest vs latest) ---
            if first_existing and day_iso == first_existing:
                if asset == "option":
                    st = self._day_parts_status(asset, symbol, interval, sink, day_iso)
                    parts = st.get("parts", [])
                    # Se manca part01, ci sono buchi o mix -> procedi (ricostruzione completa del primo giorno)
                    if st.get("missing") or st.get("has_mixed") or st.get("needs_rebuild") or (1 not in parts):
                        print(f"[RESUME-DEBUG] proceed (edge-first needs rebuild): {day_iso}  edges={first_existing}..{last_existing}")
                        return False
                    # Altrimenti il primo giorno è completo con part01 -> SKIP giorno intero
                    print(f"[RESUME-DEBUG] skip existing (edge-first complete with part01): {day_iso}  edges={first_existing}..{last_existing}")
                    return True
                # Non-option: preserva comportamento precedente (procedi sull'edge-first)
                print(f"[RESUME-DEBUG] proceed (edge-first non-option): {day_iso}  edges={first_existing}..{last_existing}")
                return False
        
            if last_existing and day_iso == last_existing:
                # Ultimo giorno edge -> procedi (si mantiene il comportamento precedente)
                print(f"[RESUME-DEBUG] proceed (edge-last): {day_iso}  edges={first_existing}..{last_existing}")
                return False
        
            # --- Middle day ---
            if asset == "option":
                st = self._day_parts_status(asset, symbol, interval, sink, day_iso)
                # se giorno incompleto/misto -> NON skippare (lo ricostruiamo)
                if st.get("missing") or st.get("has_mixed") or st.get("needs_rebuild"):
                    print(f"[RESUME-DEBUG] proceed (day...ncomplete): {day_iso}  edges={first_existing}..{last_existing}")
                    return False
            # giorno medio completo -> skip
            print(f"[RESUME-DEBUG] skip existing (middle day): {day_iso}  edges={first_existing}..{last_existing}")
            return True


        
        print(f"[RESUME-DEBUG] proceed (missing or edge day): {day_iso}  edges={first_existing}..{last_existing}")
        return False


    # -------------------------- PUBLIC API ---------------------------

    def _get_first_last_day_from_sink(
            self, asset: str, symbol: str, interval: str, sink: str
        ) -> tuple[str | None, str | None]:
            """
            Ritorna (first_day, last_day) dal sink appropriato (file-based o Influx).
            Universale per tutti asset/sink/tf.
            """
            sink_lower = (sink or "").lower()
            
            if sink_lower == "influxdb":
                prefix = (self.cfg.influx_measure_prefix or "")
                if asset == "option":
                    meas = f"{prefix}{symbol}-option-{interval}"
                else:
                    meas = f"{prefix}{symbol}-{asset}-{interval}"

                # --- >>> AVAILABLE_DATES FAST PATH — BEGIN
                fd, ld = self._influx_available_dates_first_last_day(meas)
                if fd and ld:
                    return fd, ld
                # build single dates support table if not existing yet
                else:
                    print(f"[INFLUX-META-QUERY][DEBUG] Unique dates support table not found; boostrap for creation lunched.")
                    self._influx_available_dates_bootstrap_from_main(meas)
                    fd, ld = self._influx_available_dates_first_last_day(meas)
                    if fd and ld:
                        return fd, ld                    
                # --- >>> AVAILABLE_DATES FAST PATH — END


                try:
                    import time
                    from datetime import timezone
                    cli = self._ensure_influx_client()

                    # --- >>> WARM-UP GUARD: Skip metadata query if table doesn't exist — BEGIN
                    # Querying system.parquet_files when table is in inconsistent state (deleted, warm-up)
                    # can trigger "Panic: table exists" in InfluxDB v3
                    print(f"[INFLUX-META-QUERY][PRE-CHECK] Checking if measurement '{meas}' exists...")
                    try:
                        exists = self._influx_measurement_exists(meas)
                        if not exists:
                            print(f"[INFLUX-META-QUERY][SKIP] Measurement '{meas}' does not exist, skipping metadata query")
                            return None, None
                    except Exception as e:
                        print(f"[INFLUX-META-QUERY][WARN] Existence check failed (warm-up?): {type(e).__name__}: {e}")
                        print(f"[INFLUX-META-QUERY][SKIP] Skipping metadata query due to existence check failure")
                        return None, None
                    # --- >>> WARM-UP GUARD — END

                    print(f"[INFLUX-META-QUERY][START] Querying first/last timestamp from Parquet metadata for '{meas}'...")
                    t0 = time.time()

                    # Query Parquet file metadata instead of scanning all data
                    # This is MUCH faster: reads KB of metadata instead of GB of data
                    sql = f"""
                    SELECT MIN(min_time) AS start_ts, MAX(max_time) AS end_ts
                    FROM system.parquet_files
                    WHERE table_name = '{meas}'
                    """

                    result = cli.query(sql)
                    df = result.to_pandas() if hasattr(result, "to_pandas") else result

                    # --- >>> DEBUG: Log what metadata query returned — BEGIN
                    if df is None:
                        print(f"[INFLUX-META-QUERY][DEBUG] Query returned None (df=None)")
                    elif df.empty:
                        print(f"[INFLUX-META-QUERY][DEBUG] Query returned empty DataFrame (len=0)")
                    else:
                        print(f"[INFLUX-META-QUERY][DEBUG] Query returned {len(df)} rows")
                        print(f"[INFLUX-META-QUERY][DEBUG] Columns: {df.columns.tolist()}")
                        print(f"[INFLUX-META-QUERY][DEBUG] First row: {df.iloc[0].to_dict()}")

                    # Also try to see ALL tables in system.parquet_files to verify query is working
                    try:
                        check_sql = "SELECT DISTINCT table_name FROM system.parquet_files LIMIT 10"
                        check_result = cli.query(check_sql)
                        check_df = check_result.to_pandas() if hasattr(check_result, "to_pandas") else check_result
                        if check_df is not None and not check_df.empty:
                            tables = check_df['table_name'].tolist()
                            print(f"[INFLUX-META-QUERY][DEBUG] Found {len(tables)} tables in system.parquet_files: {tables[:5]}")
                            if meas in tables:
                                print(f"[INFLUX-META-QUERY][DEBUG] ✓ Measurement '{meas}' IS in system.parquet_files!")
                            else:
                                print(f"[INFLUX-META-QUERY][DEBUG] ✗ Measurement '{meas}' NOT in system.parquet_files!")
                        else:
                            print(f"[INFLUX-META-QUERY][DEBUG] system.parquet_files appears empty")
                    except Exception as debug_e:
                        print(f"[INFLUX-META-QUERY][DEBUG] Failed to check table list: {debug_e}")
                    # --- >>> DEBUG — END

                    first_ts = None
                    last_ts = None

                    if df is not None and len(df) > 0:
                        row = df.iloc[0]

                        # start_ts and end_ts are in nanoseconds
                        if pd.notna(row.get("start_ts")):
                            start_ns = row["start_ts"]
                            first_ts = pd.Timestamp(start_ns, unit='ns', tz='UTC')

                        if pd.notna(row.get("end_ts")):
                            end_ns = row["end_ts"]
                            last_ts = pd.Timestamp(end_ns, unit='ns', tz='UTC')

                    first_day = first_ts.tz_convert("America/New_York").date().isoformat() if first_ts else None
                    last_day = last_ts.tz_convert("America/New_York").date().isoformat() if last_ts else None

                    total_elapsed = time.time() - t0
                    print(f"[INFLUX-META-QUERY][COMPLETE] Total time: {total_elapsed:.2f}s | first_day={first_day}, last_day={last_day}")

                    # --- >>> FALLBACK: If metadata returns empty, try direct SELECT on measurement — BEGIN
                    # Reason: Data may be in write cache (WAL) but not yet persisted to Parquet files
                    # This makes metadata query return empty even though data exists in measurement
                    if first_day is None and last_day is None:
                        print(f"[INFLUX-META-QUERY][FALLBACK] Metadata empty, trying direct SELECT MIN/MAX(time) on '{meas}'...")
                        try:
                            t0_fallback = time.time()

                            # Direct query on measurement (slower but reliable for fresh data)
                            direct_sql = f'SELECT MIN(time) AS min_t, MAX(time) AS max_t FROM "{meas}"'
                            direct_result = cli.query(direct_sql)
                            direct_df = direct_result.to_pandas() if hasattr(direct_result, "to_pandas") else direct_result

                            if direct_df is not None and len(direct_df) > 0:
                                direct_row = direct_df.iloc[0]

                                if pd.notna(direct_row.get("min_t")):
                                    min_t = pd.to_datetime(direct_row["min_t"], utc=True)
                                    first_day = min_t.tz_convert("America/New_York").date().isoformat()

                                if pd.notna(direct_row.get("max_t")):
                                    max_t = pd.to_datetime(direct_row["max_t"], utc=True)
                                    last_day = max_t.tz_convert("America/New_York").date().isoformat()

                                fallback_elapsed = time.time() - t0_fallback
                                print(f"[INFLUX-META-QUERY][FALLBACK] Time: {fallback_elapsed:.2f}s | first_day={first_day}, last_day={last_day}")

                        except Exception as fallback_e:
                            print(f"[INFLUX-META-QUERY][FALLBACK][WARN] Direct query failed: {type(fallback_e).__name__}: {fallback_e}")
                    # --- >>> FALLBACK — END

                    return first_day, last_day
                
                except Exception as e:
                    print(f"[SINK-GLOBAL][WARN] Influx query failed for {meas}: {e}")
                    return None, None
            
            elif sink_lower in ("csv", "parquet"):
                files = self._list_series_files(asset, symbol, interval, sink_lower)
                if not files:
                    return None, None

                def _start_from_filename(path: str) -> str | None:
                    base = os.path.basename(path)
                    # expected: 'YYYY-MM-DDT...-SYMBOL-asset-interval.csv'
                    return base.split("T", 1)[0] if "T" in base else None

                earliest = None
                latest = None

                for path in files:
                    # compute earliest from filename
                    s = _start_from_filename(path)
                    if s and (earliest is None or s < earliest):
                        earliest = s

                    # compute latest directly from filename prefix (daily files are one-day each)
                    if s and (latest is None or s > latest):
                        latest = s

                # For EOD stock/index, read the ACTUAL last date from file content
                # (batch downloads create single files spanning multiple dates)
                if interval == "1d" and asset in ("stock", "index") and files:
                    try:
                        # Get the file with the latest start date
                        latest_file = max(files, key=lambda f: _start_from_filename(f) or "")

                        if sink_lower == "csv":
                            # Read last few lines to find actual last date
                            # Files are now kept chronologically sorted, so last lines = latest dates
                            lines = self._tail_csv_last_n_lines(latest_file, n=32)
                            for line in lines:  # lines are newest-first
                                dt = self._parse_csv_first_col_as_dt(line)
                                if dt is not None:
                                    latest = dt.date().isoformat()
                                    break

                        elif sink_lower == "parquet":
                            # Read parquet and get max date
                            df = pd.read_parquet(latest_file)
                            for tcol in ("created", "timestamp", "last_trade"):
                                if tcol in df.columns:
                                    ts = pd.to_datetime(df[tcol], errors="coerce", utc=True).dropna()
                                    if not ts.empty:
                                        latest_dt = ts.max().to_pydatetime()
                                        latest = latest_dt.astimezone(timezone.utc).date().isoformat()
                                        break
                    except Exception as e:
                        # If reading fails, keep filename-based latest
                        print(f"[WARN] Could not read actual last date from {latest_file}: {e}")

                return earliest, latest

                
    def _probe_existing_last_ts_with_source(self, task, symbol: str, interval: str, sink: str) -> tuple[datetime | None, str | None]:
        """
        Come _probe_existing_last_ts ma ritorna anche il *file path* dal quale abbiamo dedotto il last timestamp.
        """
        base = self._find_existing_series_base(task.asset, symbol, interval, sink)
        if not base:
            return None, None
    
        # CSV
        if sink == "csv":
            path = self._pick_latest_part(base, "csv") or base
            if not os.path.exists(path):
                return None, None

            # Files are now kept chronologically sorted, so we can read last lines efficiently
            lines = self._tail_csv_last_n_lines(path, n=64)
            for ln in lines:  # newest-first
                dt = self._parse_csv_first_col_as_dt(ln)
                if dt is not None:
                    if interval == "1d":
                        day = dt.date()
                        from datetime import datetime as datetime_cls
                        return datetime_cls.combine(day, datetime_cls.min.time(), tzinfo=timezone.utc), path
                    return dt, path
            return None, path
    
        # Parquet (best-effort)
        if sink == "parquet":
            try:
                path = self._pick_latest_part(base, "parquet") or base
                if not os.path.exists(path):
                    return None, None
                df = pd.read_parquet(path)
                for tcol in ("timestamp", "created", "last_trade"):
                    if tcol in df.columns:
                        ts = pd.to_datetime(df[tcol], errors="coerce", utc=True).dropna()
                        if not ts.empty:
                            last_dt = ts.iloc[-1].to_pydatetime()
                            if interval == "1d":
                                day = last_dt.astimezone(timezone.utc).date()
                                last_dt = dt.combine(day, dt.min.time(), tzinfo=timezone.utc)
                            return last_dt, path
            except Exception:
                pass
            return None, None
    
        return None, None


    def _missing_1d_days_csv(self, asset: str, symbol: str, interval: str, sink: str,
                             first_day: str, last_day: str,
                             api_available_dates: Optional[set] = None) -> list[str]:
        """Identify missing business days in a daily (1d) time series by scanning all CSV parts.

        This method reads through the base CSV file and all rotated part files (_partNN) to determine
        which business days (Monday-Friday, excluding holidays) are missing from the saved data within
        the specified date range. It intelligently selects the time column from 'created', 'timestamp',
        or 'last_trade' (in priority order) and compares observed dates against expected business days.
        Returns a list of missing dates for gap-filling operations.

        Parameters
        ----------
        asset : str
            The asset type (e.g., 'stock', 'option', 'index').
        symbol : str
            The ticker symbol or root symbol (e.g., 'AAPL', 'SPY').
        interval : str
            The bar interval, must be '1d' for this method to work correctly.
        sink : str
            The sink type (e.g., 'csv', 'parquet', 'influxdb').
        first_day : str
            The start date of the range to check in ISO format "YYYY-MM-DD".
        last_day : str
            The end date of the range to check in ISO format "YYYY-MM-DD".

        Returns
        -------
        list of str
            A list of missing business day dates in ISO format "YYYY-MM-DD", sorted chronologically.
            Returns an empty list if no files exist or if all business days are present.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - Gap detection and backfill logic to identify which days need to be downloaded
        # - Data integrity verification during resume operations

        missing = manager._missing_1d_days_csv(
            asset="stock",
            symbol="AAPL",
            interval="1d",
            sink="csv",
            first_day="2024-01-01",
            last_day="2024-01-31"
        )
        # Returns: ["2024-01-05", "2024-01-12", "2024-01-19"] (business days with no data)
        """

        sink_lower = (sink or "csv").lower()

        # Handle InfluxDB sink by querying the database
        if sink_lower == "influxdb":
            measurement = f"{symbol}-{asset}-{interval}"

            # Query for all unique dates in the range
            query = f"""
                SELECT DISTINCT DATE_TRUNC('day', time) AS date
                FROM "{measurement}"
                WHERE time >= to_timestamp('{first_day}T00:00:00Z')
                  AND time <= to_timestamp('{last_day}T23:59:59Z')
                ORDER BY date
            """

            df = self._influx_query_dataframe(query)
            if df.empty:
                # No data found - all days are missing
                if api_available_dates:
                    return sorted(list(api_available_dates))
                else:
                    expected = pd.bdate_range(first_day, last_day, freq="C")
                    return [d.date().isoformat() for d in expected]

            # Extract observed dates
            observed_days = set()
            date_col = df.columns[0]  # First column is the date
            for val in df[date_col]:
                try:
                    date_str = pd.to_datetime(val).strftime("%Y-%m-%d")
                    observed_days.add(date_str)
                except Exception:
                    continue

            if not observed_days:
                if api_available_dates:
                    return sorted(list(api_available_dates))
                else:
                    expected = pd.bdate_range(first_day, last_day, freq="C")
                    return [d.date().isoformat() for d in expected]

            # Compare with expected business days (or API available dates)
            observed = pd.DatetimeIndex(pd.to_datetime(sorted(observed_days))).normalize()
            if api_available_dates:
                # Use API dates instead of business days
                missing = api_available_dates - set(observed_days)
                return sorted(list(missing))
            else:
                expected = pd.bdate_range(first_day, last_day, freq="C")
                missing = expected.difference(observed)
                return [d.date().isoformat() for d in missing]

        # Original file-based logic for CSV/Parquet
        series_dir = os.path.join(self.cfg.root_dir, "data", asset, symbol, interval, sink_lower)
        if not os.path.isdir(series_dir):
            return []

        files = []
        suffix = f"-{symbol}-{asset}-{interval}"
        for name in os.listdir(series_dir):
            if not name.lower().endswith(".csv"):
                continue
            if suffix not in name:
                continue
            files.append(os.path.join(series_dir, name))

        if not files:
            return []

        # estrai giorni osservati
        observed_days = set()
        for f in files:
            try:
                head = pd.read_csv(f, nrows=0, dtype=str)
                cols = head.columns.tolist()
                time_col = "created" if "created" in cols else ("timestamp" if "timestamp" in cols else ("last_trade" if "last_trade" in cols else None))
                if not time_col:
                    continue
                s = pd.read_csv(f, usecols=[time_col], dtype=str)[time_col]
                s = pd.to_datetime(s, errors="coerce", utc=True).dt.normalize().dropna()
                observed_days.update(s.dt.date.astype(str).tolist())
            except Exception:
                continue
    
        if not observed_days:
            return []

        # Compare with expected business days (or API available dates)
        if api_available_dates:
            # Use API dates instead of business days
            missing = api_available_dates - observed_days
            return sorted(list(missing))
        else:
            observed = pd.DatetimeIndex(pd.to_datetime(sorted(observed_days))).normalize()
            expected = pd.bdate_range(first_day, last_day, freq="C")  # Mon-Fri (festività escluse)
            missing = expected.difference(observed)
            return [d.date().isoformat() for d in missing]

    def _compute_intraday_window_et(self, asset: str, symbol: str, interval: str, sink: str, day_iso: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Ritorna (start_et_hms, end_et_hms) per una singola day_iso.
        start_et_hms è calcolato come (max timestamp del giorno già salvato) - overlap_seconds,
        >>> POI ALLINEATO AL BUCKET per M/H frames <
        """
        ET = ZoneInfo("America/New_York")
    
        # >>> INFLUX-FIRST PATH (skip file scan) <
        sink_lower = (sink or "").lower()
        if sink_lower == "influxdb":
            prefix = (self.cfg.influx_measure_prefix or "")
            if asset == "option":
                meas = f"{prefix}{symbol}-option-{interval}"
            else:
                meas = f"{prefix}{symbol}-{asset}-{interval}"
            
            day_start_utc = pd.Timestamp(f"{day_iso}T00:00:00", tz=ET).tz_convert("UTC")
            day_end_utc = (pd.Timestamp(f"{day_iso}T00:00:00", tz=ET) + pd.Timedelta(days=1)).tz_convert("UTC")
            
            try:
                last_in_day = self._influx_last_ts_between(meas, day_start_utc, day_end_utc)
                if last_in_day is not None:
                    max_ts = last_in_day.tz_convert(ET).tz_localize(None)  # ET-naive
                else:
                    return (None, None)
            except Exception as e:
                print(f"[WINDOW][INFLUX][WARN] query failed: {e}")
                return (None, None)
        else:
            # File-based path (original logic)
            parts = self._list_day_files(asset, symbol, interval, sink, day_iso)
            if not parts:
                return (None, None)
    
            # HEAD-REFILL guards
            try:
                part_list = self._list_day_part_files(asset, symbol, interval, sink, day_iso)
                if part_list and part_list[0][0] > 1:
                    return (None, None)
            except Exception:
                pass
            try:
                st = self._day_parts_status(asset, symbol, interval, sink, day_iso)
                if st.get("missing") or st.get("has_mixed"):
                    return (None, None)
            except Exception:
                pass
    
            def _max_ts_from_file(path: str) -> Optional[pd.Timestamp]:
                try:
                    if path.endswith(".csv"):
                        head = pd.read_csv(path, nrows=0, dtype=str)
                        cols = list(head.columns)
                        tcol = next((c for c in ["trade_timestamp","timestamp","bar_timestamp","datetime","created","last_trade"] if c in cols), None)
                        if not tcol:
                            return None
                        s = pd.read_csv(path, usecols=[tcol], dtype=str)[tcol]
                        ts = pd.to_datetime(s, errors="coerce")
                    else:
                        df = pd.read_parquet(path, columns=["timestamp"])
                        ts = pd.to_datetime(df["timestamp"], errors="coerce")
                    if getattr(ts.dtype, "tz", None) is not None:
                        ts = ts.dt.tz_convert(ET).dt.tz_localize(None)
                    return ts.max() if ts.notna().any() else None
                except Exception:
                    return None
    
            max_ts = None
            for p in parts:
                mt = _max_ts_from_file(p)
                if mt is not None and (max_ts is None or mt > max_ts):
                    max_ts = mt
    
            if not max_ts:
                return (None, None)
    
        # >>> COMMON: Overlap + Bucket Alignment <
        overlap = int(getattr(self.cfg, "overlap_seconds", 0) or 0)
        start_dt = max_ts - pd.Timedelta(seconds=overlap)
        
        iv = (interval or "").strip().lower()
        if iv.endswith("m") or iv.endswith("h"):
            m = int(iv[:-1]) * (60 if iv.endswith("h") else 1)
            # Floor to bucket (ET-naive input, no tz conversion needed)
            start_et_aware = start_dt.replace(tzinfo=ET)
            floored_utc = self._floor_to_interval_et(start_et_aware.astimezone(self.UTC), m)
            start_dt = floored_utc.astimezone(ET).replace(tzinfo=None)
        
        start_et_hms = start_dt.strftime("%H:%M:%S")
        return (start_et_hms, None)


    async def _write_df_to_sink(self, base_path: str, df, sink: str) -> None:
        """Writes a DataFrame to the configured sink (CSV, Parquet, or InfluxDB).

        This method handles the persistence of data to different storage backends, routing the DataFrame
        to the appropriate writer based on the sink type. Each sink type handles deduplication, rotation,
        and overlap safety differently.

        Parameters
        ----------
        base_path : str
            The full file path (for CSV/Parquet) or measurement name (for InfluxDB) where data should be written.
        df : pandas.DataFrame
            The DataFrame containing market data to persist.
        sink : str
            The sink type. Supported values: 'csv', 'parquet', 'influxdb'.

        Returns
        -------
        None
            Data is written to the specified sink but no value is returned.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _download_and_store_options() after fetching and processing option data
        # - _download_and_store_equity_or_index() after fetching stock/index data
        """
        s = (sink or "").strip().lower()
        if s == "csv":
            csv_text = df.to_csv(index=False)
            await self._append_csv_text(base_path, csv_text)
        elif s == "parquet":
            self._append_parquet_df(base_path, df)
        elif s == "influxdb":
            await self._append_influx_df(base_path, df)
        else:
            raise ValueError(f"Unsupported sink: {sink}")

        
    
    async def _append_csv_text(
        self, base_path: str, csv_text: str, *,
        force_first_part: int | None = None,
        stop_before_part: int | None = None,
        asset: str | None = None,
        interval: str | None = None
    ) -> None:
        """
        Append CSV text to part files with automatic size-based rotation and atomic writes.

        This method implements intelligent file rotation that respects the configured max_file_mb
        limit. It never writes to legacy base files; all data goes into numbered _partNN.csv files.
        The method handles header management and ensures no data loss through atomic operations.

        Parameters
        ----------
        base_path : str
            Base file path (e.g., "data/stock/AAPL/5m/csv/2024-03-15T00-00-00Z-AAPL-stock-5m.csv").
            The _partNN suffix will be added automatically.
        csv_text : str
            CSV content including header row. Must be non-empty and properly formatted.
        force_first_part : int, optional
            Default: None
            If specified, forces writing to start at this part number, ignoring existing parts.
            Used by head-refill operations.
        stop_before_part : int, optional
            Default: None
            If specified, stops writing before reaching this part number. Prevents overwriting
            existing parts during head-refill operations.

        Returns
        -------
        None
            Data is written directly to disk with side effects only.

        Example Usage
        -------------
        # Called by _download_and_store_equity_or_index and option download methods
        await manager._append_csv_text(
            base_path="data/stock/AAPL/5m/csv/2024-03-15T00-00-00Z-AAPL-stock-5m.csv",
            csv_text="timestamp,open,high,low,close,volume\\n2024-03-15T09:30:00Z,150.0,..."
        )

        # Head-refill with part constraints:
        await manager._append_csv_text(
            base_path="...", csv_text="...",
            force_first_part=1, stop_before_part=5
        )

        Notes
        -----
        - Never appends to legacy base files; always uses _partNN.csv format starting from _part01.
        - Automatically rotates to next part when current part would exceed max_file_mb.
        - Writes header only to new files (size == 0).
        - Performs byte-level space calculations to avoid exceeding size limits.
        - Processes CSV line-by-line to pack maximum data into each part file.
        - Uses synchronous file I/O (open/write/close) for atomic operations.
        - If target part already exists, appends to it until size limit reached.
        """

        # For EOD batch files (stock/index 1d), sort chronologically before writing
        # This ensures the file is always in chronological order for efficient reading
        if interval == "1d" and asset in ("stock", "index"):
            import pandas as pd
            from io import StringIO

            # Parse new CSV data as raw strings to avoid pandas auto date parsing
            # (mixed ISO8601 formats like "2024-02-01T16:46:45" vs "...45.000Z"
            # would otherwise raise and skip the batch)
            new_df = pd.read_csv(
                StringIO(csv_text),
                dtype=str,
                parse_dates=False,
                keep_default_na=False
            )
            if new_df.empty:
                return

            # Get target file (latest part or create new one)
            target = self._pick_latest_part(base_path, "csv")
            base_no_ext = base_path[:-4] if base_path.endswith(".csv") else base_path
            if not target or target == base_path:
                target = f"{base_no_ext}_part01.csv"

            # Read existing data if file exists
            existing_df = None
            if os.path.exists(target):
                try:
                    existing_df = pd.read_csv(
                        target,
                        dtype=str,
                        parse_dates=False,
                        keep_default_na=False
                    )
                except Exception as e:
                    print(f"[WARN] Could not read existing CSV for sorting: {e}, using new data only")
                    existing_df = None

            # Use common sorting/deduplication logic
            sorted_df, existing_count, new_rows, time_col = self._sort_and_deduplicate_eod_batch(existing_df, new_df)
            after_dedup = len(sorted_df)

            # Write sorted data back to file
            sorted_df.to_csv(target, index=False)
            print(f"[EOD-BATCH][SORT] Wrote {after_dedup} total rows to {os.path.basename(target)} ({existing_count} existing + {new_rows} new, sorted chronologically)")
            return

        # Seleziona il target corrente o l'ultimo _partNN
        target = self._pick_latest_part(base_path, "csv")
        base_no_ext = base_path[:-4] if base_path.endswith(".csv") else base_path
        
        # Forza part iniziale (usato dal head-refill): ignora i part esistenti e scrivi a partire da _partXX
        if force_first_part is not None:
            target = f"{base_no_ext}_part{int(force_first_part):02d}.csv"            
        
        # helper per sapere il numero del part corrente
        def _part_num(path: str) -> int:
            m = re.search(r"_part(\d{2})\.csv$", path or "")
            return int(m.group(1)) if m else 0

        if stop_before_part is not None and _part_num(target) >= int(stop_before_part):
            return

        # Mai scrivere sul "base" senza _partNN: se non ci sono part o c'è solo il base, inizia da _part01
        if not target or target == base_path:
            target = self._next_part_path(base_path, "csv")  # es. ..._part01.csv

        cap_bytes = int(self.cfg.max_file_mb * 1024 * 1024)
    
        lines = csv_text.splitlines()
        if not lines:
            return
    
        header, body = lines[0], lines[1:]
    
        while True:
            # assicurati di non superare cap sul file target corrente
            if not os.path.exists(target):
                cur_size = 0
            else:
                cur_size = os.path.getsize(target)
    
            write_header = (cur_size == 0)
            # spazio residuo (considera l'eventuale header)
            header_bytes = (len(header.encode("utf-8")) + 1) if write_header else 0
            space_left = cap_bytes - cur_size - header_bytes
    
            # se non c'è spazio neanche per l'header+una riga -> ruota
            if space_left <= 0:
                nxt = self._next_part_path(target, "csv")
                # stop prima dei part già presenti (es. primo esistente = 08 → ci fermiamo a 07)
                if stop_before_part is not None and _part_num(nxt) >= int(stop_before_part):
                    break
                target = nxt
                continue
    
            # impacchetta quante righe del body ci stanno nel residuo
            chunk = []
            used = 0
            for i, row in enumerate(body):
                row_bytes = len(row.encode("utf-8")) + 1  # + '\n'
                if used + row_bytes > space_left:
                    break
                chunk.append(row)
                used += row_bytes
    
            # niente da scrivere? ruota e riprova
            if not chunk and not write_header:
                nxt = self._next_part_path(target, "csv")
                if stop_before_part is not None and _part_num(nxt) >= int(stop_before_part):
                    break
                target = nxt
                continue
    
            # scrivi header (se serve) + chunk
            print(f"[_append_csv_text] Writing to target={target}, write_header={write_header}, chunk_size={len(chunk)}")
            with open(target, "a", encoding="utf-8", newline="") as f:
                if write_header:
                    f.write(header + "\n")
                if chunk:
                    f.write("\n".join(chunk) + "\n")
            print(f"[_append_csv_text] Write completed, file size now={os.path.getsize(target)}")
    
            # rimuovi dal body ciò che è stato scritto; se finito -> stop
            body = body[len(chunk):]
            if not body:
                break
    
            # passi successivi: il prossimo giro controllerà di nuovo spazio e ruoterà se serve
            target = self._next_part_path(target, "csv")



            

    def _append_parquet_df(
        self,
        base_path: str,
        df_new,
        *,
        force_first_part: int | None = None,
        stop_before_part: int | None = None,
        asset: str | None = None,
        interval: str | None = None
    ) -> int:
        """
        Write DataFrame to Parquet format using atomic, size-capped, append-by-rotation part files.

        This method provides intelligent data persistence with automatic deduplication, stable sorting,
        and file size management. It never modifies existing Parquet files in-place; instead, it creates
        new numbered part files (_partNN.parquet) with atomic write guarantees.

        Parameters
        ----------
        base_path : str
            Base file path. Any existing _partNN suffix is automatically removed before processing.
            Example: "data/option/AAPL/5m/parquet/2024-03-15T00-00-00Z-AAPL-option-5m.parquet"
        df_new : pandas.DataFrame
            New data to append. Must contain valid data; empty DataFrames are skipped.
        force_first_part : int, optional
            Default: None
            If specified, forces writing to start at this part number, bypassing latest part detection.
            Used for head-refill operations.
        stop_before_part : int, optional
            Default: None
            If specified, stops writing before reaching this part number. Prevents overwriting
            existing parts during head-refill operations.

        Returns
        -------
        int
            Number of rows successfully written to disk.

        Example Usage
        -------------
        # Called by _write_parquet_from_csv and option download methods
        rows_written = manager._append_parquet_df(
            base_path="data/stock/AAPL/5m/parquet/2024-03-15T00-00-00Z-AAPL-stock-5m.parquet",
            df_new=dataframe_with_new_bars
        )
        # Returns: 1500 (number of rows written)

        # Head-refill with constraints:
        rows_written = manager._append_parquet_df(
            base_path="...", df_new=df,
            force_first_part=1, stop_before_part=8
        )

        Behavior
        --------
        1. Sorting: Orders by timestamp → expiration → strike → right (when columns present).
        2. Deduplication: Removes duplicates within new batch and at boundary with last part.
        3. Boundary handling: For options, uses complex key (timestamp, expiration, strike, right).
           For stocks/indices, uses timestamp only with tail-based deduplication.
        4. Size management: Binary searches to find maximum rows fitting within max_file_mb limit.
        5. Rotation: Automatically creates next _partNN file when current exceeds size limit.
        6. Atomic writes: Uses temporary file + os.replace to prevent corruption.

        Notes
        -----
        - Never appends data inside existing Parquet files; always creates new part files.
        - Assumes timestamps are ET-naive; no timezone conversion performed.
        - Uses overlap_seconds config for boundary deduplication window.
        - For options: deduplicates on (timestamp, expiration, strike, right).
        - For stocks/indices: deduplicates on timestamp only.
        - Implements binary search to efficiently pack maximum rows per part.
        - Zero-byte files are automatically cleaned up.
        - PyArrow engine used for optimal Parquet I/O performance.
        """
    
        # normalizza: se arriva un path già con _partNN rimuovilo
        base_path = re.sub(r"_part\d{2}(?=\.(?:csv|parquet)$)", "", base_path)

        if df_new is None or len(df_new) == 0:
            return 0

        # For EOD batch files (stock/index 1d), sort chronologically before writing
        # This ensures the file is always in chronological order for efficient reading
        if interval == "1d" and asset in ("stock", "index"):
            import pandas as pd

            # Get target file (latest part or create new one)
            target = self._pick_latest_part(base_path, "parquet")
            base_no_ext = base_path[:-8] if base_path.endswith(".parquet") else base_path
            if not target or target == base_path:
                target = f"{base_no_ext}_part01.parquet"

            # Read existing data if file exists
            existing_df = None
            if os.path.exists(target):
                try:
                    existing_df = pd.read_parquet(target)
                except Exception as e:
                    print(f"[WARN] Could not read existing Parquet for sorting: {e}, using new data only")
                    existing_df = None

            # Use common sorting/deduplication logic
            sorted_df, existing_count, new_rows, time_col = self._sort_and_deduplicate_eod_batch(existing_df, df_new)
            after_dedup = len(sorted_df)

            # Write sorted data back to file
            sorted_df.to_parquet(target, engine='pyarrow', index=False)
            print(f"[EOD-BATCH][SORT] Wrote {after_dedup} total rows to {os.path.basename(target)} ({existing_count} existing + {new_rows} new, sorted chronologically)")
            return after_dedup

        # --- keys & columns ---
        ts_col = "timestamp"
        key_candidates = [ts_col, "symbol", "expiration", "strike", "right", "sequence"]  # include 'sequence' for ticks
        key_cols = [c for c in key_candidates if c in df_new.columns]
        if not key_cols:
            key_cols = [ts_col] if ts_col in df_new.columns else list(df_new.columns[:1])
            
        # --- FRONTIER DEDUPE vs latest part (tail-resume safe) ---
        # HEAD-REFILL 
        latest_part = None if force_first_part is not None else getattr(self, "_pick_latest_part", lambda *_: None)(base_path, "parquet")
        if latest_part and os.path.exists(latest_part) and os.path.getsize(latest_part) > 0 and ts_col in df_new.columns:
            cols_to_read = [c for c in key_cols if c in df_new.columns]
            try:
                last_df = pd.read_parquet(latest_part, columns=cols_to_read)
            except Exception:
                last_df = None

            if last_df is not None and not last_df.empty and ts_col in last_df.columns:
                last_max_ts = last_df[ts_col].max()
                if pd.notna(last_max_ts):
                    overlap = int(getattr(self.cfg, "overlap_seconds", 0) or 0)
                    cutoff = last_max_ts - pd.Timedelta(seconds=overlap)

                    # 1) drop <= cutoff (solo tail-resume; per head-refill questa sezione è saltata)
                    df_new = df_new[df_new[ts_col] > cutoff].copy()

                    # 2) boundary dedupe: distingui opzioni (chiave complessa) vs stock/index (solo timestamp)
                    if not df_new.empty:
                        if len(cols_to_read) > 1:
                            # OPZIONI: chiave complessa (timestamp + expiration + strike + right)
                            same_ts_mask = df_new[ts_col].eq(last_max_ts)
                            if same_ts_mask.any():
                                existing_keys = set(map(tuple, last_df.loc[last_df[ts_col].eq(last_max_ts), cols_to_read].to_numpy()))
                                if existing_keys:
                                    eq_df = df_new.loc[same_ts_mask, cols_to_read]
                                    keep_mask = ~eq_df.apply(tuple, axis=1).isin(existing_keys)
                                    df_new = pd.concat(
                                        [df_new.loc[~same_ts_mask], df_new.loc[same_ts_mask][keep_mask]],
                                        ignore_index=True
                                    )
                        else:
                            # STOCK/INDEX: solo timestamp
                            try:
                                tail_df = pd.read_parquet(latest_part).tail(500)
                                if ts_col in tail_df.columns:
                                    tail_ts = pd.to_datetime(tail_df[ts_col], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
                                    existing_ts = set(tail_ts.dropna())
                                    if existing_ts:
                                        use_seq = (interval == "tick" and "sequence" in df_new.columns and "sequence" in tail_df.columns)
                                        
                                        before = len(df_new)
                                        
                                        if use_seq:
                                            # Dedup su coppia (timestamp, sequence) per evitare di collassare tick distinti
                                            existing_keys = set(zip(tail_df[ts_col], tail_df["sequence"]))
                                            new_keys = list(zip(df_new[ts_col], df_new["sequence"]))
                                            df_new = df_new[[key not in existing_keys for key in new_keys]]
                                        else:
                                            new_ts = pd.to_datetime(df_new[ts_col], errors="coerce")
                                            df_new = df_new[~new_ts.isin(existing_ts)]
                                        
                                        removed = before - len(df_new)
                                        if removed > 0:
                                            print(f"[PARQUET][BOUNDARY-DEDUP] removed {removed} {'tick (ts+seq)' if use_seq else 'stock/index (ts)'} duplicates")

                            except Exception as e:
                                print(f"[PARQUET][WARN] stock/index boundary dedup failed: {e}")

    
        if df_new is None or df_new.empty:
            return 0
    
        # --- ORDER & INTRA-BATCH DEDUPE ---
        order_cols = [c for c in [ts_col, "expiration", "strike", "right"] if c in df_new.columns]
        if order_cols:
            df_new = df_new.sort_values(order_cols, kind="mergesort")  # stable
        df_new = df_new.drop_duplicates(subset=key_cols, keep="last")
    
        # --- SIZE-CAP chunking (Parquet usa fattore 6.4 rispetto al baseline CSV) ---
        max_mb = float(getattr(self.cfg, "max_file_mb", 64) or 64.0)
        max_bytes = int(max_mb * 1024 * 1024)
    
        rows = len(df_new)
        if rows == 0:
            return 0
    
        est_bytes = int(df_new.memory_usage(index=False, deep=True).sum())
        bytes_per_row = max(1, est_bytes // max(rows, 1))
        base_rows = max(1000, max_bytes // bytes_per_row)
        rows_per_part = max(1000, int(base_rows * 6.4))
    
        # --- head-refill controls & naming helpers ---
        def _part_num_from_path(p: str) -> int:
            m = _re.search(r"_part(\d{2})\.parquet$", p or "")
            return int(m.group(1)) if m else 0
    
        base_no_ext = os.path.splitext(base_path)[0]
    
        if force_first_part is not None:
            next_part = int(force_first_part)
        else:
            latest = getattr(self, "_pick_latest_part")(base_path, "parquet")
            next_part = _part_num_from_path(latest) + 1 if latest else 1
    
        def _make_target(n: int) -> str:
            return f"{base_no_ext}_part{n:02d}.parquet"
    
        def _should_stop(n: int) -> bool:
            return (stop_before_part is not None) and (n >= int(stop_before_part))
    
        # --- ATOMIC WRITE LOOP (one chunk -> one new part) ---
        write_count = 0
    
        def _write_one_chunk(chunk: pd.DataFrame) -> bool:
            nonlocal next_part, write_count
    
            # rispetto dello stop: non creare/oltrepassare part esistente successivo
            if _should_stop(next_part):
                return False
    
            target = _make_target(next_part)
    
            # non sovrascrivere: se esiste già e ha contenuto, avanza
            while os.path.exists(target) and os.path.getsize(target) > 0:
                next_part += 1
                if _should_stop(next_part):
                    return False
                target = _make_target(next_part)
    
            os.makedirs(os.path.dirname(target), exist_ok=True)
            tbl = pa.Table.from_pandas(chunk, preserve_index=False)
            tmp = f"{target}.{uuid.uuid4().hex}.tmp"
            pq.write_table(tbl, tmp)
            try:
                os.replace(tmp, target)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
            write_count += len(chunk)
            next_part += 1
            return True
    
        if rows_per_part >= rows:
            _write_one_chunk(df_new)
        else:
            for start in range(0, rows, rows_per_part):
                if not _write_one_chunk(df_new.iloc[start:start + rows_per_part]):
                    break
    
        return int(write_count)



    def _write_parquet_from_csv(self, base_path: str, csv_text: str, asset: str | None = None, interval: str | None = None) -> None:
        """
        Convert CSV text to DataFrame and persist into capped Parquet parts
        (..._partNN.parquet) using the same binary-search chunking as _append_parquet_df.
        """
        df_new = pd.read_csv(io.StringIO(csv_text), dtype=str)
        if df_new.empty:
            return

        # Normalizza tipi chiave come nel writer Parquet (timestamp già in UTC naive/string)
        if "timestamp" in df_new.columns:
            df_new["timestamp"] = pd.to_datetime(df_new["timestamp"], errors="coerce", utc=True).dt.tz_localize(None)

        if "expiration" in df_new.columns:
            df_new["expiration"] = pd.to_datetime(df_new["expiration"], errors="coerce").dt.date

        if "strike" in df_new.columns:
            df_new["strike"] = pd.to_numeric(df_new["strike"], errors="coerce")

        if "right" in df_new.columns:
            df_new["right"] = (
                df_new["right"].astype(str).str.strip().str.lower()
                .map({"c": "call", "p": "put", "call": "call", "put": "put"})
                .fillna(df_new["right"])
            )


        # --- SCRITTURA PARQUET (append sicuro) ---
        written = self._append_parquet_df(base_path, df_new, asset=asset, interval=interval)
        return written


    def _normalize_ts_to_utc(
        self,
        df: pd.DataFrame,
        *,
        datetime_cols: Iterable[str] | None = None,
        date_only_cols: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        """
        Convert all datetime-like columns from ET to UTC (naive) preserving ns precision.

        ThetaData API restituisce timestamp in ET (America/New_York timezone).
        Questa funzione converte ET → UTC.
        """
        if df is None or df.empty:
            return df

        dt_cols = list(datetime_cols) if datetime_cols else [
            "timestamp", "trade_timestamp", "bar_timestamp", "datetime",
            "created", "last_trade", "quote_timestamp", "timestamp_oi",
            "underlying_timestamp", "time", "date"
        ]
        date_cols = list(date_only_cols) if date_only_cols else ["expiration", "effective_date_oi"]

        out = df.copy()

        for col in dt_cols:
            if col in out.columns:
                s = pd.to_datetime(out[col], errors="coerce")

                # Timestamps are in ET - convert to UTC
                if getattr(s.dtype, "tz", None) is None:
                    # Naive timestamp: assume ET, convert to UTC
                    try:
                        s = s.dt.tz_localize("US/Eastern", nonexistent="shift_forward", ambiguous="NaT")
                    except:
                        # Fallback if pandas has issues
                        s = s.dt.tz_localize("US/Eastern", nonexistent="shift_forward")
                else:
                    # Already has timezone: convert to ET first
                    s = s.dt.tz_convert("US/Eastern")

                # Convert to UTC and remove timezone info
                out[col] = s.dt.tz_convert("UTC").dt.tz_localize(None)

        for col in date_cols:
            if col in out.columns:
                out[col] = pd.to_datetime(out[col], errors="coerce").dt.date

        return out


    def _format_dt_columns_isoz(self, df: pd.DataFrame, datetime_cols: Iterable[str] | None = None) -> pd.DataFrame:
        """
        Convert datetime columns (already UTC-naive or UTC tz-aware) to ISO-8601 strings with trailing 'Z'.
        Preserves sub-second precision up to ns as provided by pandas Timestamp.isoformat().
        """
        if df is None or df.empty:
            return df
        cols = list(datetime_cols) if datetime_cols else [
            "timestamp", "trade_timestamp", "bar_timestamp", "datetime",
            "created", "last_trade", "quote_timestamp", "timestamp_oi",
            "underlying_timestamp", "time", "date"
        ]
        out = df.copy()
        for col in cols:
            if col in out.columns:
                s = pd.to_datetime(out[col], errors="coerce", utc=True)
                out[col] = s.apply(lambda x: x.isoformat().replace("+00:00", "Z") if pd.notna(x) else None)
        return out


    def _normalize_df_types(
        self,
        df: pd.DataFrame,
        *,
        string_cols: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        """
        Heuristic normalization: parse timestamps/dates, convert numerics where safe,
        and leave known categorical columns as strings. Designed to avoid arithmetic
        on stringified numbers (e.g., mixed-format CSV payloads).

        NOTE: Timezone conversion is handled by _normalize_ts_to_utc() which should be
        called BEFORE this function. This function only ensures proper dtype parsing.
        """
        if df is None or df.empty:
            return df

        df_norm = df.copy()

        # Known timestamp-like columns - just ensure they're datetime type, NO timezone conversion
        # (timezone conversion is already handled by _normalize_ts_to_utc())
        ts_candidates = [
            "timestamp", "trade_timestamp", "bar_timestamp", "datetime",
            "created", "last_trade", "date", "time", "timestamp_oi",
            "underlying_timestamp"
        ]
        for col in ts_candidates:
            if col in df_norm.columns:
                # Only parse to datetime if not already datetime type, no timezone operations
                if not pd.api.types.is_datetime64_any_dtype(df_norm[col]):
                    df_norm[col] = pd.to_datetime(df_norm[col], errors="coerce")

        # Date-only columns
        date_cols = ["expiration", "effective_date_oi"]
        for col in date_cols:
            if col in df_norm.columns:
                df_norm[col] = pd.to_datetime(df_norm[col], errors="coerce").dt.date

        # Columns that must stay string/categorical
        default_string_cols = {
            "symbol", "root", "option_symbol", "exchange", "condition", "right",
            "type", "side", "currency", "mic", "venue", "status", "flag", "id",
            "code", "indicator", "source"
        }
        if string_cols:
            default_string_cols.update(string_cols)

        # Prefer numeric conversion for well-known numeric fields
        numeric_preferred = {
            "price", "bid", "ask", "mid", "last", "open", "high", "low", "close",
            "volume", "size", "trade_size", "bid_size", "ask_size", "oi", "open_interest",
            "implied_volatility", "implied_vol", "iv_error", "bid_implied_vol",
            "ask_implied_vol", "midpoint", "bar_iv", "trade_iv", "vega", "theta",
            "gamma", "delta", "rho", "multiplier", "strike", "sequence", "spread",
            "underlying_price"
        }

        for col in df_norm.columns:
            if col in default_string_cols or col in ts_candidates or col in date_cols:
                continue

            series = df_norm[col]
            # Skip pure string-like columns (non-numeric content)
            numeric_candidate = None
            try:
                numeric_candidate = pd.to_numeric(series, errors="coerce")
            except Exception:
                numeric_candidate = None

            if col in numeric_preferred:
                if numeric_candidate is not None:
                    df_norm[col] = numeric_candidate
                continue

            if numeric_candidate is None:
                continue

            # Heuristic: convert if at least 80% of non-null values parse as numeric
            non_null = series.notna().sum()
            if non_null == 0:
                continue
            numeric_ratio = numeric_candidate.notna().sum() / non_null
            if numeric_ratio >= 0.8:
                df_norm[col] = numeric_candidate

        return df_norm


    def _ensure_ts_utc_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a DataFrame that contains the __ts_utc column required for Influx verification."""
        if df is None or "__ts_utc" in df.columns or len(df) == 0:
            return df

        df_ts = df.copy()
        t_candidates = ["timestamp", "trade_timestamp", "bar_timestamp", "datetime", "created", "last_trade", "date", "time"]
        tcol = next((c for c in t_candidates if c in df_ts.columns), None)
        if not tcol:
            raise RuntimeError("No time column found while preparing data for Influx verification.")

        series = pd.to_datetime(df_ts[tcol], errors="coerce")
        if getattr(series.dtype, "tz", None) is None:
            series = series.dt.tz_localize("UTC")
        else:
            series = series.dt.tz_convert("UTC")
        df_ts["__ts_utc"] = series
        return df_ts

        
    # -------------------- INFLUXDB WRITER (append with overlap) --------------------
    async def _append_influx_df(self, base_path: str, df_new) -> int:
        """
        Append in InfluxDB 3 (FlightSQL) SENZA global cutoff.
        Applica un dedup "hard" lato codice su chiave logica:
        (__ts_utc + eventuali tag: symbol, expiration, right, strike [+ sequence se presente]).
        """
        from .exceptions import InfluxDBAuthError

        try:
            cli = self._ensure_influx_client()
        except Exception as e:
            # Check for auth errors in client initialization
            err_msg = str(e).lower()
            if 'unauthorized' in err_msg or '401' in err_msg or '403' in err_msg or 'auth' in err_msg:
                print(f"[INFLUX][FATAL] Authentication failed: {e}")
                raise InfluxDBAuthError(f"InfluxDB authentication failed: {e}") from e
            raise

        if df_new is None or len(df_new) == 0:
            return 0
    
        measurement = self._influx_measurement_from_base(base_path)
    
        measurement = self._influx_measurement_from_base(base_path)

        # --- >>> INFLUX WARM-UP SAFE TABLE CHECK — BEGIN
        # InfluxDB v3 can "panic" (server-side) if we probe a missing table with SELECT during warm-up.
        # Therefore this is a best-effort metadata check used only for logging.
        if not hasattr(self, "_influx_seen_measurements"):
            self._influx_seen_measurements = set()

        if measurement not in self._influx_seen_measurements:
            exists = False
            try:
                exists = bool(self._influx_measurement_exists(measurement))
            except Exception as e:
                # Do not fail the download/write path because of a metadata hiccup during warm-up.
                print(f"[INFLUX][WARN] measurement existence check skipped (warm-up?): {measurement}: {type(e).__name__}: {e}")
                exists = False

            if not exists:
                print(f"[INFLUX] creating new table {measurement}")

            self._influx_seen_measurements.add(measurement)
        # --- >>> INFLUX WARM-UP SAFE TABLE CHECK — END

        # 1) Individua la colonna tempo e normalizza in UTC
        t_candidates = ["timestamp","trade_timestamp","bar_timestamp","datetime","created","last_trade","date","time"]
        tcol = next((c for c in t_candidates if c in df_new.columns), None)
        if not tcol:
            raise RuntimeError("No time column found for Influx write.")

        # Let pandas infer timestamp format automatically
        s = pd.to_datetime(df_new[tcol], errors="coerce")
        if getattr(s.dtype, "tz", None) is None:
            s = s.dt.tz_localize("UTC")
        else:
            s = s.dt.tz_convert("UTC")
    
        df = df_new.copy()
        df["__ts_utc"] = s

        # --- >>> AVAILABLE_DATES: derive ET day(s) for this write batch — BEGIN
        try:
            _day_series = df["__ts_utc"].dropna().dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")
            _day_isos = set(_day_series.unique().tolist())
        except Exception:
            _day_isos = set()
        # --- >>> AVAILABLE_DATES: derive ET day(s) for this write batch — END

            
        # 2) NORMALIZZAZIONI leggere per chiavi logiche (servono al dedup)
        if "expiration" in df.columns:
            df["expiration"] = pd.to_datetime(df["expiration"], errors="coerce").dt.date
        if "strike" in df.columns:
            df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        if "right" in df.columns:
            df["right"] = (
                df["right"].astype(str).str.strip().str.lower()
                .map({"c": "call", "p": "put", "call": "call", "put": "put"})
                .fillna(df["right"])
            )
        if "sequence" in df.columns:
            df["sequence"] = pd.to_numeric(df["sequence"], errors="coerce")

        # 3) HARD DEDUPE: __ts_utc + tag se presenti (+ sequence se presente)
        tag_keys = [c for c in ["symbol","expiration","right","strike"] if c in df.columns]
        if "sequence" in df.columns:
            tag_keys.append("sequence")
        key_cols = ["__ts_utc"] + tag_keys
        before = len(df)
        df = df.dropna(subset=["__ts_utc"]).drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)
        dropped = before - len(df)
        print(f"[INFLUX][DEDUP] measurement={measurement} keys={key_cols} kept={len(df)}/{before} dropped={dropped}")
    
        # 4) (opzionale) esponi timestamp extra come campi numerici ns UTC
        _extra_ts_cols = ["underlying_timestamp", "timestamp_oi", "effective_date_oi"]
        for _c in _extra_ts_cols:
            if _c in df.columns:
                # Let pandas infer timestamp format automatically
                _ts = pd.to_datetime(df[_c], errors="coerce")
                if getattr(_ts.dtype, "tz", None) is None:
                    _ts = _ts.dt.tz_localize("UTC")
                else:
                    _ts = _ts.dt.tz_convert("UTC")
                try:
                    ns = _ts.astype("int64", copy=False).astype("float64")
                except Exception:
                    ns = _ts.to_numpy(dtype="datetime64[ns]").astype("int64").astype("float64")
                ns[_ts.isna()] = np.nan
                df[_c] = ns
    
        # 5) Line Protocol (solo campi numerici finiti; i tag: symbol, expiration, right, strike)
        def _esc(s: str) -> str:
            return str(s).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")
    
        def _to_ns(ts) -> int:
            ts = pd.Timestamp(ts)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            return int(ts.value)  # ns epoch

        tag_keys = ["symbol","expiration","right","strike"]
        if "sequence" in df.columns:
            tag_keys.append("sequence")
        not_field = set(["__ts_utc", "timestamp"]) | set(tag_keys)
        meas_esc = _esc(measurement)
    
        lines = []
        for idx, r in df.iterrows():
            try:
                if "__ts_utc" not in r or pd.isna(r["__ts_utc"]):
                    print(f"[INFLUX][ROW-SKIP] idx={idx} no __ts_utc")
                    continue
                ts_ns = _to_ns(r["__ts_utc"])
    
                tags = []
                for k in tag_keys:
                    if k in r and pd.notna(r[k]):
                        tags.append(f"{_esc(k)}={_esc(r[k])}")
                tagset = ",".join(tags)
    
                fields = []
                for k, v in r.items():
                    if k in not_field:
                        continue
                    try:
                        fv = float(v)
                        if math.isfinite(fv):
                            fields.append(f"{_esc(k)}={repr(fv)}")
                    except Exception:
                        continue
    
                if not fields:
                    continue
    
                line = meas_esc
                if tagset:
                    line += f",{tagset}"
                line += f" {','.join(fields)} {ts_ns}"
                lines.append(line)
    
            except Exception as e:
                print(f"[INFLUX][ROW-SKIP] idx={idx} err={type(e).__name__}: {e}")
                try:
                    dump_keys = ['__ts_utc','symbol','expiration','right','strike']
                    print(f"[INFLUX][ROW-DUMP] {{ { {k: (None if (k in r and pd.isna(r[k])) else r.get(k, None)) for k in dump_keys} } }}")
                except Exception:
                    pass
                continue
    
        if not lines:
            return 0
    
        # 6) Scrittura con retry automatico
        batch = int(max(1, getattr(self.cfg, "influx_write_batch", 5000)))
        written = 0
        total = len(lines)
        total_batches = (total + batch - 1) // batch

        all_chunks_ok = True

        for i in range(0, total, batch):
            chunk = lines[i:i+batch]
            batch_idx = i // batch + 1

            # Metadata per recovery
            metadata = {
                'measurement': measurement,
                'total_lines': total,
                'batch_size': len(chunk)
            }

            # Usa retry manager
            success = self._influx_retry_mgr.write_with_retry(
                client=cli,
                lines=chunk,
                measurement=measurement,
                batch_idx=batch_idx,
                metadata=metadata
            )

            if success:
                written += len(chunk)
                print(f"[INFLUX][WRITE] measurement={measurement} "
                      f"batch={batch_idx}/{total_batches} points={len(chunk)} status=ok")
            else:
                all_chunks_ok = False
                print(f"[INFLUX][WRITE] measurement={measurement} "
                      f"batch={batch_idx}/{total_batches} points={len(chunk)} status=FAILED (saved for recovery)")

        # --- >>> AVAILABLE_DATES: persist unique ET day(s) — BEGIN
        if all_chunks_ok and written > 0:
            try:
                self._influx_available_dates_note_days(measurement, _day_isos)
            except Exception as _e:
                print(f"[INFLUX][AVAILABLE_DATES][WARN] {measurement}: {type(_e).__name__}: {_e}")
        # --- >>> AVAILABLE_DATES: persist unique ET day(s) — END


        return written
    

    def _make_file_basepath(self, asset: str, symbol: str, interval: str, start_iso: str, ext: str) -> str:
        """Constructs the full file path for storing market data based on asset, symbol, interval, and date.

        This method creates a standardized directory structure and filename format for organizing market data
        files. The directory hierarchy is: root/data/{asset}/{symbol}/{interval}/{sink}/, and files are named
        with an ISO timestamp prefix for chronological sorting.

        Parameters
        ----------
        asset : str
            The asset type (e.g., 'option', 'stock', 'index').
        symbol : str
            The ticker symbol or root symbol.
        interval : str
            The bar interval (e.g., '1d', '5m', '1h').
        start_iso : str
            The ISO datetime string for the data start time (e.g., '2024-01-15T00-00-00Z').
        ext : str
            The file extension/sink type (e.g., 'csv', 'parquet', 'influxdb').

        Returns
        -------
        str
            The complete absolute file path including directory and filename.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _download_and_store_options() to determine where to save option data
        # - _download_and_store_equity_or_index() to determine where to save stock/index data
        # - Various methods that need to construct file paths for data persistence
        """
        sink_dir = self._sink_dir_name(ext)
        folder = os.path.join(self._root_sink_dir, asset, symbol, interval, sink_dir)
        # ⬇️ non creare cartelle per Influx
        if sink_dir != "influxdb":
            os.makedirs(folder, exist_ok=True)
        fname = f"{start_iso}-{symbol}-{asset}-{interval}.{ext}"
        return os.path.join(folder, fname)


    def _get_dates_in_eod_batch_files(self, asset: str, symbol: str, interval: str, sink_lower: str) -> set:
        """
        Legge i file batch EOD/measurement e restituisce un set di date (YYYY-MM-DD) effettivamente presenti.
        Usato per mild_skip mode per determinare quali giorni sono già scaricati.

        Per InfluxDB: usa query aggregata su metadati Parquet (veloce, una sola query)
        Per CSV/Parquet: legge file locali
        """
        # INFLUXDB: Query aggregata sui metadati per massima efficienza
        if sink_lower == "influxdb":
            prefix = self.cfg.influx_measure_prefix or ""
            if asset == "option":
                meas = f"{prefix}{symbol}-option-{interval}"
            else:
                meas = f"{prefix}{symbol}-{asset}-{interval}"
            return self._influx_available_dates_get_all(meas)

        # CSV/PARQUET: Continua con logica esistente
        files = self._list_series_files(asset, symbol, interval, sink_lower)
        if not files:
            return set()

        dates = set()

        for file_path in files:
            try:
                if sink_lower == "csv":
                    # Leggi CSV e estrai date
                    import csv
                    with open(file_path, 'r') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            # Cerca colonna timestamp/created/last_trade
                            for col in ('created', 'timestamp', 'last_trade'):
                                if col in row and row[col]:
                                    try:
                                        ts = pd.to_datetime(row[col], utc=True)
                                        dates.add(ts.date().isoformat())
                                        break
                                    except:
                                        continue

                elif sink_lower == "parquet":
                    # Leggi Parquet e estrai date
                    df = pd.read_parquet(file_path)
                    for col in ('created', 'timestamp', 'last_trade'):
                        if col in df.columns:
                            try:
                                ts_series = pd.to_datetime(df[col], utc=True, errors='coerce').dropna()
                                dates.update(ts_series.dt.date.astype(str).unique())
                                break
                            except:
                                continue

            except Exception as e:
                print(f"[WARN] Could not read dates from {file_path}: {e}")
                continue

        return dates

    def _list_series_files(self, asset: str, symbol: str, interval: str, sink_lower: str) -> list:
        """
        Return only canonical daily-part files for (asset, symbol, interval, sink), sorted by name.
        Canonical pattern (strict):
            YYYY-MM-DDT00-00-00Z-SYMBOL-asset-interval_partNN.(csv|parquet)
    
        Any non-matching file (e.g., "no dup 2025-11-07T...csv") is ignored.
    
        Parameters
        ----------
        asset : str
            "option", "stock", or "index".
        symbol : str
            Underlying symbol (case-insensitive on matching).
        interval : str
            Timeframe (e.g., "5m", "1m", "1d").
        sink_lower : str
            "csv" or "parquet".
    
        Returns
        -------
        list
            Sorted list of absolute file paths (canonical only).
        """
    
        ext = ".csv" if sink_lower == "csv" else ".parquet"
        series_dir = os.path.join(self.cfg.root_dir, "data", asset, symbol, interval, sink_lower)
        if not os.path.isdir(series_dir):
            return []
    
        sym_u = symbol.upper()
        asset_l = asset.lower()
        interval_l = interval.lower()
    
        # Strict canonical filename:
        #  YYYY-MM-DDT00-00-00Z-SYMBOL-asset-interval_partNN.ext
        pat = re.compile(
            rf'^\d{{4}}-\d{{2}}-\d{{2}}T00-00-00Z-{re.escape(sym_u)}-{asset_l}-{interval_l}_part\d+{re.escape(ext)}$',
            re.IGNORECASE
        )
    
        files = []
        for f in os.listdir(series_dir):
            # Quick extension check
            if not f.lower().endswith(ext):
                continue
            # Strict canonical check
            if not pat.match(f):
                continue
            files.append(os.path.join(series_dir, f))
    
        files.sort()
        return files

        
    
    def _list_day_files(self, asset: str, symbol: str, interval: str, sink: str, day_iso: str) -> list[str]:
        """
        Return all files associated with a specific trading day, including all part files.

        This method finds both legacy base files and all numbered part files (_part01, _part02, etc.)
        that belong to a single trading day. It's used for operations that need to process or analyze
        all data fragments for a given day.

        Parameters
        ----------
        asset : str
            Asset type: "option", "stock", or "index".
        symbol : str
            Ticker symbol.
        interval : str
            Timeframe (e.g., "5m", "1d").
        sink : str
            File format: "csv" or "parquet".
        day_iso : str
            Trading day in ISO format "YYYY-MM-DD".

        Returns
        -------
        list[str]
            Sorted list of absolute file paths matching the day prefix. Includes both base file
            (if exists) and all _partNN files. Returns empty list if no files found or directory
            doesn't exist.

        Example Usage
        -------------
        # Called by _day_parts_status, _csv_has_day, and cleanup operations
        day_files = manager._list_day_files(
            asset="stock", symbol="AAPL", interval="5m",
            sink="csv", day_iso="2024-03-15"
        )
        # Returns: [
        #   "path/2024-03-15T00-00-00Z-AAPL-stock-5m.csv",
        #   "path/2024-03-15T00-00-00Z-AAPL-stock-5m_part01.csv",
        #   "path/2024-03-15T00-00-00Z-AAPL-stock-5m_part02.csv"
        # ]

        Notes
        -----
        - Matches filename prefix: "{day_iso}T00-00-00Z-{symbol}-{asset}-{interval}"
        - Includes files with and without _partNN suffix.
        - Returns sorted list for stable ordering in processing operations.
        """
        sink_dir = self._sink_dir_name(sink)
        folder = os.path.join(self._root_sink_dir, asset, symbol, interval, sink_dir)
        if not os.path.isdir(folder):
            return []
    
        # Prefisso esatto del giorno (compatibile sia con base che con _partNN)
        prefix = f"{day_iso}T00-00-00Z-{symbol}-{asset}-{interval}"
        ext = f".{sink.lower()}"
    
        out = []
        try:
            for name in os.listdir(folder):
                # BASTA prefisso + estensione: prende sia "... .csv" sia "..._partNN.csv"
                if name.startswith(prefix) and name.endswith(ext):
                    out.append(os.path.join(folder, name))
        except FileNotFoundError:
            return []
    
        # Ordinamento stabile
        return sorted(out)


    
    def _list_day_part_files(self, asset: str, symbol: str, interval: str, sink_lower: str, day_iso: str):
        """Return sorted list of (part_num, filepath) for the given day."""
        folder = os.path.join(self.cfg.root_dir, "data", asset, symbol, interval, self._sink_dir_name(sink_lower))
        if not os.path.isdir(folder):
            return []
        base = f"{day_iso}T00-00-00Z-{symbol}-{asset}-{interval}"
        pat = re.compile(rf"^{re.escape(base)}_part(\d{{2}})\.{re.escape(sink_lower)}$")
        out = []
        for name in os.listdir(folder):
            m = pat.match(name)
            if m:
                out.append((int(m.group(1)), os.path.join(folder, name)))
        out.sort(key=lambda x: x[0])
        return out
    
    def _day_parts_status(self, asset: str, symbol: str, interval: str, sink: str, day_iso: str) -> dict:
        """
        Inspect that day's files. Support days with ONLY _partNN files (no legacy base).
        Returns:
          - base_path: canonical base path (may not exist on disk)
          - has_base : True if legacy non-part file exists
          - parts    : list of present part indices (e.g., [1,2,5])
          - missing  : gaps inside [1..max(parts)]
          - has_mixed: legacy base + parts together
          - needs_rebuild: True if head missing or gaps -> purge & rewrite from _part01
        """
    
        ext = sink
        # Canonical base path (anche se non esiste su disco)
        base_path = self._make_file_basepath(asset, symbol, interval, f"{day_iso}T00-00-00Z", sink)
    
        # Elenca tutti i file del giorno
        day_files = self._list_day_files(asset, symbol, interval, sink, day_iso)
    
        # Rileva presenza del legacy "base" (senza _partNN)
        has_base = os.path.exists(base_path)
    
        # Estrai gli indici dei parts presenti
        parts = []
        if day_files:
            pat = re.compile(rf"_part(\d{{2}})\.{re.escape(ext)}$")
            for p in day_files:
                m = pat.search(p)
                if m:
                    parts.append(int(m.group(1)))
        parts = sorted(set(parts))
    
        # Calcola i buchi
        missing = []
        if parts:
            mx = max(parts)
            missing = [i for i in range(1, mx + 1) if i not in parts]
    
        has_mixed = has_base and bool(parts)
        needs_rebuild = (1 not in parts and bool(parts)) or bool(missing) or has_mixed
    
        return {
            "base_path": base_path,
            "has_base": has_base,
            "parts": parts,
            "missing": missing,
            "has_mixed": has_mixed,
            "needs_rebuild": needs_rebuild,
        }
        


    def _find_existing_series_base(self, asset, symbol, interval, sink):
        sink_dir = self._sink_dir_name(sink)
        folder = os.path.join(self.cfg.root_dir, "data", asset, symbol, interval, sink_dir)
        if not os.path.isdir(folder):
            return None
    
        # accetta "...-SYMBOL-asset-interval(.|_partNN.)ext"
        pat = re.compile(
            rf"-{re.escape(symbol)}-{re.escape(asset)}-{re.escape(interval)}"
            rf"(?:_part\d{{2}})?\.{re.escape(sink)}$"
        )
        for name in os.listdir(folder):
            if pat.search(name):
                name_clean = re.sub(r"_part\d{2}(?=\." + re.escape(sink) + r"$)", "", name)
                return os.path.join(folder, name_clean)
        return None




    def _find_existing_daily_base_for_day(self, asset, symbol, interval, sink, day_iso):
        sink_dir = self._sink_dir_name(sink)  # richiede il piccolo helper _sink_dir_name
        folder = os.path.join(self._root_sink_dir, asset, symbol, interval, sink_dir)
        if not os.path.isdir(folder):
            return None
        suffix = f"-{symbol}-{asset}-{interval}.{sink}"
        # match: YYYY-MM-DDTHH-MM-SSZ-... oppure legacy YYYY-MM-DD-... (fallback)
        pat = re.compile(
            rf"^{re.escape(day_iso)}T?\d{{0,2}}-?\d{{0,2}}-?\d{{0,2}}Z?-"
            rf"{re.escape(symbol)}-{re.escape(asset)}-{re.escape(interval)}\.{re.escape(sink)}$"
        )
        for name in os.listdir(folder):
            if (name.endswith(suffix) and (name.startswith(day_iso) or pat.match(name))):
                # rimuovi eventuale _partNN
                name_clean = re.sub(r"_part\d{2}(?=\." + re.escape(sink) + r"$)", "", name)
                return os.path.join(folder, name_clean)
        return None

    def _sort_and_deduplicate_eod_batch(
        self,
        existing_df: Optional[pd.DataFrame],
        new_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, int, int, str]:
        """
        Sort and deduplicate EOD batch data chronologically.

        Common logic shared between CSV and Parquet EOD batch processing.

        Parameters
        ----------
        existing_df : Optional[pd.DataFrame]
            Existing data from file (None if new file)
        new_df : pd.DataFrame
            New data to append

        Returns
        -------
        tuple[pd.DataFrame, int, int, str]
            (sorted_df, existing_count, new_rows, time_col)
        """
        import pandas as pd

        # Track existing count
        existing_count = len(existing_df) if existing_df is not None else 0

        # Combine existing and new data
        if existing_df is not None:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df

        # Sort by trading date (prefer last_trade over created for EOD data)
        # last_trade = actual trading date, created = processing timestamp
        time_col_candidates = ['last_trade', 'created', 'timestamp', 'date']

        # Parse each candidate column separately, then merge row-wise (first non-NaT wins)
        parsed_map: Dict[str, pd.Series] = {}
        cand_debug: List[Tuple[str, int, int, List[str]]] = []
        for col in time_col_candidates:
            if col not in combined_df.columns:
                continue
            # Clean obvious control chars before parsing (handles CR/LF/TAB from CSV payloads)
            series = combined_df[col].astype(str).str.replace(r"[\r\n\t]", "", regex=True).str.strip()
            # Parse with errors='coerce'; pandas 2.x handles mixed ISO formats
            parsed = pd.to_datetime(series, utc=True, errors='coerce')
            valid = int(parsed.notna().sum())
            invalid_examples = series[parsed.isna()].head(3).tolist()
            cand_debug.append((col, len(series), valid, invalid_examples))
            parsed_map[col] = parsed

        if not parsed_map:
            print(f"[WARN] No timestamp-like columns found while sorting EOD batch; tried={time_col_candidates}")
            after_dedup = len(combined_df)
            new_rows = after_dedup - existing_count
            return combined_df, existing_count, new_rows, combined_df.columns[0]

        merged_ts = None
        for col in time_col_candidates:
            if col not in parsed_map:
                continue
            merged_ts = parsed_map[col] if merged_ts is None else merged_ts.fillna(parsed_map[col])

        time_col = "__eod_time"
        combined_df[time_col] = merged_ts

        total_valid = int(combined_df[time_col].notna().sum())

        # If nothing parsed (all NaT), log detailed debug and keep data without time-dedupe
        if total_valid == 0:
            for idx, (col, total, valid, bad) in enumerate(cand_debug, start=1):
                print(f"[EOD-DEBUG-TS] #{idx} col={col} valid={valid}/{total} bad_examples={bad}")
            print(f"[WARN] Could not parse any timestamps in columns {list(parsed_map.keys())}; writing batch in arrival order without time-based dedupe")
            after_dedup = len(combined_df)
            new_rows = after_dedup - existing_count
            return combined_df, existing_count, new_rows, time_col

        # Remove rows with NaT (failed parsing) BEFORE deduplication
        nat_mask = combined_df[time_col].isna()
        if nat_mask.any():
            failed_count = int(nat_mask.sum())
            sample = ", ".join(combined_df.loc[nat_mask, time_col_candidates[0] if time_col_candidates[0] in combined_df.columns else combined_df.columns[0]].astype(str).head(3).tolist())
            # Debug candidates to understand why parsing failed
            for idx, (col, total, valid, bad) in enumerate(cand_debug, start=1):
                print(f"[EOD-DEBUG-TS] #{idx} col={col} valid={valid}/{total} bad_examples={bad}")
            if sample:
                print(f"[WARN] Dropping {failed_count} rows with unparseable timestamps during EOD sorting (column={time_col}; examples: {sample})")
            else:
                print(f"[WARN] Dropping {failed_count} rows with unparseable timestamps during EOD sorting (column={time_col})")
            combined_df = combined_df[~nat_mask]

        # Sort chronologically
        combined_df = combined_df.sort_values(by=time_col)

        # Remove duplicates based on trading date (keep last occurrence)
        combined_df = combined_df.drop_duplicates(subset=[time_col], keep='last')
        after_dedup = len(combined_df)

        # Calculate new rows added
        new_rows = after_dedup - existing_count

        return combined_df, existing_count, new_rows, time_col


    def _pick_latest_part(self, base_path: str, ext: str) -> Optional[str]:
        """Find the latest existing part for a given daily base path."""
        base_no_ext = base_path[:-len(ext)-1]  # remove ".ext"
        candidates: List[str] = []
        p0 = f"{base_no_ext}.{ext}"
        if os.path.exists(p0):
            candidates.append(p0)
        for i in range(1, 1000):
            pi = f"{base_no_ext}_part{i:02d}.{ext}"
            if os.path.exists(pi):
                candidates.append(pi)
        return candidates[-1] if candidates else None
        

    def _next_part_path(self, path: str, ext: str) -> str:
        base, _ = os.path.splitext(path)
        if "_part" in base and base[-2:].isdigit():
            prefix, n = base[:-2], int(base[-2:])
            return f"{prefix}{n+1:02d}.{ext}"
        return f"{base}_part01.{ext}"

    

    
    def _ensure_under_cap(self, path: str, cap_mb: int) -> str:
        """Rotate to a new _partNN file if `path` exceeds the configured cap (in MB)."""
        if not os.path.exists(path):
            return path
        size_mb = os.path.getsize(path) / (1024 * 1024)
        if size_mb < cap_mb:
            return path
        base, ext = os.path.splitext(path)
        if "_part" in base and base[-2:].isdigit():
            prefix = base[:-2]
            n = int(base[-2:])
            new_base = f"{prefix}{n+1:02d}"
        else:
            new_base = f"{base}_part01"
        return f"{new_base}{ext}"

    def _purge_day_files(self, asset: str, symbol: str, interval: str, sink: str, day_iso: str) -> None:
        """Delete ALL files for that day (base + _partNN)."""
        for p in self._list_day_files(asset, symbol, interval, sink, day_iso):
            try:
                os.remove(p)
            except Exception:
                pass



    # ---------------------------- CACHE ------------------------------

    def _sink_dir_name(self, sink: str) -> str:
        """Normalize a sink type string to its corresponding directory name.

        This method converts various sink type spellings and aliases to their canonical directory
        names used in the file system structure. It handles common variations and ensures consistent
        folder naming across the data storage hierarchy.

        Parameters
        ----------
        sink : str
            The sink type string which may be in various forms (e.g., "CSV", "csv", "svc", "Parquet",
            "influx", "influxdb", "influxdb3", or any other sink identifier).

        Returns
        -------
        str
            The normalized directory name: "csv", "parquet", "influxdb", or the original sink string
            if no normalization rule applies. Defaults to "csv" if sink is None or empty.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _make_file_basepath() to construct file paths
        # - _list_series_files() to locate data directories
        # - Other file system operations that need consistent directory naming

        dir_name = manager._sink_dir_name("CSV")  # Returns: "csv"
        dir_name = manager._sink_dir_name("influxdb3")  # Returns: "influxdb"
        dir_name = manager._sink_dir_name(None)  # Returns: "csv" (default)
        """
        s = (sink or "").strip().lower()
        if s in ("svc", "csv"): return "csv"
        if s == "parquet": return "parquet"
        if s in ("influx", "influxdb", "influxdb3"): return "influxdb"
        return s or "csv"


    def _csv_has_day(self, base_path: str, day_iso: str) -> bool:
        """Check if any data exists for a specific day across all CSV parts (base and _partNN files).

        This method scans through the base CSV file and all rotated part files (_part01, _part02, etc.)
        to determine if at least one row exists for the specified trading day. It intelligently selects
        the time column from 'created', 'timestamp', or 'last_trade' (in that priority order) and parses
        dates to check for matches. This is used to verify data coverage and detect gaps.

        Parameters
        ----------
        base_path : str
            The base file path (e.g., "data/stock/AAPL/5m/csv/2024-03-15T00-00-00Z-AAPL-stock-5m.csv").
            The method will automatically check this file plus all _partNN variants.
        day_iso : str
            The target day to search for in ISO format "YYYY-MM-DD" (e.g., "2024-03-15").

        Returns
        -------
        bool
            True if at least one row exists for the specified day in any of the CSV files (base or parts).
            False if no data exists for that day or if no files are found.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _skip_existing_middle_day() to determine if a day should be skipped during download
        # - Gap detection logic to identify missing trading days

        has_data = manager._csv_has_day(
            base_path="data/stock/AAPL/5m/csv/2024-03-15T00-00-00Z-AAPL-stock-5m.csv",
            day_iso="2024-03-15"
        )
        # Returns: True if data exists for March 15, 2024
        """

        # elenca tutti i part: base + _partNN
        files = []
        base_no_ext = base_path[:-4] if base_path.endswith(".csv") else base_path
        # Aggiungi il legacy base se esiste
        p0 = f"{base_no_ext}.csv"
        if os.path.exists(p0):
            files.append(p0)

        # Aggiungi TUTTI i _partNN esistenti (non interrompere alla prima mancanza)
        for i in range(1, 1000):
            pi = f"{base_no_ext}_part{i:02d}.csv"
            if os.path.exists(pi):
                files.append(pi)

        if not files:
            return False
    
        for f in files:
            try:
                head = pd.read_csv(f, nrows=0, dtype=str)
                cols = head.columns.tolist()
                tcol = "created" if "created" in cols else ("timestamp" if "timestamp" in cols else ("last_trade" if "last_trade" in cols else None))
                if not tcol:
                    continue
                s = pd.read_csv(f, usecols=[tcol], dtype=str)[tcol]
                d = pd.to_datetime(s, errors="coerce", utc=True).dt.date.astype(str)
                if (d == day_iso).any():
                    return True
            except Exception:
                continue
        return False


    def _last_csv_day(self, base_path: str) -> Optional[str]:
        """Extract the last (most recent) day present in CSV data including rotated part files.

        This method identifies the latest part file (_partNN) for the given base path, reads the last
        line from that file, and extracts the date from the first column timestamp. This is used to
        determine the most recent date in the saved data for resume operations and coverage tracking.

        Parameters
        ----------
        base_path : str
            The base file path (e.g., "data/stock/AAPL/1d/csv/2024-01-01T00-00-00Z-AAPL-stock-1d.csv").

        Returns
        -------
        str or None
            The last day present in ISO format "YYYY-MM-DD" (e.g., "2024-03-15").
            Returns None if no files exist or if the last line cannot be parsed.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _get_first_last_day_from_sink() to determine date ranges
        # - Resume logic to find the last saved date

        last_day = manager._last_csv_day("data/stock/AAPL/1d/csv/2024-01-01T00-00-00Z-AAPL-stock-1d.csv")
        # Returns: "2024-03-15"
        """
        target = self._pick_latest_part(base_path, "csv")
        if not target:
            target = self._next_part_path(base_path, "csv")  # start from _part01
        last = self._tail_one_line(target)
        if not last:
            return None
        first = last.split(",", 1)[0].strip()
        try:
            dtu = self._as_utc(first)
            return dtu.date().isoformat()
        except Exception:
            return None


    def _last_csv_timestamp(self, path: str) -> Optional[str]:
        """
        Return the last timestamp of the CSV as ISO UTC string with Z.
        """

        if not os.path.exists(path):
            return None
        try:
            head = pd.read_csv(path, nrows=0, dtype=str)
        except Exception:
            return None
    
        cols = list(head.columns)
        time_candidates = ["trade_timestamp","timestamp","bar_timestamp","datetime","created","last_trade"]
        tcol = next((c for c in time_candidates if c in cols), None)
        if not tcol:
            return None
    
        try:
            s = pd.read_csv(path, usecols=[tcol], dtype=str)[tcol]
            ts = pd.to_datetime(s, errors="coerce", utc=True)
            ts = ts.dt.tz_convert("UTC")
            ts = ts.dropna()
            if ts.empty:
                return None
            return ts.iloc[-1].isoformat().replace("+00:00", "Z")
        except Exception:
            return None


    
    
    ### >>> REALTIME INTRADAY WINDOW (inclusive ET) — HELPERS [BEGIN]
    def _first_timestamp_in_csv(self, path: str):
        """Read a small chunk and return the earliest timestamp as pandas.Timestamp(UTC) or None."""

        try:
            df = pd.read_csv(path, nrows=500, dtype=str)
            if df is None or df.empty:
                return None
            col = self._detect_time_col(df.columns)
            if not col:
                return None
            s = df[col]
            # try unix seconds first, fallback to parse as datetime string
            v = pd.to_datetime(s, unit="s", utc=True, errors="coerce")
            if v.isna().all():
                v = pd.to_datetime(s, utc=True, errors="coerce")
            if v.isna().all():
                return None
            return v.min()
        except Exception:
            return None


    def _first_timestamp_in_parquet(self, path: str, ts_candidates=("timestamp","ts","time","datetime")):
        # legge il primo timestamp dal primo row group del parquet
        pf = pq.ParquetFile(path)
        names = set(pf.schema_arrow.names)
        col = next((c for c in ts_candidates if c in names), None)
        if not col:
            return None
        try:
            arr = pf.read_row_group(0, columns=[col]).column(0)
            # torna oggetto python (pandas gestisce conversione a Timestamp)
            return arr[0].as_py() if len(arr) else None
        except Exception:
            return None




        
    
    def _max_file_timestamp(self, path: str, tcol: str, sink_lower: str):
        """
        Ritorna il MAX timestamp (naive UTC) dalla colonna tcol in CSV o Parquet.
        Se non esiste/errore → None.
        """

        try:
            df = self._read_minimal_frame(path, [tcol], sink_lower)
            if tcol not in df.columns:
                return None
            ts = pd.to_datetime(df[tcol], errors="coerce", utc=True).dt.tz_localize(None)
            if ts.notna().any():
                return ts.max()
        except Exception:
            pass
        return None


        

    def _read_minimal_frame(self, path: str, usecols: list, sink_lower: str):
        """
        Legge SOLO le colonne richieste da un file CSV o Parquet.
        - Normalizza 'timestamp' in naive UTC (senza tz) se presente.
        - Non fa nessuna altra trasformazione.
        """

        sink_lower = (sink_lower or "csv").lower()
        if sink_lower == "csv":
            # CSV: limitiamo le colonne in read
            head = pd.read_csv(path, nrows=0, dtype=str)
            cols = [c for c in usecols if c in head.columns]
            df = pd.read_csv(path, usecols=cols, dtype=str) if cols else pd.read_csv(path, dtype=str)
        else:
            # PARQUET: proviamo a leggere solo le colonne richieste
            try:
                df = pd.read_parquet(path, columns=usecols)
            except Exception:
                df = pd.read_parquet(path)  # fallback, poi sottoselezioniamo
                keep = [c for c in usecols if c in df.columns]
                if keep:
                    df = df[keep]
    
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
        return df
    

    def _ensure_influx_client(self):
        """
        InfluxDB 3 client (FlightSQL).
        pip install influxdb3-python pyarrow
        """
        if getattr(self, "_influx", None):
            return self._influx
        if InfluxDBClient3 is None:
            raise RuntimeError(
                "influxdb3-python è richiesto per sink='influxdb' (pip install influxdb3-python pyarrow)"
            )

        host = self.cfg.influx_url or "http://localhost:8181"   # mappa url→host (v3)
        database = self.cfg.influx_bucket                       # mappa bucket→database (v3)
        token = self.cfg.influx_token
        if not (host and database and token):
            raise RuntimeError("Config v3 incompleta (influx_url/host, influx_bucket=database, influx_token).")

        timeout_env = os.environ.get("INFLUXDB_WRITE_TIMEOUT_MS") or os.environ.get("INFLUXDB_V2_TIMEOUT")
        try:
            timeout_ms = int(timeout_env) if timeout_env else 60000
        except ValueError:
            timeout_ms = 60000

        self._influx = InfluxDBClient3(
            host=host,
            token=token,
            database=database,
            timeout=timeout_ms
        )
        self.influx_client = self._influx  # Assegna anche a influx_client per check_duplicates
        return self._influx

    def _influx_query_dataframe(self, query: str) -> pd.DataFrame:
        """Execute a FlightSQL query and always return a pandas DataFrame."""
        try:
            cli = self._ensure_influx_client()
            result = cli.query(query)
            if isinstance(result, pd.DataFrame):
                return result
            if hasattr(result, "to_pandas"):
                return result.to_pandas()
            if hasattr(result, "to_pylist"):
                return pd.DataFrame(result.to_pylist())
            if isinstance(result, list):
                return pd.DataFrame(result)
            if result is None:
                return pd.DataFrame()
            return pd.DataFrame(result)
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def _extract_scalar_from_df(df: pd.DataFrame, preferred_names: Iterable[str]) -> Any:
        """Extract the first available scalar value matching preferred column names."""
        if df is None or df.empty:
            return None
        # Normalize lookup dict once
        lower_map = {col.lower(): col for col in df.columns}
        for name in preferred_names:
            col = lower_map.get(name.lower())
            if col and not df[col].empty:
                return df[col].iloc[0]
        # Fallback: first non-empty column
        for col in df.columns:
            series = df[col]
            if not series.empty:
                return series.iloc[0]
        return None

    @staticmethod
    def _coerce_timestamp(value: Any) -> Optional[pd.Timestamp]:
        """Convert various scalar types (pyarrow, numpy, str) to pandas Timestamp UTC."""
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.tz_convert("UTC") if value.tz is not None else value.tz_localize("UTC")
        try:
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            if pd.isna(ts):
                return None
            return ts
        except Exception:
            try:
                return pd.Timestamp(str(value), tz="UTC")
            except Exception:
                return None

    def _pick_timestamp_from_dataframe(self, df: Optional[pd.DataFrame]) -> Optional[pd.Timestamp]:
        """Heuristic to extract a timestamp column from a dataframe row."""
        if df is None or df.empty:
            return None
        candidate_cols = (
            "time",
            "timestamp",
            "datetime",
            "date",
            "QUOTE_DATETIME",
            "TRADE_DATETIME",
            "QUOTE_UNIXTIME",
            "TRADE_UNIXTIME",
        )
        for col in candidate_cols:
            if col in df.columns:
                try:
                    val = df[col].iloc[0]
                    ts = self._coerce_timestamp(val)
                    if ts is not None:
                        return ts
                except Exception:
                    continue
        # Fall back to first column if it looks like a datetime
        for col in df.columns:
            try:
                val = df[col].iloc[0]
                ts = self._coerce_timestamp(val)
                if ts is not None:
                    return ts
            except Exception:
                continue
        return None

    def _infer_first_last_from_sink(
        self,
        asset: str,
        symbol: str,
        interval: str,
        sink: str,
    ) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """Use query_local_data to fetch first and last timestamps from any sink."""
        first_ts = None
        last_ts = None
        try:
            df_first, _ = self.query_local_data(
                asset=asset,
                symbol=symbol,
                interval=interval,
                sink=sink,
                get_first_n_rows=1,
                _allow_full_scan=True,
            )
            first_ts = self._pick_timestamp_from_dataframe(df_first)
        except Exception:
            first_ts = None

        try:
            df_last, _ = self.query_local_data(
                asset=asset,
                symbol=symbol,
                interval=interval,
                sink=sink,
                get_last_n_rows=1,
                _allow_full_scan=True,
            )
            last_ts = self._pick_timestamp_from_dataframe(df_last)
        except Exception:
            last_ts = None

        return first_ts, last_ts

    def _iter_influx_parquet_files(self, measurement: str) -> Iterable[Path]:
        """Yield real Parquet files for a measurement following the InfluxDB v3 folder layout."""
        base_dir = self.cfg.influx_data_dir
        if not base_dir:
            return
        base = Path(base_dir)
        if not base.exists():
            return

        # Influx layout:
        # {root}/{node}/dbs/{database-id}/{measurement-id}/{YYYY-MM-DD}/{HH-MM}/*.parquet
        def candidate_roots(path: Path) -> List[Path]:
            roots = []
            if (path / "dbs").is_dir():
                roots.append(path)
            else:
                for child in path.iterdir():
                    if (child / "dbs").is_dir():
                        roots.append(child)
            return roots

        bucket_lower = (self.cfg.influx_bucket or "").lower()

        for node_root in candidate_roots(base):
            dbs_dir = node_root / "dbs"
            if not dbs_dir.is_dir():
                continue
            for db_dir in dbs_dir.iterdir():
                if not db_dir.is_dir():
                    continue
                if bucket_lower and not db_dir.name.lower().startswith(bucket_lower):
                    continue

                # Locate measurement directory by exact or prefix match (measurement may have shard suffix)
                measurement_dirs: List[Path] = []
                direct = db_dir / measurement
                if direct.is_dir():
                    measurement_dirs.append(direct)
                else:
                    target_lower = measurement.lower()
                    for child in db_dir.iterdir():
                        if child.is_dir():
                            name_lower = child.name.lower()
                            if name_lower == target_lower or name_lower.startswith(target_lower + "-"):
                                measurement_dirs.append(child)
                if not measurement_dirs:
                    continue

                for meas_dir in measurement_dirs:
                    for day_dir in meas_dir.iterdir():
                        if not day_dir.is_dir():
                            continue
                        for hour_dir in day_dir.iterdir():
                            if not hour_dir.is_dir():
                                continue
                            for file_path in hour_dir.glob("*.parquet"):
                                yield file_path

    def _list_influx_tables(self) -> List[str]:
        """
        Retrieve list of measurement tables from InfluxDB v3.

        Executes 'SHOW TABLES' query via HTTP API and filters only tables from
        the 'iox' schema, excluding system and information_schema tables.

        Uses direct HTTP request to /api/v3/query_sql endpoint as it's more
        reliable than the Python client for metadata queries.

        Returns
        -------
        List[str]
            List of measurement/table names available in InfluxDB.
            Returns empty list if InfluxDB is not configured or query fails.

        Examples
        --------
        >>> tables = manager._list_influx_tables()
        >>> print(tables)
        ['SPY-option-tick', 'SPY-option-5m', 'TLRY-option-tick']
        """
        try:
            # Get InfluxDB configuration
            influx_url = self.cfg.influx_url or "http://localhost:8181"
            influx_bucket = self.cfg.influx_bucket
            influx_token = self.cfg.influx_token

            if not (influx_url and influx_bucket and influx_token):
                return []

            # Use HTTP API directly for SHOW TABLES (more reliable than client)
            url = f"{influx_url}/api/v3/query_sql"
            params = {"db": influx_bucket, "q": "SHOW TABLES", "format": "json"}

            response = requests.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {influx_token}"},
                timeout=10
            )

            if response.status_code != 200:
                return []

            # Parse JSON response
            data = response.json()

            # Extract table names from iox schema only
            iox_tables = []
            for row in data:
                if isinstance(row, dict):
                    table_schema = row.get('table_schema', '')
                    table_name = row.get('table_name', '')

                    # Filter only iox schema tables
                    if table_schema == 'iox' and table_name:
                        iox_tables.append(table_name)

            return iox_tables

        except Exception as e:
            return []

    def _influx_measurement_from_base(self, base_path: str) -> str:
        """
        Derive measurement name from our canonical base filename:
        {YYYY-MM-DDTHH-MM-SSZ}-{symbol}-{asset}-{interval}.<sink>
        -> measurement = [{prefix}]{symbol}-{asset}-{interval}
        """
        base = os.path.basename(base_path)
        stem, _ = os.path.splitext(base)
        parts = stem.split("Z-", 1)
        tail = parts[1] if len(parts) == 2 else stem
        return f"{(self.cfg.influx_measure_prefix or '')}{tail}"

    # === >>> INFLUX MEASUREMENT EXISTENCE CHECK (no data proxy) — BEGIN
    def _influx_measurement_exists(self, measurement: str) -> bool:
        """Return True if the InfluxDB measurement/table exists.

        Why this exists
        ---------------
        Do **not** treat `_influx_last_timestamp()` returning None as "table does not exist".
        `MAX(time)` can be NULL even when the table exists but is empty, a write is not yet visible,
        or metadata is lagging.

        Strategy
        --------
        1) Try metadata via InfluxQL: `SHOW MEASUREMENTS` (as in the user's example).
        2) Fallback to the InfluxDB v3 SQL endpoint: `SHOW TABLES`.
        3) Final fallback: run a cheap `SELECT ... LIMIT 1`; if it errors as "unknown table", treat as missing.
        """
        m = str(measurement)

        # (1) InfluxQL metadata query (preferred: matches the user's manual check)
        try:
            cli = self._ensure_influx_client()
            try:
                t = cli.query('SHOW MEASUREMENTS', language='influxql')
            except TypeError:
                # Older client versions may not accept the 'language' kwarg.
                t = cli.query('SHOW MEASUREMENTS')

            df = t.to_pandas() if hasattr(t, 'to_pandas') else t
            if df is not None and len(df) > 0:
                # Common output column is 'name', but keep this resilient.
                if 'name' in df.columns:
                    col = 'name'
                elif 'measurement' in df.columns:
                    col = 'measurement'
                elif 'table_name' in df.columns:
                    col = 'table_name'
                else:
                    col = df.columns[0]
                names = set(df[col].dropna().astype(str).tolist())
                return m in names
        except Exception:
            # Fall back below.
            pass

        # (2) HTTP SQL endpoint fallback (reliable on v3): SHOW TABLES
        try:
            tables = self._list_influx_tables()
            if tables:
                return m in set(map(str, tables))
        except Exception:
            # Fall back below.
            pass

        # (3) Disabled fallback: probing with SELECT against a missing table can trigger server-side panics
        # on some InfluxDB v3 builds during warm-up. If metadata endpoints are unavailable or lagging,
        # assume the table is missing and let the first write create it.
        return False

    # === >>> INFLUX MEASUREMENT EXISTENCE CHECK (no data proxy) — END


    # === >>> INFLUX AVAILABLE-DATES INDEX (per-measurement, unique days) — BEGIN

    def _influx_available_dates_measurement(self, measurement: str) -> str:
        return f"{measurement}_available_dates"

    def _influx_available_dates__ensure_state(self) -> None:
        if not hasattr(self, "_influx_available_dates_cache"):
            self._influx_available_dates_cache = {}
        if not hasattr(self, "_influx_available_dates_loaded"):
            self._influx_available_dates_loaded = set()
        if not hasattr(self, "_influx_available_dates_bootstrapping"):
            self._influx_available_dates_bootstrapping = set()
        if not hasattr(self, "_influx_available_dates_bootstrap_last"):
            self._influx_available_dates_bootstrap_last = {}

    def _influx_available_dates__load_existing(self, measurement: str) -> set[str]:
        """Best-effort load of already indexed days (YYYY-MM-DD)."""
        self._influx_available_dates__ensure_state()
        if measurement in self._influx_available_dates_loaded:
            return self._influx_available_dates_cache.setdefault(measurement, set())

        avail = self._influx_available_dates_measurement(measurement)
        try:
            if not self._influx_measurement_exists(avail):
                return set()
        except Exception:
            # Fall through to query attempt below.
            pass
        df = self._influx_query_dataframe(f'SELECT trade_day FROM "{avail}"')

        s = set()
        if df is not None and (not df.empty) and ("trade_day" in df.columns):
            s = {str(x).strip()[:10] for x in df["trade_day"].dropna().astype(str).tolist()}

        self._influx_available_dates_cache[measurement] = s
        self._influx_available_dates_loaded.add(measurement)
        return s

    def _influx_available_dates_note_days(self, measurement: str, day_isos: set[str]) -> None:
        """Insert only new days into <measurement>_available_dates."""
        if not day_isos:
            return
        existing = self._influx_available_dates__load_existing(measurement)

        new_days = sorted({str(d).strip()[:10] for d in day_isos if d} - existing)
        if not new_days:
            return

        try:
            cli = self._ensure_influx_client()
        except Exception:
            return

        avail = self._influx_available_dates_measurement(measurement)

        # store 1 point per day; trade_day as TAG, ET midnight as timestamp
        lines = []
        for d in new_days:
            ts = pd.Timestamp(d).tz_localize("America/New_York").tz_convert("UTC")
            ts_ns = int(ts.value)
            lines.append(f"{avail},trade_day={d} present=1i {ts_ns}")

        # write with retry (same as main)
        ok = self._influx_retry_mgr.write_with_retry(
            client=cli,
            lines=lines,
            measurement=avail,
            batch_idx=1,
            metadata={"measurement": avail, "total_lines": len(lines), "source": "available_dates"},
        )
        if ok:
            existing.update(new_days)

    def _influx_available_dates_get_all(self, measurement: str) -> set[str]:
        self._influx_available_dates__ensure_state()
        if measurement in self._influx_available_dates_loaded:
            return self._influx_available_dates_cache.setdefault(measurement, set())

        avail = self._influx_available_dates_measurement(measurement)
        df = None

        try:
            if self._influx_measurement_exists(avail):
                df = self._influx_query_dataframe(f'SELECT trade_day FROM "{avail}"')
        except Exception:
            df = None

        if df is not None and (not df.empty) and ("trade_day" in df.columns):
            days = {str(x).strip()[:10] for x in df["trade_day"].dropna().astype(str).tolist()}
            self._influx_available_dates_cache[measurement] = days
            self._influx_available_dates_loaded.add(measurement)
            return days

        # Support table missing or empty -> try one bootstrap (guarded inside the method).
        days = self._influx_available_dates_bootstrap_from_main(measurement)
        if measurement in self._influx_available_dates_loaded:
            return self._influx_available_dates_cache.setdefault(measurement, set())
        return days or set()

    def _influx_available_dates_first_last_day(self, measurement: str):
        avail = self._influx_available_dates_measurement(measurement)
        df = self._influx_query_dataframe(
            f'SELECT MIN(trade_day) AS first_day, MAX(trade_day) AS last_day FROM "{avail}"'
        )
        if df is None or df.empty:
            return None, None
        row = df.iloc[0]
        fd = row.get("first_day")
        ld = row.get("last_day")
        fd = str(fd).strip()[:10] if fd is not None and str(fd).strip() not in ("", "None", "NaT") else None
        ld = str(ld).strip()[:10] if ld is not None and str(ld).strip() not in ("", "None", "NaT") else None
        return fd, ld

    def _influx_available_dates_has_day(self, measurement: str, day_iso: str) -> bool:
        d = str(day_iso).strip()[:10]
        if not d:
            return False
        avail = self._influx_available_dates_measurement(measurement)
        df = self._influx_query_dataframe(
            f"SELECT 1 AS one FROM \"{avail}\" WHERE trade_day = '{d}' LIMIT 1"
        )
        return df is not None and (not df.empty)

    def _influx_available_dates_bootstrap_from_main(
        self,
        measurement: str,
        first_day: str | None = None,
        last_day: str | None = None
    ) -> set[str]:
        """One-time expensive fallback: scan the main measurement to build the unique day-index table.

        Parameters
        ----------
        measurement:
            Main measurement name (e.g., 'QQQ-option-5m').
        first_day, last_day:
            Optional 'YYYY-MM-DD' bounds to restrict the scan. If omitted, scans the whole table.

        Returns
        -------
        set[str]
            Set of 'YYYY-MM-DD' days present in the main measurement (as indexed in <measurement>_available_dates).
        """
        self._influx_available_dates__ensure_state()
        if measurement in self._influx_available_dates_bootstrapping:
            return self._influx_available_dates_cache.setdefault(measurement, set())

        import time
        now = time.time()
        last_attempt = self._influx_available_dates_bootstrap_last.get(measurement, 0.0)
        if now - last_attempt < 2.0:
            return self._influx_available_dates_cache.setdefault(measurement, set())

        self._influx_available_dates_bootstrap_last[measurement] = now
        self._influx_available_dates_bootstrapping.add(measurement)
        try:
            existing = self._influx_available_dates_cache.get(measurement, set())
            existing_loaded = self._influx_available_dates__load_existing(measurement)
            if existing_loaded:
                self._influx_available_dates_cache[measurement] = set(existing_loaded)
                self._influx_available_dates_loaded.add(measurement)
                return set(existing_loaded)

            # Skip scan if the main measurement doesn't exist.
            try:
                if not self._influx_measurement_exists(measurement):
                    self._influx_available_dates_cache[measurement] = set(existing)
                    self._influx_available_dates_loaded.add(measurement)
                    return set(existing)
            except Exception:
                # Best-effort: continue to scan if metadata is unavailable.
                pass

            where_parts: list[str] = []
            if first_day:
                d0 = str(first_day).strip()[:10]
                if d0:
                    where_parts.append(f"time >= to_timestamp('{d0}T00:00:00Z')")
            if last_day:
                d1 = str(last_day).strip()[:10]
                if d1:
                    where_parts.append(f"time <= to_timestamp('{d1}T23:59:59Z')")

            where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

            q = f"""
                SELECT DISTINCT DATE_TRUNC('day', time) AS date
                FROM "{measurement}"
                {where_sql}
                ORDER BY date
            """
            df = self._influx_query_dataframe(q)
            if df is None or df.empty or "date" not in df.columns:
                self._influx_available_dates_cache[measurement] = set(existing)
                self._influx_available_dates_loaded.add(measurement)
                return set(existing)

            days: set[str] = set()
            for v in df["date"].dropna().tolist():
                ts = pd.to_datetime(v, utc=True, errors="coerce")
                if pd.notna(ts):
                    days.add(ts.date().isoformat())

            self._influx_available_dates_note_days(measurement, days)
            all_days = set(existing)
            all_days.update(days)
            self._influx_available_dates_cache[measurement] = all_days
            self._influx_available_dates_loaded.add(measurement)
            return all_days
        finally:
            self._influx_available_dates_bootstrapping.discard(measurement)

    # === >>> INFLUX AVAILABLE-DATES INDEX (per-measurement, unique days) — END


    def _influx_last_timestamp(self, measurement: str):
        # --- >>> WARM-UP GUARD: avoid querying missing measurement — BEGIN
        try:
            if not self._influx_measurement_exists(measurement):
                return None
        except Exception as e:
            print(f"[INFLUX][DEBUG] measurement_exists check failed: {type(e).__name__}: {e}")
            return None
        # --- >>> WARM-UP GUARD — END

        cli = self._ensure_influx_client()
        q = f'SELECT MAX(time) AS last_ts FROM "{measurement}"'
        try:
            t = cli.query(q)
            df = t.to_pandas() if hasattr(t, "to_pandas") else t
            if df is not None and len(df):
                # nome robusto: prova 'last_ts', altrimenti prendi la prima/ultima colonna timestamp-like
                col = "last_ts" if "last_ts" in getattr(df, "columns", []) else (df.columns[-1] if hasattr(df, "columns") and len(df.columns) else None)
                if col is not None:
                    v = df.iloc[0][col]
                    ts = pd.to_datetime(v, utc=True) if pd.notna(v) else None
                    return ts
        except Exception as e:
            # Discriminate between connection/timeout errors vs table-not-found
            err_msg = str(e).lower()

            # Connection/timeout errors → Re-raise (don't create table)
            if any(x in err_msg for x in ['timeout', 'connection', 'unavailable', 'failed to connect', 'unreachable']):
                print(f"[INFLUX][ERROR] Connection/timeout error checking table {measurement}: {e}")
                raise  # Re-raise to signal server is down

            # Table not found errors → Return None (OK to create table)
            if any(x in err_msg for x in ['not found', 'does not exist', 'unknown table', 'no such table']):
                return None  # Table doesn't exist, OK to create

            # Other errors → Return None but log warning
            print(f"[INFLUX][WARN] Unexpected error checking table {measurement}: {e}")
            return None
        return None

    async def _expected_option_combos_for_day(self, symbol: str, day_iso: str) -> Optional[int]:
        """
        Stima il numero atteso di combinazioni (expiration x strike x right) per un giorno di opzioni
        interrogando ThetaData: list_contracts + list_strikes. Restituisce None in caso di errore.
        """
        try:
            ymd = self._td_ymd(day_iso)
            payload, _ = await self.client.option_list_contracts(
                request_type="trade", date=ymd, symbol=symbol, format_type="json"
            )
            expirations: list[str] = []
            if isinstance(payload, dict):
                for col in ("expiration", "expirationDate", "exp"):
                    colv = payload.get(col)
                    if isinstance(colv, list):
                        expirations.extend(colv)
            elif isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        e = item.get("expiration") or item.get("expirationDate") or item.get("exp")
                        if e:
                            expirations.append(e)
            expirations = sorted({str(e).replace("-", "") for e in expirations if str(e).strip()})
            if not expirations:
                return None

            total_strikes = 0
            for exp in expirations:
                try:
                    strikes_payload, _ = await self.client.option_list_strikes(
                        symbol=symbol, expiration=exp, format_type="json"
                    )
                    if isinstance(strikes_payload, dict) and "strikes" in strikes_payload:
                        strikes_list = strikes_payload["strikes"]
                    else:
                        strikes_list = strikes_payload if isinstance(strikes_payload, list) else []
                    total_strikes += len(strikes_list)
                except Exception as e:
                    print(f"[VALIDATION][WARN] list_strikes exp={exp} {day_iso}: {e}")
            if total_strikes == 0:
                return None
            # right=both → consideriamo call e put
            return total_strikes * 2
        except Exception as e:
            print(f"[VALIDATION][WARN] expected combos fetch failed {symbol} {day_iso}: {e}")
            return None


    # --- NEW: day utilities (ET day → UTC bounds, presence/first-ts) ---
    def _influx__et_day_bounds_to_utc(self, day_iso: str):
        """Return (start_utc_iso, end_utc_iso) for ET day [day_iso, day_iso+1)."""

        ET = ZoneInfo("America/New_York")
        d = dt.fromisoformat(day_iso).replace(tzinfo=ET)
        start_utc = d.astimezone(timezone.utc)
        end_utc = (d + timedelta(days=1)).astimezone(timezone.utc)
        return start_utc.isoformat().replace("+00:00", "Z"), end_utc.isoformat().replace("+00:00", "Z")

    def _influx__first_ts_between(self, measurement: str, start_utc_iso: str, end_utc_iso: str):
        """Return first timestamp in [start,end) as pandas.Timestamp(UTC) or None."""
        # --- >>> INFLUX WARM-UP GUARD (avoid querying missing measurement) — BEGIN
        # Querying a non-existing measurement with SELECT can trigger server-side panics on some InfluxDB v3 builds.
        # We therefore check existence via metadata first (best-effort). If uncertain, behave as "no rows".
        try:
            if not self._influx_measurement_exists(measurement):
                return None
        except Exception as e:
            print(f"[RESUME-INFLUX][DEBUG] measurement_exists check failed: {type(e).__name__}: {e}")
            return None
        # --- >>> INFLUX WARM-UP GUARD (avoid querying missing measurement) — END

        cli = self._ensure_influx_client()
        q = (
            f'SELECT MIN(time) AS first_ts FROM "{measurement}" '
            f"WHERE time >= TIMESTAMP '{start_utc_iso}' AND time < TIMESTAMP '{end_utc_iso}'"
        )
        try:
            t = cli.query(q)
            df = t.to_pandas() if hasattr(t, "to_pandas") else t
            if df is not None and len(df) and "first_ts" in df.columns:
                v = df["first_ts"].iloc[0]
                out = pd.to_datetime(v, utc=True) if pd.notna(v) else None
                print(f"[RESUME-INFLUX][CHECK] meas={measurement} first_ts_between={out} "
                      f"range=[{start_utc_iso},{end_utc_iso})")
                return out
        except Exception as e:
            print(f"[RESUME-INFLUX][DEBUG] first_ts_between error: {type(e).__name__}: {e}")
            return None
        return None


    def _influx_day_has_any(self, measurement: str, day_iso: str) -> bool:
        # Fast check via available_dates table
        return self._influx_available_dates_has_day(measurement, day_iso)

    def _influx_first_ts_for_et_day(self, measurement: str, day_iso: str):
        """Primo ts del giorno ET in UTC, oppure None."""
        s, e = self._influx__et_day_bounds_to_utc(day_iso)
        return self._influx__first_ts_between(measurement, s, e)


    
    def _influx_last_ts_between(self, measurement: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp):
        """
        Ritorna l'ultimo timestamp (tz-aware UTC) presente in [start_utc, end_utc) per il measurement,
        oppure None se vuoto. Usa SQL Influx v3.
        """
        # --- >>> INFLUX WARM-UP GUARD (avoid querying missing measurement) — BEGIN
        # Querying a non-existing measurement with SELECT can trigger server-side panics on some InfluxDB v3 builds.
        # We therefore check existence via metadata first (best-effort). If uncertain, behave as "no rows".
        try:
            if not self._influx_measurement_exists(measurement):
                return None
        except Exception as e:
            print(f"[RESUME-INFLUX][DEBUG] measurement_exists check failed: {type(e).__name__}: {e}")
            return None
        # --- >>> INFLUX WARM-UP GUARD (avoid querying missing measurement) — END

        # costruisci client/query function come negli altri helper
        client = getattr(self, "_influx", None)
        if client is None and hasattr(self, "client_influx"):
            client = self.client_influx
        if client is None:
            client = InfluxDBClient3(host=self.cfg.influx_url, token=self.cfg.influx_token,
                                     database=self.cfg.influx_bucket, org=None)

        s = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        e = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        q = (
            f'SELECT MAX(time) AS t FROM "{measurement}" '
            f"WHERE time >= TIMESTAMP '{s}' AND time < TIMESTAMP '{e}'"
        )
        try:
            t = client.query(q)
            df = t.to_pandas() if hasattr(t, "to_pandas") else None
            if df is None or df.empty or "t" not in df.columns:
                return None
            ts = pd.to_datetime(df["t"].iloc[0], utc=True, errors="coerce")
            return ts if pd.notna(ts) else None
        except Exception as ex:
            print(f"[RESUME-INFLUX][WARN] last_ts_between query failed: {ex}")
            return None


    # ----------------------- OI CACHE METHODS (INTRADAY OPTIMIZATION) -----------------------

    def _get_oi_cache_path(self, symbol: str, date_ymd: str) -> str:
        """Get the file path for cached OI data."""
        # Format: {root_dir}/.oi_cache/{symbol}/{YYYYMMDD}.parquet
        cache_dir = os.path.join(self.cfg.root_dir, self.cfg.oi_cache_dir, symbol)
        os.makedirs(cache_dir, exist_ok=True)
        return os.path.join(cache_dir, f"{date_ymd}.parquet")

    def _check_oi_cache(self, symbol: str, date_ymd: str) -> bool:
        """
        Check if Open Interest data for the given symbol and date is already cached.

        OI data is cached in local Parquet files to avoid redundant API calls during
        intraday downloads. Since OI only changes at EOD, the same data can be reused
        throughout the trading day.

        Works with ANY sink (csv, parquet, influxdb) - cache is independent of main data storage.

        Parameters
        ----------
        symbol : str
            Ticker symbol (e.g., "AAPL", "SPY")
        date_ymd : str
            Date in YYYYMMDD format (e.g., "20250102")

        Returns
        -------
        bool
            True if OI data for this date is cached, False otherwise
        """
        if not self.cfg.enable_oi_caching:
            return False

        cache_path = self._get_oi_cache_path(symbol, date_ymd)
        exists = os.path.exists(cache_path)

        if exists:
            print(f"[OI-CACHE][CHECK] {symbol} date={date_ymd} - Cache file found: {cache_path}")

        return exists

    def _load_oi_from_cache(self, symbol: str, date_ymd: str) -> Optional[pd.DataFrame]:
        """
        Load cached Open Interest data from local Parquet file for the given symbol and date.

        Returns RAW data exactly as ThetaData API would return it for cache transparency.
        When requesting OI for date X, returns OI from previous trading session with
        original API column names (timestamp, open_interest, expiration, strike, right, etc.).

        Works with ANY sink (csv, parquet, influxdb) - cache is independent of main data storage.

        Parameters
        ----------
        symbol : str
            Ticker symbol (e.g., "AAPL", "SPY")
        date_ymd : str
            Date in YYYYMMDD format (e.g., "20250102") - the requested date

        Returns
        -------
        pd.DataFrame or None
            RAW DataFrame with columns: timestamp, open_interest, expiration, strike, right,
            symbol, root, option_symbol. Returns None if cache miss or error.
        """
        if not self.cfg.enable_oi_caching:
            return None

        cache_path = self._get_oi_cache_path(symbol, date_ymd)

        if not os.path.exists(cache_path):
            print(f"[OI-CACHE][LOAD] {symbol} date={date_ymd} - Cache file not found")
            return None

        try:
            df = pd.read_parquet(cache_path)

            if df is None or df.empty:
                print(f"[OI-CACHE][LOAD] {symbol} date={date_ymd} - Cache file is empty")
                return None

            # Remove request_date column (used only for cache querying)
            if 'request_date' in df.columns:
                df = df.drop(columns=['request_date'])

            print(f"[OI-CACHE][LOAD] {symbol} date={date_ymd} - Loaded {len(df)} OI records from cache")
            return df

        except Exception as e:
            print(f"[OI-CACHE][LOAD][WARN] Failed to load cache for {symbol}/{date_ymd}: {e}")
            return None

    def _save_oi_to_cache(self, symbol: str, date_ymd: str, oi_df: pd.DataFrame) -> bool:
        """
        Save Open Interest data to local Parquet file for reuse during intraday downloads.

        Saves RAW data from ThetaData API + request_date column for cache transparency.
        Cache stores TWO dates:
        1. request_date (query date, e.g., "20250102") - used to retrieve data
        2. timestamp (OI effective date, e.g., "2025-01-01") - from API response

        This mirrors ThetaData behavior: requesting OI for 2025-01-02 returns OI from
        previous trading session (2025-01-01 EOD).

        Works with ANY sink (csv, parquet, influxdb) - cache is independent of main data storage.

        Parameters
        ----------
        symbol : str
            Ticker symbol (e.g., "AAPL", "SPY")
        date_ymd : str
            Date in YYYYMMDD format (e.g., "20250102") - the requested date
        oi_df : pd.DataFrame
            DataFrame with columns: request_date, timestamp, open_interest,
            expiration, strike, right, symbol, root, option_symbol

        Returns
        -------
        bool
            True if save succeeded, False otherwise
        """
        if not self.cfg.enable_oi_caching or oi_df is None or oi_df.empty:
            return False

        cache_path = self._get_oi_cache_path(symbol, date_ymd)

        try:
            # Save RAW data without transformations for cache transparency
            oi_df.to_parquet(cache_path, index=False, compression='snappy')
            print(f"[OI-CACHE][SAVE] {symbol} date={date_ymd} - Cached {len(oi_df)} OI records to {cache_path}")
            return True

        except Exception as e:
            print(f"[OI-CACHE][SAVE][WARN] Failed to save cache for {symbol}/{date_ymd}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _get_cached_first_date(self, asset: str, symbol: str, req_type: str) -> Optional[str]:
        key = self._cache_key(asset, symbol, req_type)
        val = self._coverage_cache.get(key, {}).get("first_date")
        if val is None:
            return None
        if isinstance(val, (list, tuple)):
            return self._extract_first_date_from_any(list(val))
        if isinstance(val, dict):
            return self._extract_first_date_from_any([val])
        if isinstance(val, str):
            return self._normalize_date_str(val)
        return None

    def _set_cached_first_date(self, asset: str, symbol: str, req_type: str, first_date: str) -> None:
        fd = self._normalize_date_str(first_date) if isinstance(first_date, str) else None
        if not fd and isinstance(first_date, (list, tuple)):
            fd = self._extract_first_date_from_any(list(first_date))
        elif not fd and isinstance(first_date, dict):
            fd = self._extract_first_date_from_any([first_date])
        if not fd:
            return
        self._coverage_cache[self._cache_key(asset, symbol, req_type)] = {"first_date": fd}

    # ----------------------- JSON & DATE UTILS -----------------------

    def _load_cache_file(self) -> Dict[str, Dict[str, str]]:
        """Loads the coverage cache from disk containing first-date and last-date metadata for data series.

        This method reads a JSON file that stores metadata about synchronized data series, including the
        first and last dates of saved data. This cache enables efficient resume logic by avoiding redundant
        discovery and file scanning.

        Parameters
        ----------
        None
            Uses the instance's _cache_path attribute to locate the cache file.

        Returns
        -------
        dict of str to dict
            A dictionary mapping cache keys to metadata dictionaries. Each metadata dict contains keys like
            'series_last_day' and 'first_saved_day' with 'YYYY-MM-DD' date values. Returns an empty dict
            if the cache file doesn't exist or is corrupted.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - __init__() during manager initialization to load existing cache
        # - Methods that need to check cached first/last dates for series
        """
        path = getattr(self, "_cache_path", None)
        if not path or not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            # Corrupted or unreadable -> start fresh
            return {}
    
    def _save_cache_file(self) -> None:
        """Persists the coverage cache to disk atomically using a temporary file and atomic replace.

        This method safely writes the in-memory coverage cache to disk by first writing to a temporary
        file, then atomically replacing the existing cache file. This prevents corruption from interrupted
        writes and ensures cache consistency.

        Parameters
        ----------
        None
            Uses the instance's _cache_path and _coverage_cache attributes.

        Returns
        -------
        None
            The cache is written to disk but no value is returned.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - run() after completing all synchronization tasks
        # - clear_first_date_cache() after removing a cache entry
        # - _touch_cache() after updating cache metadata
        """
        cache_dir = os.path.dirname(self._cache_path)
        os.makedirs(cache_dir, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(prefix="cov_", suffix=".json", dir=cache_dir)
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(self._coverage_cache, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self._cache_path)
        except Exception:
            # Best-effort: if atomic replace fails, try direct write (last resort)
            try:
                with open(self._cache_path, "w", encoding="utf-8") as f:
                    json.dump(self._coverage_cache, f, ensure_ascii=False, indent=2)
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
    
    def _touch_cache(
        self,
        asset: str,
        symbol: str,
        interval: str,
        sink: str,
        *,
        first_day: Optional[str] = None,
        last_day: Optional[str] = None,
    ) -> None:
        """Updates the coverage cache entry for a data series, expanding the known date coverage window.

        This method updates the cached first and last dates for a series, ensuring the coverage window
        only expands (never shrinks). It's used to track the extent of synchronized data without
        needing to scan files on every run.

        Parameters
        ----------
        asset : str
            The asset type (e.g., 'stock', 'option', 'index').
        symbol : str
            The ticker symbol or root symbol.
        interval : str
            The bar interval (e.g., '1d', '5m', '1h').
        sink : str
            The sink type (e.g., 'csv', 'parquet', 'influxdb').
        first_day : str or None, optional
            Default: None

            The first date with data in 'YYYY-MM-DD' format. If provided and earlier than the cached
            first date, the cache is updated to this earlier date.
        last_day : str or None, optional
            Default: None

            The last date with data in 'YYYY-MM-DD' format. If provided and later than the cached
            last date, the cache is updated to this later date.

        Returns
        -------
        None
            The cache is updated in memory but not immediately persisted to disk.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _download_and_store_options() after successfully downloading option data for a day
        # - _download_and_store_equity_or_index() after downloading stock/index data
        # - Methods that modify data series and need to update coverage metadata
        """
        key = self._cache_key(asset, symbol, interval, sink)
        entry = self._coverage_cache.get(key, {})
        fd = entry.get("first_saved_day")
        ld = entry.get("series_last_day")
    
        if first_day:
            fd = first_day if (fd is None or first_day < fd) else fd
        if last_day:
            ld = last_day if (ld is None or last_day > ld) else ld
    
        new_entry = {}
        if fd is not None:
            new_entry["first_saved_day"] = fd
        if ld is not None:
            new_entry["series_last_day"] = ld
    
        if new_entry:
            self._coverage_cache[key] = new_entry

    

    def _cache_key(self, asset: str, symbol: str, interval: str, sink: str) -> str:
        """Constructs a standardized cache key for identifying a specific data series.

        This method creates a unique string key by combining asset, symbol, interval, and sink parameters,
        which is used for caching first-date coverage and other series metadata.

        Parameters
        ----------
        asset : str
            The asset type (e.g., 'stock', 'option', 'index').
        symbol : str
            The ticker symbol or root symbol.
        interval : str
            The bar interval (e.g., '1d', '5m', '1h').
        sink : str
            The sink type (e.g., 'csv', 'parquet', 'influxdb').

        Returns
        -------
        str
            A colon-separated cache key string in the format 'asset:symbol:interval:sink'.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - clear_first_date_cache() to delete specific cache entries
        # - _touch_cache() to update cache entries
        # - _load_cache_file() and _save_cache_file() for cache persistence operations
        """
        return f"{asset}:{symbol}:{interval}:{sink.lower()}"
    

    # =========================================================================
    # (END)
    # DATA SYNCHRONIZATION
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # MARKET SCREENING
    # =========================================================================
    async def screen_option_oi_concentration(
        self,
        symbols: list[str],
        day_iso: str,
        threshold_pct: float = 0.30,
        scope: Literal["strike", "strike_and_right"] = "strike",
        right: Literal["call", "put", "both"] = "both",
        min_chain_oi: int = 5_000,
        min_contract_oi: int = 500,
        min_chain_contracts: Optional[int] = None,
    ) -> pd.DataFrame:
        """Screen for abnormal open interest concentration by symbol and expiration.

        This method identifies strikes or contracts with unusually high open interest concentration
        within their expiration chains. It pulls prior-day OI data from the ThetaData API and flags
        positions that exceed concentration thresholds, which may indicate large institutional
        positions, hedging activity, or potential market-moving events.

        Parameters
        ----------
        symbols : list[str]
            List of ticker symbols to screen (e.g., ["SPY", "AAPL", "TLRY"]).
        day_iso : str
            The trading date for the screening in ISO format "YYYY-MM-DD".
            This pulls prior-day OI data for this date.
        threshold_pct : float, optional
            Minimum concentration percentage to flag (default: 0.30 = 30%).
            Rows where (contract_oi / chain_oi) >= threshold_pct are flagged.
        scope : Literal["strike", "strike_and_right"], optional
            Default: "strike"
            - "strike": Aggregate calls and puts together (C+P summed per strike)
            - "strike_and_right": Keep calls and puts separate (analyze each contract)
        right : Literal["call", "put", "both"], optional
            Default: "both"
            Filter by option type: "call", "put", or "both".
        min_chain_oi : int, optional
            Minimum total open interest for the entire expiration chain (default: 5,000).
            Chains below this threshold are excluded from screening.
        min_contract_oi : int, optional
            Minimum open interest for individual contracts/strikes to be flagged (default: 500).
        min_chain_contracts : int, optional
            Minimum number of distinct contracts in the chain (default: 5 for strike scope,
            10 for strike_and_right scope). Helps filter out illiquid/thin chains.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns:
            - symbol : str - Ticker symbol
            - expiration : str - Expiration date (ISO format)
            - strike : float - Strike price
            - right : str - Option type ("call" or "put")
            - contract_oi : int - Open interest for this contract/strike
            - chain_oi : int - Total OI for the entire expiration chain
            - share_pct : float - Concentration percentage (contract_oi / chain_oi * 100)
            - contracts_in_chain : int - Number of distinct contracts in this expiration
            - rank_in_expiration : int - Rank by OI within this expiration (1 = highest)
            - source_url : str - API URL used to fetch the data

        Example Usage
        -------------
        # Screen for high OI concentration in SPY and QQQ
        df = await manager.screen_option_oi_concentration(
            symbols=["SPY", "QQQ"],
            day_iso="2025-11-07",
            threshold_pct=0.25,
            scope="strike",
            min_chain_oi=10_000
        )

        # Find strikes with >30% OI concentration
        high_concentration = df[df['share_pct'] >= 30.0]
        print(high_concentration[['symbol', 'expiration', 'strike', 'share_pct']])
        """
        async def _fetch_oi(sym: str):
            csv_txt, url = await self.client.option_history_open_interest(
                symbol=sym, expiration="*", date=day_iso, strike="*", right=right, format_type="csv"
            )
            return sym, csv_txt, url

        eff_min_chain_contracts = (
            min_chain_contracts
            if min_chain_contracts is not None
            else (5 if scope == "strike" else 10)
        )
    
        tasks = [asyncio.create_task(_fetch_oi(s)) for s in symbols]
        results = []
    
        for fut in asyncio.as_completed(tasks):
            try:
                sym, csv_txt, url = await fut
            except Exception:
                continue
            if not csv_txt:
                continue
    
            df = pd.read_csv(io.StringIO(csv_txt), dtype=str)
            if df is None or df.empty:
                continue
    
            # Robust column normalization
            # expiration
            if "expiration" not in df.columns:
                # try fallback names (rare)
                for alt in ("exp", "expiry"):
                    if alt in df.columns:
                        df = df.rename(columns={alt: "expiration"})
                        break
    
            # strike
            if "strike" not in df.columns:
                for alt in ("option_strike", "OPTION_STRIKE"):
                    if alt in df.columns:
                        df = df.rename(columns={alt: "strike"})
                        break
    
            # right
            if "right" not in df.columns and "option_right" in df.columns:
                df = df.rename(columns={"option_right": "right"})
    
            # open_interest
            oi_col = next((c for c in ("open_interest", "oi", "OI", "openInterest") if c in df.columns), None)
            if oi_col is None:
                continue
            df = df.rename(columns={oi_col: "oi"})
    
            # Normalize right values if present
            if "right" in df.columns:
                df["right"] = df["right"].map(
                    {"C": "call", "P": "put", "CALL": "call", "PUT": "put", "call": "call", "put": "put"}
                ).fillna(df.get("right"))
    
            # Aggregate by requested scope
            if scope == "strike":
                # Sum C+P at the same strike
                grp_cols = ["expiration", "strike"]
            else:
                grp_cols = ["expiration", "strike", "right"]
    
            dfa = df.groupby(grp_cols, as_index=False, dropna=False)["oi"].sum().rename(columns={"oi": "contract_oi"})
            # Count distinct contracts in the chain (per expiration)
            chain_counts = dfa.groupby("expiration")["contract_oi"].transform("size")
            # Chain total OI
            chain_tot = dfa.groupby("expiration")["contract_oi"].transform("sum")
            dfa["chain_oi"] = chain_tot
            dfa["contracts_in_chain"] = chain_counts
            dfa["share_pct"] = (dfa["contract_oi"] / dfa["chain_oi"]).astype(float)
    
            # Guards + threshold
            mask = (
                (dfa["share_pct"] >= threshold_pct)
                & (dfa["contract_oi"] >= min_contract_oi)
                & (dfa["chain_oi"] >= min_chain_oi)
                & (dfa["contracts_in_chain"] >= eff_min_chain_contracts)
            )
            hits = dfa.loc[mask].copy()
            if hits.empty:
                continue
    
            # Ranking within expiration by share
            hits["rank_in_expiration"] = hits.groupby("expiration")["share_pct"].rank(method="dense", ascending=False).astype(int)
    
            # Add symbol, right column if missing (scope=strike => synthetic "both")
            hits.insert(0, "symbol", sym)
            if "right" not in hits.columns:
                hits["right"] = "both"
    
            # Useful for traceability
            hits["source_url"] = url
            results.append(hits)
    
        if not results:
            return pd.DataFrame(columns=[
                "symbol","expiration","strike","right","contract_oi","chain_oi","share_pct",
                "contracts_in_chain","rank_in_expiration","source_url"
            ])
    
        out = pd.concat(results, ignore_index=True)
        # Order by severity
        out = out.sort_values(["share_pct","chain_oi","contract_oi"], ascending=[False, False, False]).reset_index(drop=True)
        return out
    
    
    async def screen_option_volume_concentration(
        self,
        symbols: list[str],
        day_iso: str,
        threshold_pct: float = 0.25,
        scope: Literal["strike", "contract"] = "strike",
        right: Literal["call", "put", "both"] = "both",
        min_chain_volume: int = 5_000,
        min_contract_volume: int = 500,
        min_chain_contracts: Optional[int] = None,
    ) -> pd.DataFrame:
        """Screen for abnormal daily volume concentration by symbol and expiration.

        This method identifies strikes or contracts with unusually high trading volume concentration
        within their expiration chains. It pulls end-of-day (EOD) volume data from the ThetaData API
        and flags positions that exceed concentration thresholds, which may indicate unusual trading
        activity, large institutional trades, or significant market interest.

        Parameters
        ----------
        symbols : list[str]
            List of ticker symbols to screen (e.g., ["SPY", "AAPL", "TLRY"]).
        day_iso : str
            The trading date for the screening in ISO format "YYYY-MM-DD".
            Pulls EOD data for this specific date.
        threshold_pct : float, optional
            Minimum concentration percentage to flag (default: 0.25 = 25%).
            Rows where (contract_volume / chain_volume) >= threshold_pct are flagged.
        scope : Literal["strike", "contract"], optional
            Default: "strike"
            - "strike": Aggregate calls and puts together (C+P summed per strike)
            - "contract": Keep calls and puts separate (analyze each contract individually)
        right : Literal["call", "put", "both"], optional
            Default: "both"
            Filter by option type: "call", "put", or "both".
        min_chain_volume : int, optional
            Minimum total daily volume for the entire expiration chain (default: 5,000).
            Chains below this threshold are excluded from screening.
        min_contract_volume : int, optional
            Minimum daily volume for individual contracts/strikes to be flagged (default: 500).
        min_chain_contracts : int, optional
            Minimum number of distinct contracts in the chain (default: 5 for strike scope,
            10 for contract scope). Helps filter out illiquid/thin chains.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns:
            - symbol : str - Ticker symbol
            - expiration : str - Expiration date (ISO format)
            - strike : float - Strike price
            - right : str - Option type ("call" or "put")
            - contract_volume : int - Daily volume for this contract/strike
            - chain_volume : int - Total daily volume for the entire expiration chain
            - share_pct : float - Concentration percentage (contract_volume / chain_volume * 100)
            - contracts_in_chain : int - Number of distinct contracts in this expiration
            - rank_in_expiration : int - Rank by volume within this expiration (1 = highest)
            - source_url : str - API URL used to fetch the data

        Example Usage
        -------------
        # Screen for high volume concentration in SPY and QQQ
        df = await manager.screen_option_volume_concentration(
            symbols=["SPY", "QQQ"],
            day_iso="2025-11-07",
            threshold_pct=0.20,
            scope="strike",
            min_chain_volume=10_000
        )

        # Find strikes with >25% volume concentration
        high_volume = df[df['share_pct'] >= 25.0]
        print(high_volume[['symbol', 'expiration', 'strike', 'contract_volume', 'share_pct']])
        """
        async def _fetch_eod(sym: str):
            csv_txt, url = await self.client.option_history_eod(
                symbol=sym, expiration="*", start_date=day_iso, end_date=day_iso, strike="*", right=right, format_type="csv"
            )
            return sym, csv_txt, url

        eff_min_chain_contracts = (
            min_chain_contracts
            if min_chain_contracts is not None
            else (5 if scope == "strike" else 10)
        )
    
        tasks = [asyncio.create_task(_fetch_eod(s)) for s in symbols]
        results = []
    
        for fut in asyncio.as_completed(tasks):
            try:
                sym, csv_txt, url = await fut
            except Exception:
                continue
            if not csv_txt:
                continue
    
            df = pd.read_csv(io.StringIO(csv_txt), dtype=str)
            if df is None or df.empty:
                continue
    
            # Columns
            if "expiration" not in df.columns:
                for alt in ("exp", "expiry"):
                    if alt in df.columns:
                        df = df.rename(columns={alt: "expiration"})
                        break
            if "strike" not in df.columns:
                for alt in ("option_strike", "OPTION_STRIKE"):
                    if alt in df.columns:
                        df = df.rename(columns={alt: "strike"})
                        break
            if "right" not in df.columns and "option_right" in df.columns:
                df = df.rename(columns={"option_right": "right"})
    
            vol_col = next((c for c in ("volume", "vol", "Volume") if c in df.columns), None)
            if vol_col is None:
                continue
            df = df.rename(columns={vol_col: "volume"})
    
            if "right" in df.columns:
                df["right"] = df["right"].map(
                    {"C": "call", "P": "put", "CALL": "call", "PUT": "put", "call": "call", "put": "put"}
                ).fillna(df.get("right"))
    
            if scope == "strike":
                grp_cols = ["expiration", "strike"]
            else:
                grp_cols = ["expiration", "strike", "right"]
    
            dfa = df.groupby(grp_cols, as_index=False, dropna=False)["volume"].sum().rename(columns={"volume": "contract_volume"})
            chain_counts = dfa.groupby("expiration")["contract_volume"].transform("size")
            chain_tot = dfa.groupby("expiration")["contract_volume"].transform("sum")
            dfa["chain_volume"] = chain_tot
            dfa["contracts_in_chain"] = chain_counts
            dfa["share_pct"] = (dfa["contract_volume"] / dfa["chain_volume"]).astype(float)
    
            mask = (
                (dfa["share_pct"] >= threshold_pct)
                & (dfa["contract_volume"] >= min_contract_volume)
                & (dfa["chain_volume"] >= min_chain_volume)
                & (dfa["contracts_in_chain"] >= eff_min_chain_contracts)
            )
            hits = dfa.loc[mask].copy()
            if hits.empty:
                continue
    
            hits["rank_in_expiration"] = hits.groupby("expiration")["share_pct"].rank(method="dense", ascending=False).astype(int)
            hits.insert(0, "symbol", sym)
            if "right" not in hits.columns:
                hits["right"] = "both"
            hits["source_url"] = url
            results.append(hits)
    
        if not results:
            return pd.DataFrame(columns=[
                "symbol","expiration","strike","right","contract_volume","chain_volume","share_pct",
                "contracts_in_chain","rank_in_expiration","source_url"
            ])
    
        out = pd.concat(results, ignore_index=True)
        out = out.sort_values(["share_pct","chain_volume","contract_volume"], ascending=[False, False, False]).reset_index(drop=True)
        return out



    # =========================================================================
    # (END)
    # MARKET SCREENING
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # DATA QUALITY VALIDATION
    # =========================================================================
    def generate_duplicate_report(
        self,
        asset: str,
        symbol: str,
        intervals: Optional[List[str]] = None,
        sinks: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sample_limit: int = 5,
        verbose: bool = True
    ) -> dict:
        """Generate a comprehensive duplicate detection report across multiple intervals and sinks.

        This method creates a complete duplicate analysis report for a given symbol by iterating through
        all specified time intervals (or all available intervals if not specified) and all storage sinks
        (or all available sinks if not specified). For each combination of interval and sink, it calls
        check_duplicates_multi_day() to perform the analysis and aggregates the results into a unified
        report. This provides a holistic view of data quality across all dimensions, making it easy to
        identify which intervals or sinks have duplicate issues. The report includes summary statistics,
        per-interval-sink breakdowns, and actionable insights for data cleanup.

        Parameters
        ----------
        asset : str
            The asset type to analyze. Possible values: "option", "stock", "index".
            Determines which data structure to analyze across all intervals and sinks.
        symbol : str
            The ticker symbol or root symbol to analyze (e.g., "TLRY", "AAPL", "SPY", "ES").
            The report will cover all data for this symbol across specified intervals and sinks.
        intervals : list of str, optional
            Default: None (checks all available intervals)
            List of bar intervals to check (e.g., ["tick", "1m", "5m", "1h", "1d"]).
            When None, automatically detects and checks all intervals that have data for this
            symbol in any of the specified sinks. Common intervals: "tick", "1m", "5m", "10m",
            "15m", "30m", "1h", "1d".
        sinks : list of str, optional
            Default: None (checks all available sinks)
            List of storage backends to check. Possible values: ["influxdb", "csv", "parquet"].
            When None, checks all three sink types. Each sink may have different data coverage
            and duplicate patterns depending on ingestion history.
        start_date : str, optional
            Default: None (auto-detect from data)
            The start date for the analysis in ISO format "YYYY-MM-DD" (e.g., "2025-11-03").
            When None, uses the earliest available date across all interval-sink combinations.
            Applied uniformly to all checks for consistency.
        end_date : str, optional
            Default: None (auto-detect from data)
            The end date for the analysis in ISO format "YYYY-MM-DD" (e.g., "2025-11-07").
            When None, uses the latest available date across all interval-sink combinations.
            Applied uniformly to all checks for consistency.
        sample_limit : int, optional
            Default: 5
            Maximum number of duplicate key examples to collect per day per interval-sink
            combination. Lower values reduce memory usage and report size. Set to 0 to
            disable example collection.
        verbose : bool, optional
            Default: True
            If True, prints detailed progress and results for each interval-sink combination
            as they are analyzed, plus a final summary table. If False, runs silently and
            only returns the results dictionary. Useful for batch processing and automation.

        Returns
        -------
        dict
            A comprehensive report dictionary containing:
            - 'symbol' (str): The analyzed symbol
            - 'asset' (str): The asset type
            - 'date_range' (dict): The date range analyzed with 'start' and 'end'
            - 'intervals_checked' (list): List of intervals that were analyzed
            - 'sinks_checked' (list): List of sinks that were analyzed
            - 'total_combinations' (int): Total number of interval-sink combinations checked
            - 'combinations_with_duplicates' (int): Number of combinations that have duplicates
            - 'results' (list): List of per-combination results, each containing:
                - 'interval' (str): The interval checked
                - 'sink' (str): The sink checked
                - 'days_analyzed' (int): Number of trading days checked
                - 'total_rows' (int): Total rows in this combination
                - 'total_duplicates' (int): Total duplicates found
                - 'duplicate_rate' (float): Percentage of duplicates
                - 'days_with_duplicates' (int): Number of days with duplicates
                - 'status' (str): "CLEAN" or "DUPLICATES_FOUND"
            - 'summary' (dict): Aggregate statistics across all combinations:
                - 'total_rows_all' (int): Sum of rows across all combinations
                - 'total_duplicates_all' (int): Sum of duplicates across all combinations
                - 'overall_duplicate_rate' (float): Global duplicate percentage
                - 'worst_combination' (dict): Interval-sink with highest duplicate rate
                - 'cleanest_sinks' (list): Sinks with no duplicates
                - 'problematic_intervals' (list): Intervals with duplicates in any sink

        Example Usage
        -------------
        # Example 1: Full comprehensive report (all intervals, all sinks, all dates)
        manager = ThetaSyncManager(cfg, client=client)
        report = manager.generate_duplicate_report(
            asset="option",
            symbol="TLRY",
            verbose=True
        )
        print(f"Checked {report['total_combinations']} combinations")
        print(f"Found issues in {report['combinations_with_duplicates']} combinations")

        # Example 2: Specific intervals and date range
        report = manager.generate_duplicate_report(
            asset="stock",
            symbol="AAPL",
            intervals=["1m", "5m", "1h"],
            sinks=["csv", "parquet"],
            start_date="2025-11-01",
            end_date="2025-11-30",
            verbose=True
        )

        # Example 3: Silent mode for scripting
        report = manager.generate_duplicate_report(
            asset="option",
            symbol="TLRY",
            intervals=["tick", "5m"],
            sinks=["influxdb"],
            verbose=False
        )
        if report['combinations_with_duplicates'] > 0:
            print("⚠️  Duplicates found! Check report['results'] for details")

        # Example 4: Compare all sinks for a specific interval
        report = manager.generate_duplicate_report(
            asset="option",
            symbol="TLRY",
            intervals=["5m"],  # Only check 5-minute data
            verbose=True
        )
        for result in report['results']:
            print(f"{result['sink']}: {result['total_duplicates']} duplicates")

        # Example 5: Process results programmatically
        report = manager.generate_duplicate_report(
            asset="stock",
            symbol="AAPL",
            verbose=False
        )
        worst = report['summary']['worst_combination']
        if worst:
            print(f"Worst: {worst['interval']} in {worst['sink']}: "
                  f"{worst['duplicate_rate']}% duplicates")
        """

        # Determine which intervals to check
        if intervals is None:
            # Auto-detect: find all intervals that have data
            intervals = []
            test_sinks = sinks if sinks else ["csv", "parquet", "influxdb"]
            for sink in test_sinks:
                try:
                    sink_dir = os.path.join(self.cfg.root_dir, "data", asset, symbol)
                    if os.path.exists(sink_dir):
                        for interval_name in os.listdir(sink_dir):
                            interval_path = os.path.join(sink_dir, interval_name)
                            if os.path.isdir(interval_path) and interval_name not in intervals:
                                intervals.append(interval_name)
                except Exception:
                    pass

            if not intervals:
                intervals = ["tick", "1m", "5m", "10m", "15m", "30m", "1h", "1d"]  # Default fallback

        # Determine which sinks to check
        if sinks is None:
            sinks = ["csv", "parquet", "influxdb"]

        if verbose:
            print(f"\n{'='*80}")
            print(f"COMPREHENSIVE DUPLICATE REPORT")
            print(f"Symbol: {symbol} ({asset})")
            print(f"Intervals: {', '.join(intervals)}")
            print(f"Sinks: {', '.join(sinks)}")
            if start_date and end_date:
                print(f"Date range: {start_date} to {end_date}")
            else:
                print(f"Date range: Auto-detect")
            print(f"{'='*80}\n")

        # Collect results for each combination
        results = []
        total_rows_all = 0
        total_duplicates_all = 0
        combinations_with_duplicates = 0

        for interval in intervals:
            for sink in sinks:
                if verbose:
                    print(f"\n--- Checking {interval} in {sink.upper()} ---")

                try:
                    result = self.check_duplicates_multi_day(
                        asset=asset,
                        symbol=symbol,
                        interval=interval,
                        sink=sink,
                        start_date=start_date,
                        end_date=end_date,
                        sample_limit=sample_limit,
                        verbose=False  # Suppress individual reports
                    )

                    # Extract list of dates with duplicates
                    dates_with_duplicates = [
                        day_result['date']
                        for day_result in result.get("daily_results", [])
                        if day_result.get('duplicates', 0) > 0
                    ]

                    combination_result = {
                        "interval": interval,
                        "sink": sink,
                        "days_analyzed": result.get("days_analyzed", 0),
                        "total_rows": result.get("total_rows", 0),
                        "total_duplicates": result.get("total_duplicates", 0),
                        "duplicate_rate": result.get("global_duplicate_rate", 0.0),
                        "days_with_duplicates": result.get("days_with_duplicates", 0),
                        "dates_with_duplicates": dates_with_duplicates,
                        "status": result.get("summary", {}).get("status", "unknown")
                    }

                    results.append(combination_result)

                    total_rows_all += combination_result["total_rows"]
                    total_duplicates_all += combination_result["total_duplicates"]

                    if combination_result["total_duplicates"] > 0:
                        combinations_with_duplicates += 1

                    if verbose:
                        status_icon = "✅" if combination_result["total_duplicates"] == 0 else "⚠️"
                        print(f"  {status_icon} Rows: {combination_result['total_rows']:,}, "
                              f"Duplicates: {combination_result['total_duplicates']:,} "
                              f"({combination_result['duplicate_rate']:.2f}%)")

                        # Print dates with duplicates if any
                        if dates_with_duplicates:
                            dates_str = ", ".join(dates_with_duplicates)
                            print(f"      Dates with duplicates: {dates_str}")

                except Exception as e:
                    if verbose:
                        print(f"  ❌ Error: {str(e)}")
                    results.append({
                        "interval": interval,
                        "sink": sink,
                        "error": str(e),
                        "status": "error"
                    })

        # Calculate summary statistics
        overall_duplicate_rate = (total_duplicates_all / total_rows_all * 100) if total_rows_all > 0 else 0.0

        # Find worst combination
        worst_combination = None
        max_dup_rate = 0.0
        for result in results:
            if result.get("duplicate_rate", 0) > max_dup_rate:
                max_dup_rate = result["duplicate_rate"]
                worst_combination = result

        # Find cleanest sinks
        cleanest_sinks = []
        for sink in sinks:
            sink_results = [r for r in results if r.get("sink") == sink and r.get("status") != "error"]
            if all(r.get("total_duplicates", 0) == 0 for r in sink_results):
                cleanest_sinks.append(sink)

        # Find problematic intervals
        problematic_intervals = []
        for interval in intervals:
            interval_results = [r for r in results if r.get("interval") == interval and r.get("status") != "error"]
            if any(r.get("total_duplicates", 0) > 0 for r in interval_results):
                problematic_intervals.append(interval)

        # Print summary
        if verbose:
            print(f"\n{'='*80}")
            print(f"SUMMARY")
            print(f"{'='*80}")
            print(f"Total combinations checked: {len(results)}")
            print(f"Combinations with duplicates: {combinations_with_duplicates}")
            print(f"Total rows across all: {total_rows_all:,}")
            print(f"Total duplicates across all: {total_duplicates_all:,}")
            print(f"Overall duplicate rate: {overall_duplicate_rate:.2f}%")

            if worst_combination and worst_combination.get("duplicate_rate", 0) > 0:
                print(f"\n⚠️  Worst combination: {worst_combination['interval']} in {worst_combination['sink']} "
                      f"({worst_combination['duplicate_rate']:.2f}% duplicates)")
                if worst_combination.get("dates_with_duplicates"):
                    dates_str = ", ".join(worst_combination["dates_with_duplicates"])
                    print(f"    Affected dates: {dates_str}")

            if cleanest_sinks:
                print(f"\n✅ Clean sinks (no duplicates): {', '.join(cleanest_sinks)}")

            if problematic_intervals:
                print(f"\n⚠️  Intervals with duplicates: {', '.join(problematic_intervals)}")

            # List all combinations with duplicates and their dates
            problematic_combinations = [r for r in results if r.get("total_duplicates", 0) > 0]
            if problematic_combinations:
                print(f"\n{'='*80}")
                print(f"DETAILED BREAKDOWN - COMBINATIONS WITH DUPLICATES")
                print(f"{'='*80}")
                for combo in problematic_combinations:
                    print(f"\n  {combo['interval']} in {combo['sink'].upper()}: "
                          f"{combo['total_duplicates']:,} duplicates ({combo['duplicate_rate']:.2f}%)")
                    if combo.get("dates_with_duplicates"):
                        dates_str = ", ".join(combo["dates_with_duplicates"])
                        print(f"    Dates: {dates_str}")

            print(f"\n{'='*80}\n")

        return {
            "symbol": symbol,
            "asset": asset,
            "date_range": {
                "start": start_date,
                "end": end_date
            },
            "intervals_checked": intervals,
            "sinks_checked": sinks,
            "total_combinations": len(results),
            "combinations_with_duplicates": combinations_with_duplicates,
            "results": results,
            "summary": {
                "total_rows_all": total_rows_all,
                "total_duplicates_all": total_duplicates_all,
                "overall_duplicate_rate": round(overall_duplicate_rate, 2),
                "worst_combination": worst_combination,
                "cleanest_sinks": cleanest_sinks,
                "problematic_intervals": problematic_intervals
            }
        }

    def duplication_and_strike_checks(
        self,
        asset: str,
        symbol: str,
        interval: str,
        sink: str = "csv",
        start_date: str = None,
        end_date: str = None,
        show: bool = False,
    ):
        """Perform detailed per-day audit: duplicate detection and strike×expiration accounting.

        This method performs comprehensive daily audits of option data quality by analyzing:
        1. Duplicate records based on composite keys
        2. Unique (strike, expiration) pair counting per day
        3. Overlap matrices between file parts within each day

        The composite key is: timestamp + symbol + expiration + strike (+ right if present).
        For each day, it builds an overlap matrix showing:
        - Diagonal: Intra-file duplicates (within the same part file)
        - Off-diagonal: Inter-file key intersections (between different part files)

        Parameters
        ----------
        asset : str
            The asset type: "option", "stock", or "index".
        symbol : str
            The ticker symbol or root symbol (e.g., "TLRY", "SPY").
        interval : str
            The time interval (e.g., "tick", "5m", "1h", "1d").
        sink : str, optional
            The storage backend: "csv" or "parquet" (default: "csv").
            Note: This method does not support InfluxDB.
        start_date : str, optional
            Start date in ISO format "YYYY-MM-DD". If None, starts from earliest available data.
        end_date : str, optional
            End date in ISO format "YYYY-MM-DD". If None, goes to latest available data.
        show : bool, optional
            If True, displays detailed results for each day (default: False).

        Returns
        -------
        tuple[dict, pd.DataFrame]
            A tuple containing:
            - results_by_day : dict
                Dictionary keyed by date (ISO string), with detailed daily analysis including
                overlap matrices, duplicate counts, and unique strike×expiration pairs.
            - summary_df : pd.DataFrame
                Summary DataFrame with one row per day containing aggregated statistics:
                date, total_records, unique_keys, intra_file_duplicates, inter_file_overlaps,
                unique_strikes, unique_expirations, etc.

        Notes
        -----
        - Timestamps are normalized to naive UTC to avoid aware/naive mismatches
        - Intra-file duplicate count ignores rows with NaN in any key column
        - The overlap matrix helps identify whether duplicates come from within files
          or across multiple part files for the same day

        Example Usage
        -------------
        # Audit TLRY tick data for November 2025
        results, summary = manager.duplication_and_strike_checks(
            asset="option",
            symbol="TLRY",
            interval="tick",
            sink="csv",
            start_date="2025-11-01",
            end_date="2025-11-30",
            show=True
        )

        # Check summary statistics
        print(summary[['date', 'total_records', 'intra_file_duplicates', 'unique_strikes']])
        """

    
        sink_lower = (sink or "csv").lower()
        assert sink_lower in ("csv", "parquet")
    
        # Preferred columns (we enforce 'timestamp' for options)
        tcol = "timestamp"
        key_base = ["symbol", "expiration", "strike"]
    
        def _key_cols(df: pd.DataFrame):
            """Return the effective key columns present in df."""
            cols = [tcol] + key_base + (["right"] if "right" in df.columns else [])
            return [c for c in cols if c in df.columns]
    
        def _read_minimal(path: str, usecols: list) -> pd.DataFrame:
            """
            Minimal, robust reader.
            - If self._read_minimal_frame exists, use it.
            - Otherwise, read only needed columns when possible.
            - Normalize timestamp to naive UTC (no tz info).
            """
            if hasattr(self, "_read_minimal_frame"):
                df = self._read_minimal_frame(path, usecols, sink_lower)
            else:
                if sink_lower == "csv":
                    head = pd.read_csv(path, nrows=0, dtype=str)
                    keep = [c for c in usecols if c in head.columns]
                    df = pd.read_csv(path, usecols=keep, dtype=str) if keep else pd.read_csv(path, dtype=str)
                else:
                    try:
                        df = pd.read_parquet(path, columns=usecols)
                    except Exception:
                        df = pd.read_parquet(path)
                        keep = [c for c in usecols if c in df.columns]
                        if keep:
                            df = df[keep]
    
            if tcol in df.columns:
                # normalize to naive UTC to avoid aware/naive comparison issues
                df[tcol] = pd.to_datetime(df[tcol], errors="coerce", utc=True).dt.tz_localize(None)
    
            return df
    
        # Optional display
        _display = ipy_display if show and ipy_display is not None else None
    
        results_by_day = {}
        summary_rows = []
    
        # Iterate over days using manager helpers
        for day in self._iter_days(start_date, end_date):

            # ### >>> STRONG SKIP — MIDDLE-DAY SHORT-CIRCUIT (INFLUX & FILE) — BEGIN
            # Se è attivo lo 'skip' forte, non fare NESSUN CHECK sui giorni *strictly in mezzo*.
            if getattr(self, "_fast_resume_skip_middle", False):
                # Recupera (una volta) il first/last day dal sink, se non li hai già.
                # Usa variabili locali esistenti se le hai (es. first_day_et, last_day_et).
                try:
                    fl = (first_day_et, last_day_et)  # se già calcolate prima del loop
                except NameError:
                    fl = self._get_first_last_day_from_sink(task.asset, symbol, interval, task.sink)
                if fl and fl[0] and fl[1]:
                    _first, _last = fl
                    if _first < day < _last:
                        self._dbg(f"[FAST-SKIP] middle-day short-circuit day={day} first={_first} last={_last}")
                        continue  # salta il giorno *senza* chiamare alcun check
            # ### >>> STRONG SKIP — MIDDLE-DAY SHORT-CIRCUIT (INFLUX & FILE) — END


            # --- ANTI-DUP DAY SCOPE (seen-by-key) ---
            seen_keys_day: set[tuple] = set()
            day_key_cols = ["timestamp","symbol","expiration","strike"]


            # Get file list for the day using internal API if available
            try:
                parts = self._list_day_files(asset, symbol, interval, sink_lower, day)
            except Exception:
                ext = "csv" if sink_lower == "csv" else "parquet"
                pattern = os.path.join(
                    self.cfg.root_dir, "data", asset, symbol, interval, self._sink_dir_name(sink_lower),
                    f"{day}T00-00-00Z-{symbol}-{asset}-{interval}_part*.{ext}"
                )
                parts = sorted(glob.glob(pattern))
    
            parts = [p for p in parts if os.path.exists(p)]
            if not parts:
                # No files for this day
                results_by_day[day] = {
                    "files": [],
                    "total_rows": 0,
                    "unique_key": 0,
                    "duplicates_key": 0,
                    "unique_strike_exp": 0,
                    "overlap_matrix_key": pd.DataFrame(),
                    "per_file_counts": {},
                    "top_overlap_timestamps": pd.Series(dtype="int64"),
                }
                summary_rows.append({"date": day, "files": 0, "rows": 0, "unique_key": 0, "dup_key": 0, "uniq_strike_exp": 0})
                continue
    
            # Read all parts for the day (only minimal columns)
            usecols = [tcol, "symbol", "expiration", "strike", "right"]
            per_file_df = []
            per_file_key_sets = {}
            per_file_counts = {}
            per_file_intra_dup = {}
    
            for p in parts:
                dfp = _read_minimal(p, usecols)
    
                # Normalize essential types
                if "expiration" in dfp.columns:
                    dfp["expiration"] = pd.to_datetime(dfp["expiration"], errors="coerce").dt.date
                if "strike" in dfp.columns:
                    dfp["strike"] = pd.to_numeric(dfp["strike"], errors="coerce")
    
                # Effective key columns for this file
                kcols = _key_cols(dfp)
                fname = os.path.basename(p)
    
                if tcol not in dfp.columns or not kcols:
                    # Incompatible file: track as empty
                    per_file_df.append(dfp.iloc[0:0])
                    per_file_key_sets[fname] = set()
                    per_file_counts[fname] = 0
                    per_file_intra_dup[fname] = 0
                    continue
    
                # Build key set for inter-file overlaps
                key_tuples = set(map(tuple, dfp[kcols].dropna().itertuples(index=False, name=None)))
                per_file_key_sets[fname] = key_tuples
                per_file_counts[fname] = len(key_tuples)
    
                # Intra-file duplicates on the key (diagonal)
                valid_mask = dfp[kcols].notna().all(axis=1)
                valid_rows = int(valid_mask.sum())
                unique_rows = int(dfp.loc[valid_mask, kcols].drop_duplicates().shape[0])
                per_file_intra_dup[fname] = max(valid_rows - unique_rows, 0)
    
                per_file_df.append(dfp)
    
            # Concat all files for the day
            day_df = pd.concat(per_file_df, ignore_index=True) if per_file_df else pd.DataFrame(columns=usecols)
    
            # Keep only rows with non-null timestamp
            if tcol in day_df.columns:
                day_df = day_df[day_df[tcol].notna()]
    
            # Day-level counts
            kcols_all = _key_cols(day_df)
            total_rows = int(len(day_df))
            unique_key = int(day_df[kcols_all].dropna().drop_duplicates().shape[0]) if kcols_all else 0
            duplicates_key = max(total_rows - unique_key, 0)
    
            # Unique (strike, expiration) pairs
            if "strike" in day_df.columns and "expiration" in day_df.columns:
                unique_strike_exp = int(day_df[["strike", "expiration"]].dropna().drop_duplicates().shape[0])
            else:
                unique_strike_exp = 0
    
            # Top timestamps where duplicates occurred (sum of excess per ts)
            top_overlap_ts = pd.Series(dtype="int64")
            if kcols_all:
                grp = day_df[kcols_all].dropna().groupby(tcol).size()
                uniq_per_ts = day_df[kcols_all].dropna().drop_duplicates().groupby(tcol).size()
                over_ts = (grp - uniq_per_ts).astype("int64")
                top_overlap_ts = over_ts[over_ts > 0].sort_values(ascending=False).head(10)
    
            # Build overlap matrix (diagonal = intra-file dup; off-diagonal = intersections)
            file_names = [os.path.basename(p) for p in parts]
            overlap_mat = pd.DataFrame(0, index=file_names, columns=file_names, dtype="int64")
    
            for i, f1 in enumerate(file_names):
                s1 = per_file_key_sets.get(f1, set())
                for j, f2 in enumerate(file_names):
                    if j < i:
                        overlap_mat.iat[i, j] = overlap_mat.iat[j, i]
                        continue
                    if i == j:
                        overlap_mat.iat[i, j] = int(per_file_intra_dup.get(f1, 0))
                        continue
                    s2 = per_file_key_sets.get(f2, set())
                    cnt = len(s1 & s2) if (s1 and s2) else 0

                    # DEBUG: se troviamo una singola intersezione, stampa la chiave e verifica il ts
                    if cnt and cnt <= 3:
                        inter = list(s1 & s2)
                        try:
                            print(f"[DEBUG-OVERLAP] {f1} ∩ {f2} = {cnt}  sample={inter[:1]}")
                        except Exception:
                            pass

                    
                    overlap_mat.iat[i, j] = cnt
    
            # Store results for this day
            results_by_day[day] = {
                "files": parts,
                "total_rows": total_rows,
                "unique_key": unique_key,
                "duplicates_key": duplicates_key,
                "unique_strike_exp": unique_strike_exp,
                "overlap_matrix_key": overlap_mat,
                "per_file_counts": per_file_counts,
                "top_overlap_timestamps": top_overlap_ts,
            }
    
            summary_rows.append({
                "date": day,
                "files": len(parts),
                "rows": total_rows,
                "unique_key": unique_key,
                "dup_key": duplicates_key,
                "uniq_strike_exp": unique_strike_exp,
            })
    
            # Optional display
            if show:
                print(f"\n[{day}] righe totali: {total_rows:,}  uniche(key): {unique_key:,}  overlap(dup): {duplicates_key:,}")
                print(f"strike×expiration uniche: {unique_strike_exp:,}")
                if not top_overlap_ts.empty:
                    print("Top timestamp con overlap:")
                    print(top_overlap_ts)
                if _display is not None:
                    try:
                        print("Overlap/Dup matrix (diag = intra-file dup; off-diag = inter-file overlap):")
                        _display(overlap_mat)
                    except Exception:
                        print(overlap_mat)
    
        summary_df = pd.DataFrame(summary_rows).sort_values("date").reset_index(drop=True)
    
        if show:
            print("\n=== RIEPILOGO ===")
            try:
                if _display is not None:
                    _display(summary_df)
                else:
                    print(summary_df)
            except Exception:
                print(summary_df)
    
        return results_by_day, summary_df



    def check_duplicates_in_sink(
        self,
        asset: str,
        symbol: str,
        interval: str,
        sink: str,
        day_iso: Optional[str] = None,
        sample_limit: int = 10
    ) -> dict:
        """Check for duplicate records in a specific sink for a single day or entire dataset.

        This method verifies the presence of duplicate records in the specified storage sink
        by checking composite key uniqueness. The key columns vary by asset type and sink.
        Returns detailed statistics about duplication rates and sample duplicate keys.

        Parameters
        ----------
        asset : str
            The asset type: "option", "stock", or "index".
            Determines which columns are used as the composite key.
        symbol : str
            The ticker symbol or root symbol (e.g., "TLRY", "AAPL", "SPY").
        interval : str
            The timeframe (e.g., "tick", "1m", "5m", "1h", "1d").
        sink : str
            The storage backend: "influxdb", "csv", or "parquet".
            Different sinks may use different duplicate detection strategies.
        day_iso : str, optional
            Specific trading day in ISO format "YYYY-MM-DD" (e.g., "2025-11-07").
            If None, checks the entire dataset across all available days.
        sample_limit : int, optional
            Maximum number of duplicate key examples to return (default: 10).
            Limits the size of the duplicate_keys list in the result.

        Returns
        -------
        dict
            Dictionary with the following keys:
            - total_rows : int
                Total number of rows in the dataset
            - unique_rows : int
                Number of unique rows (distinct composite keys)
            - duplicates : int
                Number of duplicate rows (total_rows - unique_rows)
            - duplicate_rate : float
                Percentage of duplicate rows (0-100)
            - duplicate_keys : list
                List of the first N duplicate key examples (limited by sample_limit)
            - key_columns : list
                List of column names used as the composite key for duplicate detection

        Example Usage
        -------------
        # Check for duplicates in a specific day
        result = manager.check_duplicates_in_sink(
            asset="option",
            symbol="TLRY",
            interval="tick",
            sink="csv",
            day_iso="2025-11-07",
            sample_limit=5
        )
        print(f"Found {result['duplicates']} duplicates ({result['duplicate_rate']:.2f}%)")

        # Check entire dataset for duplicates
        result = manager.check_duplicates_in_sink(
            asset="stock",
            symbol="AAPL",
            interval="1d",
            sink="parquet",
            day_iso=None
        )
        """
        sink_lower = sink.lower()
        
        if sink_lower == "influxdb":
            return self._check_duplicates_influx(asset, symbol, interval, day_iso, sample_limit)
        elif sink_lower in ("csv", "parquet"):
            return self._check_duplicates_file(asset, symbol, interval, sink_lower, day_iso, sample_limit)
        else:
            return {
                "error": f"Sink non supportato: {sink}",
                "total_rows": 0,
                "unique_rows": 0,
                "duplicates": 0,
                "duplicate_rate": 0.0,
                "duplicate_keys": [],
                "key_columns": []
            }
    
    def check_duplicates_multi_day(
        self,
        asset: str,
        symbol: str,
        interval: str,
        sink: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sample_limit: int = 10,
        verbose: bool = True
    ) -> dict:
        """Check for duplicate records across multiple trading days in the specified data sink.

        This method performs a comprehensive duplicate detection analysis across a date range of trading
        days (Monday-Friday), automatically skipping weekends. It iterates through each trading day in
        the specified range, calls check_duplicates_in_sink() for each day, and aggregates the results
        into a comprehensive multi-day report. The method can auto-detect the available date range from
        the data sink if start_date and end_date are not provided. This is essential for data quality
        assurance, identifying data ingestion issues, and maintaining clean historical datasets across
        CSV, Parquet, and InfluxDB storage backends.

        Parameters
        ----------
        asset : str
            The asset type to check. Possible values: "option", "stock", "index".
            Determines which data structure and key columns to use for duplicate detection.
        symbol : str
            The ticker symbol or root symbol to analyze (e.g., "TLRY", "AAPL", "SPY", "ES").
            Must match an existing symbol in the specified sink.
        interval : str
            The bar interval or timeframe (e.g., "tick", "1m", "5m", "10m", "1h", "1d").
            Determines which data series to check for duplicates.
        sink : str
            The storage backend to check. Possible values: "influxdb", "csv", "parquet".
            Different sinks use different duplicate detection strategies:
            - "influxdb": Queries InfluxDB for duplicate timestamps
            - "csv": Reads CSV files and checks for duplicate composite keys
            - "parquet": Reads Parquet files and checks for duplicate composite keys
        start_date : str, optional
            Default: None (auto-detect from data)
            The start date of the range to check in ISO format "YYYY-MM-DD" (e.g., "2025-11-03").
            When None, automatically detects the earliest date available in the sink. Only trading
            days (Mon-Fri) within this range are analyzed.
        end_date : str, optional
            Default: None (auto-detect from data)
            The end date of the range to check in ISO format "YYYY-MM-DD" (e.g., "2025-11-07").
            When None, automatically detects the latest date available in the sink. Only trading
            days (Mon-Fri) within this range are analyzed.
        sample_limit : int, optional
            Default: 10
            Maximum number of duplicate key examples to collect and display per day. Limits the
            size of the example lists in the daily results to prevent memory issues with large
            duplicate sets. Set to 0 to disable example collection.
        verbose : bool, optional
            Default: True
            If True, prints detailed daily progress reports to console including per-day statistics,
            duplicate rates, and overall summary. If False, runs silently and only returns the
            results dictionary. Useful for scripting and automated quality checks.

        Returns
        -------
        dict
            A comprehensive results dictionary containing:
            - 'days_analyzed' (int): Number of trading days checked (excludes weekends)
            - 'total_rows' (int): Total number of data rows across all checked days
            - 'total_duplicates' (int): Total number of duplicate records found across all days
            - 'global_duplicate_rate' (float): Overall percentage of duplicates (0.0-100.0)
            - 'days_with_duplicates' (int): Number of days that contain at least one duplicate
            - 'daily_results' (list): List of per-day result dictionaries, each containing:
                - 'date' (str): ISO date "YYYY-MM-DD"
                - 'rows' (int): Number of rows for that day
                - 'duplicates' (int): Number of duplicates found
                - 'duplicate_rate' (float): Percentage of duplicates for that day
                - 'duplicate_keys' (list): Sample duplicate keys (limited by sample_limit)
            - 'summary' (dict): Summary information with:
                - 'start_date' (str): Actual start date checked
                - 'end_date' (str): Actual end date checked
                - 'status' (str): "CLEAN" if no duplicates, "DUPLICATES_FOUND" otherwise
            - 'error' (str, optional): Error message if date detection or parsing failed

        Example Usage
        -------------
        # Example 1: Auto-detect date range (checks all available data)
        manager = ThetaSyncManager(cfg, client=client)
        result = manager.check_duplicates_multi_day(
            asset="option",
            symbol="TLRY",
            interval="tick",
            sink="influxdb",
            verbose=True  # Print detailed progress
        )
        print(f"Days analyzed: {result['days_analyzed']}")
        print(f"Total duplicates: {result['total_duplicates']:,}")
        print(f"Duplicate rate: {result['global_duplicate_rate']}%")

        # Example 2: Specify explicit date range
        result = manager.check_duplicates_multi_day(
            asset="option",
            symbol="TLRY",
            interval="5m",
            sink="influxdb",
            start_date="2025-11-03",
            end_date="2025-11-07",
            sample_limit=5,  # Show up to 5 duplicate examples per day
            verbose=True
        )

        # Example 3: Silent mode (no console output)
        result = manager.check_duplicates_multi_day(
            asset="stock",
            symbol="AAPL",
            interval="1m",
            sink="parquet",
            start_date="2025-11-01",
            end_date="2025-11-30",
            verbose=False  # No console output
        )
        if result['days_with_duplicates'] > 0:
            print(f"Found duplicates in {result['days_with_duplicates']} days")

        # Example 4: Process results programmatically
        result = manager.check_duplicates_multi_day(
            asset="option",
            symbol="TLRY",
            interval="5m",
            sink="csv",
            verbose=False
        )
        if result['days_with_duplicates'] > 0:
            for day_result in result['daily_results']:
                if day_result['duplicates'] > 0:
                    print(f"{day_result['date']}: {day_result['duplicates']} duplicates")

        # Example 5: Compare different sinks
        for sink_type in ["csv", "parquet", "influxdb"]:
            result = manager.check_duplicates_multi_day(
                asset="option",
                symbol="TLRY",
                interval="5m",
                sink=sink_type,
                start_date="2025-11-03",
                end_date="2025-11-07",
                verbose=False
            )
            print(f"{sink_type}: {result['total_duplicates']:,} duplicates")
        """
        
        # START: Auto-detect date range if not provided
        if not start_date or not end_date:
            first_day, last_day = self._get_first_last_day_from_sink(
                asset, symbol, interval, sink.lower()
            )
            
            if not first_day or not last_day:
                print("[MULTI-DAY-CHECK][ERROR] No data found in sink. Cannot determine date range.")
                return {
                    "error": "No data found in sink",
                    "days_analyzed": 0,
                    "total_rows": 0,
                    "total_duplicates": 0,
                    "global_duplicate_rate": 0.0,
                    "days_with_duplicates": 0,
                    "daily_results": [],
                    "summary": {}
                }
            
            start_date = start_date or first_day
            end_date = end_date or last_day
        # END: Auto-detect date range
        
        # Parse dates
        try:
            start_dt = dt.fromisoformat(start_date)
            end_dt = dt.fromisoformat(end_date)
        except ValueError as e:
            return {
                "error": f"Invalid date format: {e}",
                "days_analyzed": 0,
                "total_rows": 0,
                "total_duplicates": 0,
                "global_duplicate_rate": 0.0,
                "days_with_duplicates": 0,
                "daily_results": [],
                "summary": {}
            }
        
        if verbose:
            print(f"\n{'='*70}")
            print(f"DUPLICATE CHECK - {symbol} {asset} {interval} [{sink}]")
            print(f"Date range: {start_date} -> {end_date}")
            print(f"{'='*70}\n")
        
        # START: Iterate through all days and check duplicates
        total_days = 0
        total_rows_all = 0
        total_duplicates_all = 0
        daily_results = []
        
        current_dt = start_dt
        while current_dt <= end_dt:
            day_iso = current_dt.date().isoformat()
            
            # Skip weekends
            if current_dt.weekday() >= 5:
                current_dt += timedelta(days=1)
                continue
            
            # Check duplicates for this day
            result = self.check_duplicates_in_sink(
                asset=asset,
                symbol=symbol,
                interval=interval,
                sink=sink,
                day_iso=day_iso,
                sample_limit=sample_limit
            )
            
            # Only process days with data
            if result.get('total_rows', 0) > 0:
                total_days += 1
                total_rows_all += result['total_rows']
                total_duplicates_all += result['duplicates']
                
                # Store daily result
                daily_result = {
                    'date': day_iso,
                    'total_rows': result['total_rows'],
                    'duplicates': result['duplicates'],
                    'duplicate_rate': result['duplicate_rate'],
                    'duplicate_keys': result['duplicate_keys'][:3]  # first 3 examples
                }
                daily_results.append(daily_result)
                
                # Print daily progress if verbose
                if verbose:
                    status = "✅" if result['duplicates'] == 0 else "❌"
                    print(f"{status} {day_iso}: {result['total_rows']:6d} rows, "
                          f"{result['duplicates']:4d} duplicates ({result['duplicate_rate']:5.2f}%)")
            
            current_dt += timedelta(days=1)
        # END: Iterate through days
        
        # START: Calculate global statistics
        global_dup_rate = (
            100.0 * total_duplicates_all / total_rows_all 
            if total_rows_all > 0 else 0.0
        )
        days_with_dups = sum(1 for r in daily_results if r['duplicates'] > 0)
        # END: Calculate global statistics
        
        # START: Print summary report if verbose
        if verbose:
            print(f"\n{'='*70}")
            print(f"SUMMARY")
            print(f"{'='*70}")
            print(f"Days analyzed:          {total_days}")
            print(f"Total rows:             {total_rows_all:,}")
            print(f"Total duplicates:       {total_duplicates_all:,}")
            print(f"Global duplicate rate:  {global_dup_rate:.2f}%")
            print(f"Days with duplicates:   {days_with_dups}")
            
            # Show detailed breakdown for days with duplicates
            if days_with_dups > 0:
                print(f"\n{'='*70}")
                print(f"DAYS WITH DUPLICATES - DETAILS")
                print(f"{'='*70}")
                for day_info in daily_results:
                    if day_info['duplicates'] > 0:
                        print(f"\n📅 {day_info['date']}: {day_info['duplicates']} duplicates "
                              f"out of {day_info['total_rows']} rows ({day_info['duplicate_rate']:.2f}%)")
                        if day_info['duplicate_keys']:
                            print(f"   Example duplicate keys:")
                            for i, key in enumerate(day_info['duplicate_keys'], 1):
                                print(f"     {i}. {key}")
            else:
                print(f"\n✅ No duplicates found in any day!")
            
            print(f"\n{'='*70}\n")
        # END: Print summary report
        
        return {
            "days_analyzed": total_days,
            "total_rows": total_rows_all,
            "total_duplicates": total_duplicates_all,
            "global_duplicate_rate": round(global_dup_rate, 2),
            "days_with_duplicates": days_with_dups,
            "daily_results": daily_results,
            "summary": {
                "symbol": symbol,
                "asset": asset,
                "interval": interval,
                "sink": sink,
                "start_date": start_date,
                "end_date": end_date,
                "status": "clean" if total_duplicates_all == 0 else "duplicates_found"
            }
        }
    # --------------------------------------------------------------------------
    # END: Multi-day duplicate check functionality
    # --------------------------------------------------------------------------

    def _check_duplicates_influx(
        self,
        asset: str,
        symbol: str,
        interval: str,
        day_iso: Optional[str],
        sample_limit: int
    ) -> dict:
        """Check duplicati in InfluxDB."""
        
        # Costruisci measurement name
        prefix = self.cfg.influx_measure_prefix or ""
        if asset == "option":
            meas = f"{prefix}{symbol}-option-{interval}"
        else:
            meas = f"{prefix}{symbol}-{asset}-{interval}"
        
        # Determina range temporale
        if day_iso:
            tz_et = ZoneInfo("America/New_York")
            day_start_utc = pd.Timestamp(f"{day_iso}T00:00:00", tz=tz_et).tz_convert("UTC")
            day_end_utc = (pd.Timestamp(f"{day_iso}T00:00:00", tz=tz_et) + pd.Timedelta(days=1)).tz_convert("UTC")
            time_filter = f'|> range(start: {day_start_utc.isoformat()}, stop: {day_end_utc.isoformat()})'
        else:
            time_filter = '|> range(start: 0)'  # tutto
        
        # Definisci key columns per asset
        if asset == "option":
            if interval == "tick":
                key_tags = ["symbol", "expiration", "right", "strike", "sequence"]
            else:
                key_tags = ["symbol", "expiration", "right", "strike"]
        else:
            key_tags = ["symbol"]
        
        try:
            # START: InfluxDB v3 usa SQL, non Flux
            # Costruisci filtro WHERE per il time range
            where_time = ""
            if day_iso:
                tz_et = ZoneInfo("America/New_York")
                day_start_utc = pd.Timestamp(f"{day_iso}T00:00:00", tz=tz_et).tz_convert("UTC")
                day_end_utc = (pd.Timestamp(f"{day_iso}T00:00:00", tz=tz_et) + pd.Timedelta(days=1)).tz_convert("UTC")
                where_time = f"AND time >= TIMESTAMP '{day_start_utc.isoformat()}' AND time < TIMESTAMP '{day_end_utc.isoformat()}'"
            
            # Query SQL per InfluxDB v3: leggi tutti i dati
            query_data = f"""
                SELECT *
                FROM "{meas}"
                WHERE 1=1
                {where_time}
            """
            
            result = self.influx_client.query(query=query_data)

    
            # Converti in DataFrame (InfluxDB v3 restituisce già un DataFrame)
            df = result.to_pandas() if hasattr(result, 'to_pandas') else result
            
            if df is None or len(df) == 0:
                return {
                    "total_rows": 0,
                    "unique_rows": 0,
                    "duplicates": 0,
                    "duplicate_rate": 0.0,
                    "duplicate_keys": [],
                    "key_columns": ["time"] + key_tags
                }
            
            total_rows = len(df)
            
            # Normalizza timestamp (InfluxDB v3 usa "time", non "_time")
            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"], utc=True)
                time_col = "time"
            elif "_time" in df.columns:
                df["_time"] = pd.to_datetime(df["_time"], utc=True)
                time_col = "_time"
            else:
                time_col = "time"
            
            # Normalizza key columns
            key_cols = [time_col]
            for tag in key_tags:
                if tag in df.columns:
                    if tag == "expiration":
                        df[tag] = pd.to_datetime(df[tag], errors="coerce").dt.date
                    elif tag == "strike":
                        df[tag] = pd.to_numeric(df[tag], errors="coerce")
                    elif tag == "right":
                        df[tag] = df[tag].astype(str).str.strip().str.lower()
                    key_cols.append(tag)

            # >>> STOCK TICK – include 'sequence' nella chiave se presente
            if asset == "stock" and interval == "tick" and "sequence" in df.columns and "sequence" not in key_cols:
                key_cols.append("sequence")
            
            # Conta duplicati
            total = len(df)
            df_dedup = df.drop_duplicates(subset=key_cols, keep="first")
            unique = len(df_dedup)
            duplicates = total - unique
            
            # Trova esempi di chiavi duplicate
            duplicate_keys = []
            if duplicates > 0:
                dup_mask = df.duplicated(subset=key_cols, keep=False)
                dup_df = df[dup_mask].sort_values(key_cols)
                seen = set()
                for _, row in dup_df.iterrows():
                    key = tuple(row[c] for c in key_cols)
                    if key not in seen:
                        seen.add(key)
                        duplicate_keys.append(key)
                        if len(duplicate_keys) >= sample_limit:
                            break
            
            return {
                "total_rows": total,
                "unique_rows": unique,
                "duplicates": duplicates,
                "duplicate_rate": round(100.0 * duplicates / total, 2) if total > 0 else 0.0,
                "duplicate_keys": duplicate_keys,
                "key_columns": key_cols
            }
            
        except Exception as e:
            return {
                "error": f"Query InfluxDB fallita: {e}",
                "total_rows": 0,
                "unique_rows": 0,
                "duplicates": 0,
                "duplicate_rate": 0.0,
                "duplicate_keys": [],
                "key_columns": ["_time"] + key_tags
            }
    
    def _check_duplicates_file(
        self,
        asset: str,
        symbol: str,
        interval: str,
        sink_lower: str,
        day_iso: Optional[str],
        sample_limit: int
    ) -> dict:
        """Check duplicati in CSV/Parquet."""
        
        # Trova file
        if day_iso:
            files = self._list_day_files(asset, symbol, interval, sink_lower, day_iso)
        else:
            # Tutti i file per questo symbol/interval
            base_dir = os.path.join(self.cfg.root_dir, "data", asset, symbol, interval)
            if not os.path.exists(base_dir):
                return {
                    "total_rows": 0,
                    "unique_rows": 0,
                    "duplicates": 0,
                    "duplicate_rate": 0.0,
                    "duplicate_keys": [],
                    "key_columns": []
                }
            
            ext = ".csv" if sink_lower == "csv" else ".parquet"
            files = [
                os.path.join(base_dir, f)
                for f in os.listdir(base_dir)
                if f.endswith(ext)
            ]
        
        if not files:
            return {
                "total_rows": 0,
                "unique_rows": 0,
                "duplicates": 0,
                "duplicate_rate": 0.0,
                "duplicate_keys": [],
                "key_columns": []
            }
        
        # Leggi tutti i file
        dfs = []
        for f in files:
            try:
                if sink_lower == "csv":
                    df = pd.read_csv(f, dtype=str)
                else:
                    df = pd.read_parquet(f)
                dfs.append(df)
            except Exception as e:
                print(f"[CHECK-DUP][WARN] errore lettura {f}: {e}")
                continue
        
        if not dfs:
            return {
                "total_rows": 0,
                "unique_rows": 0,
                "duplicates": 0,
                "duplicate_rate": 0.0,
                "duplicate_keys": [],
                "key_columns": []
            }
        
        df_all = pd.concat(dfs, ignore_index=True)
        
        # Definisci key columns
        if asset == "option":
            tcol = "timestamp" if "timestamp" in df_all.columns else "created"
            key_cols = [tcol, "symbol", "expiration", "strike"]
            
            if "right" in df_all.columns:
                key_cols.append("right")
            if interval == "tick" and "sequence" in df_all.columns:
                key_cols.append("sequence")
            
            # Normalizza tipi
            if "expiration" in df_all.columns:
                df_all["expiration"] = pd.to_datetime(df_all["expiration"], errors="coerce").dt.date
            if "strike" in df_all.columns:
                df_all["strike"] = pd.to_numeric(df_all["strike"], errors="coerce")
            if "right" in df_all.columns:
                df_all["right"] = df_all["right"].astype(str).str.strip().str.lower()
        else:
            tcol = "timestamp" if "timestamp" in df_all.columns else "created"
            key_cols = [tcol]
            if asset == "stock" and interval == "tick" and "sequence" in df_all.columns:
                key_cols.append("sequence")

        
        # Normalizza timestamp
        if tcol in df_all.columns:
            df_all[tcol] = pd.to_datetime(df_all[tcol], errors="coerce")
        
        # Conta duplicati
        total = len(df_all)
        df_dedup = df_all.drop_duplicates(subset=key_cols, keep="first")
        unique = len(df_dedup)
        duplicates = total - unique
        
        # Trova esempi
        duplicate_keys = []
        if duplicates > 0:
            dup_mask = df_all.duplicated(subset=key_cols, keep=False)
            dup_df = df_all[dup_mask].sort_values(key_cols)
            seen = set()
            for _, row in dup_df.iterrows():
                key = tuple(row[c] for c in key_cols)
                if key not in seen:
                    seen.add(key)
                    duplicate_keys.append(key)
                    if len(duplicate_keys) >= sample_limit:
                        break
        
        return {
            "total_rows": total,
            "unique_rows": unique,
            "duplicates": duplicates,
            "duplicate_rate": round(100.0 * duplicates / total, 2) if total > 0 else 0.0,
            "duplicate_keys": duplicate_keys,
            "key_columns": key_cols
        }

    # --------------------------------------------------------------------------
    # START: Multi-day duplicate check functionality
    # --------------------------------------------------------------------------


    # =========================================================================
    # (END)
    # DATA QUALITY VALIDATION
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # LOCAL DB QUERY
    # =========================================================================
    def list_available_data(
        self,
        asset: Optional[str] = None,
        symbol: Optional[str] = None,
        interval: Optional[str] = None,
        sink: Optional[str] = None
    ) -> pd.DataFrame:
        """List all available data series in local sinks with date range information.

        This method scans the local sink directories (CSV, Parquet, InfluxDB) and returns a summary
        of all available time series with their earliest and latest timestamps. Useful for discovering
        what data is available before querying.

        Parameters
        ----------
        asset : str, optional
            Filter by asset type ("option", "stock", "index"). If None, returns all assets.
        symbol : str, optional
            Filter by symbol. If None, returns all symbols.
        interval : str, optional
            Filter by interval (e.g., "1m", "5m", "1d"). If None, returns all intervals.
        sink : str, optional
            Filter by sink type ("csv", "parquet", "influxdb"). If None, scans all sinks.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns:
            - asset: Asset type
            - symbol: Ticker symbol
            - interval: Time interval
            - sink: Storage format
            - first_datetime: Earliest available timestamp (UTC, ISO format)
            - last_datetime: Latest available timestamp (UTC, ISO format)
            - file_count: Number of files (for CSV/Parquet) or 0 for InfluxDB
            - total_size_mb: Total size in MB (for CSV/Parquet) or 0 for InfluxDB

            Results are sorted by:
            1. sink (csv, influxdb, parquet - alphabetical)
            2. asset (option, stock, index - logical order)
            3. symbol (alphabetical)
            4. interval (tick, 1s, 5s, ... 1m, 5m, ... 1h, ... 1d - smallest to largest)

        Example Usage
        -------------
        # List all available data
        available = manager.list_available_data()

        # List only AAPL stock data in CSV format
        aapl_csv = manager.list_available_data(asset="stock", symbol="AAPL", sink="csv")

        # List all 5-minute interval data
        five_min = manager.list_available_data(interval="5m")
        """
        results = []

        # Determine which sinks to scan
        sinks_to_scan = []
        if sink is None:
            sinks_to_scan = ["csv", "parquet", "influxdb"]
        else:
            sinks_to_scan = [sink.lower()]

        # Scan CSV and Parquet sinks
        for sink_type in ["csv", "parquet"]:
            if sink_type not in sinks_to_scan:
                continue

            data_root = os.path.join(self.cfg.root_dir, "data")
            if not os.path.isdir(data_root):
                continue

            # Scan asset directories
            for asset_dir in os.listdir(data_root):
                if asset is not None and asset_dir.lower() != asset.lower():
                    continue

                asset_path = os.path.join(data_root, asset_dir)
                if not os.path.isdir(asset_path):
                    continue

                # Scan symbol directories
                for symbol_dir in os.listdir(asset_path):
                    if symbol is not None and symbol_dir.upper() != symbol.upper():
                        continue

                    symbol_path = os.path.join(asset_path, symbol_dir)
                    if not os.path.isdir(symbol_path):
                        continue

                    # Scan interval directories
                    for interval_dir in os.listdir(symbol_path):
                        if interval is not None and interval_dir.lower() != interval.lower():
                            continue

                        interval_path = os.path.join(symbol_path, interval_dir)
                        if not os.path.isdir(interval_path):
                            continue

                        # Check if sink directory exists
                        sink_path = os.path.join(interval_path, sink_type)
                        if not os.path.isdir(sink_path):
                            continue

                        # Get list of files
                        files = self._list_series_files(asset_dir, symbol_dir, interval_dir, sink_type)
                        if not files:
                            continue

                        # Get earliest and latest dates
                        earliest, latest = self._get_first_last_day_from_sink(
                            asset_dir, symbol_dir, interval_dir, sink_type
                        )

                        # Get first and last timestamps from actual data
                        first_ts = None
                        last_ts = None
                        total_size = 0

                        try:
                            # First timestamp from first file
                            if sink_type == "csv":
                                first_ts = self._first_timestamp_in_csv(files[0])
                                last_ts = self._last_csv_timestamp(files[-1])
                            else:  # parquet
                                first_ts = self._first_timestamp_in_parquet(files[0])
                                # For last timestamp, read last file
                                df_last = pd.read_parquet(files[-1])
                                time_col = self._detect_time_col(df_last.columns)
                                if time_col and not df_last.empty:
                                    last_ts = pd.to_datetime(df_last[time_col].max(), utc=True).isoformat()

                            # Calculate total size
                            for f in files:
                                if os.path.isfile(f):
                                    total_size += os.path.getsize(f)
                        except Exception as e:
                            print(f"[WARNING] Error reading timestamps for {asset_dir}/{symbol_dir}/{interval_dir}: {e}")

                        results.append({
                            "asset": asset_dir,
                            "symbol": symbol_dir,
                            "interval": interval_dir,
                            "sink": sink_type,
                            "first_datetime": first_ts,
                            "last_datetime": last_ts,
                            "file_count": len(files),
                            "total_size_mb": round(total_size / (1024 * 1024), 2) if total_size > 0 else 0
                        })

        # Scan InfluxDB sink (if configured and requested)
        if "influxdb" in sinks_to_scan:
            try:
                # Get list of InfluxDB tables using helper method
                table_names = self._list_influx_tables()

                if table_names:
                    for table_name in table_names:
                        # Parse measurement name: {prefix}{SYMBOL}-{asset}-{interval}
                        # Example: "TLRY-option-tick" or with prefix: "td_TLRY-option-tick"
                        name = str(table_name)

                        # Remove prefix if present
                        if self.cfg.influx_measure_prefix:
                            name = name.replace(self.cfg.influx_measure_prefix, "", 1)

                        # Parse: SYMBOL-asset-interval
                        parts = name.split("-")
                        if len(parts) >= 3:
                            symbol_part = parts[0]
                            asset_part = parts[1]
                            interval_part = "-".join(parts[2:])  # Handle intervals like "1d"

                            # Filter by criteria if specified
                            if asset and asset_part.lower() != asset.lower():
                                continue
                            if symbol and symbol_part.upper() != symbol.upper():
                                continue
                            if interval and interval_part.lower() != interval.lower():
                                continue

                            # Get min/max timestamps
                            ts_query = f'SELECT MIN(time) as first_ts, MAX(time) as last_ts FROM "{table_name}"'
                            try:
                                ts_df = self._influx_query_dataframe(ts_query)

                                first_ts = None
                                last_ts = None

                                if not ts_df.empty:
                                    first_val = self._extract_scalar_from_df(ts_df, ["first_ts", "min"])
                                    last_val = self._extract_scalar_from_df(ts_df, ["last_ts", "max"])
                                    co_first = self._coerce_timestamp(first_val)
                                    co_last = self._coerce_timestamp(last_val)
                                    if co_first is not None:
                                        first_ts = co_first.isoformat()
                                    if co_last is not None:
                                        last_ts = co_last.isoformat()

                                entry = {
                                    "asset": asset_part,
                                    "symbol": symbol_part,
                                    "interval": interval_part,
                                    "sink": "influxdb",
                                    "first_datetime": first_ts,
                                    "last_datetime": last_ts,
                                    "file_count": 0,
                                    "total_size_mb": 0
                                }
                                results.append(entry)
                            except Exception as e:
                                print(f"[WARNING] Error getting timestamps for {table_name}: {e}")

            except Exception as e:
                print(f"[WARNING] Error scanning InfluxDB: {e}")

        df = pd.DataFrame(results)
        if df.empty:
            return pd.DataFrame(columns=[
                "asset", "symbol", "interval", "sink", "first_datetime",
                "last_datetime", "file_count", "total_size_mb"
            ])

        # Custom sort order for intervals (tick -> 1s -> 5s -> ... -> 1d)
        interval_order = {
            "tick": 0,
            "1s": 1, "5s": 2, "10s": 3, "15s": 4, "30s": 5,
            "1m": 10, "5m": 11, "10m": 12, "15m": 13, "30m": 14,
            "1h": 20, "2h": 21, "4h": 22,
            "1d": 30, "1w": 40, "1mo": 50
        }

        # Custom sort order for assets
        asset_order = {"option": 0, "stock": 1, "index": 2}

        # Add sorting columns
        df["_interval_sort"] = df["interval"].map(lambda x: interval_order.get(x.lower(), 999))
        df["_asset_sort"] = df["asset"].map(lambda x: asset_order.get(x.lower(), 999))

        # Sort: sink → asset → symbol → interval
        df = df.sort_values(
            ["sink", "_asset_sort", "symbol", "_interval_sort"],
            ascending=[True, True, True, True]
        ).reset_index(drop=True)

        # Remove sorting columns
        df = df.drop(columns=["_interval_sort", "_asset_sort"])

        return df


    def query_local_data(
        self,
        asset: str,
        symbol: str,
        interval: str,
        sink: str,
        start_date: Optional[str] = None,
        start_datetime: Optional[str] = None,
        end_date: Optional[str] = None,
        end_datetime: Optional[str] = None,
        max_rows: Optional[int] = None,
        get_first_n_rows: Optional[int] = None,
        get_last_n_rows: Optional[int] = None,
        get_first_n_days: Optional[int] = None,
        get_last_n_days: Optional[int] = None,
        get_first_n_minutes: Optional[int] = None,
        get_last_n_minutes: Optional[int] = None,
        _allow_full_scan: bool = False
    ) -> Tuple[Optional[pd.DataFrame], List[str]]:
        """Query and extract data from local sinks within a date/time range.

        This method reads data from local storage (CSV, Parquet, or InfluxDB) for a specified
        symbol and time range. It returns both the data and a list of warnings/errors encountered.

        Parameters
        ----------
        asset : str
            Asset type: "option", "stock", or "index" (required).
        symbol : str
            Ticker symbol to query (required).
        interval : str
            Time interval: "tick", "1m", "5m", "1h", "1d", etc. (required).
        sink : str
            Storage format: "csv", "parquet", or "influxdb" (required).
        start_date : str, optional
            Start date in ISO format "YYYY-MM-DD". If provided without start_datetime,
            assumes 00:00:00 UTC on this date.
        start_datetime : str, optional
            Start datetime in ISO format "YYYY-MM-DDTHH:MM:SS" or "YYYY-MM-DD HH:MM:SS".
            If provided, takes precedence over start_date.
        end_date : str, optional
            End date in ISO format "YYYY-MM-DD". If not provided, queries up to the latest data.
            If provided without end_datetime, assumes 23:59:59 UTC on this date.
        end_datetime : str, optional
            End datetime in ISO format "YYYY-MM-DDTHH:MM:SS" or "YYYY-MM-DD HH:MM:SS".
            If provided, takes precedence over end_date.
        max_rows : int, optional
            Maximum number of rows to return. If the result exceeds this limit, returns
            only the first max_rows and adds a warning.
        get_first_n_rows : int, optional
            Return only the first N rows from the filtered data.
        get_last_n_rows : int, optional
            Return only the last N rows from the filtered data.
        get_first_n_days : int, optional
            Return data from the first N days. If start_date specified, returns N days from start.
            Otherwise, returns first N days from beginning of data.
        get_last_n_days : int, optional
            Return data from the last N days. If end_date specified, returns N days before end.
            Otherwise, returns last N days from end of data.
        get_first_n_minutes : int, optional
            Return data from the first N minutes. Applied after date filtering.
        get_last_n_minutes : int, optional
            Return data from the last N minutes. Applied after date filtering.

        Notes
        -----
        Only ONE get_* parameter can be specified at a time. If multiple are provided,
        the function will stop with a warning.

        Returns
        -------
        tuple[pd.DataFrame or None, list of str]
            A tuple containing:
            - df: Pandas DataFrame with the queried data, or None if no data found or error occurred
            - warnings: List of warning/error messages:
                * "NO_DATA_DIR": Data directory not found for this series
                * "NO_FILES": No data files found in the specified range
                * "MAX_ROWS_REACHED": Result truncated to max_rows limit
                * "READ_ERROR: <details>": Error reading data files
                * "INFLUXDB_NOT_CONFIGURED": InfluxDB sink requested but not configured

        Example Usage
        -------------
        # Query all available data for AAPL stock at 5m interval
        df, warnings = manager.query_local_data(
            asset="stock",
            symbol="AAPL",
            interval="5m",
            sink="parquet",
            start_date="2024-01-01"
        )

        # Query with specific datetime range and row limit
        df, warnings = manager.query_local_data(
            asset="option",
            symbol="SPY",
            interval="1m",
            sink="csv",
            start_datetime="2024-01-01T14:30:00",
            end_datetime="2024-01-01T16:00:00",
            max_rows=10000
        )

        # Check for warnings
        if warnings:
            for w in warnings:
                print(f"Warning: {w}")
        """
        warnings = []

        # Validate that only one get_* parameter is specified
        get_params = [
            get_first_n_rows, get_last_n_rows,
            get_first_n_days, get_last_n_days,
            get_first_n_minutes, get_last_n_minutes
        ]
        active_gets = [p for p in get_params if p is not None]
        if len(active_gets) > 1:
            warnings.append(
                "MULTIPLE_GET_PARAMS: Only one get_* parameter can be specified at a time. "
                f"Found {len(active_gets)} active parameters."
            )
            return None, warnings

        # Validate that get_*_n_minutes is not used with 1d interval
        if (get_first_n_minutes or get_last_n_minutes) and interval == '1d':
            warnings.append(
                "INVALID_MINUTES_PARAM: get_first_n_minutes and get_last_n_minutes cannot be used with interval='1d'. "
                "Use get_first_n_days or get_last_n_days instead."
            )
            return None, warnings

        # Validate inputs
        self._validate_sink(sink)
        self._validate_interval(interval)

        sink_lower = sink.lower()

        # Parse start datetime
        start_ts = None
        if start_datetime:
            try:
                start_ts = pd.to_datetime(start_datetime, utc=True)
            except Exception as e:
                warnings.append(f"INVALID_START_DATETIME: {e}")
                return None, warnings
        elif start_date:
            try:
                start_ts = pd.to_datetime(start_date, utc=True)
            except Exception as e:
                warnings.append(f"INVALID_START_DATE: {e}")
                return None, warnings
        elif not _allow_full_scan and not any([get_first_n_rows, get_last_n_rows, get_first_n_days, get_last_n_days, get_first_n_minutes, get_last_n_minutes]):
            # Start date is required only if no get_* parameter is specified (unless full scan is explicitly allowed)
            warnings.append("NO_START: Either start_date, start_datetime, or a get_* parameter is required")
            return None, warnings

        # Parse end datetime
        end_ts = None
        if end_datetime:
            try:
                end_ts = pd.to_datetime(end_datetime, utc=True)
            except Exception as e:
                warnings.append(f"INVALID_END_DATETIME: {e}")
                return None, warnings
        elif end_date:
            try:
                # End date means "up to and including this entire day"
                # So we set it to the start of the NEXT day (exclusive upper bound)
                end_ts = pd.to_datetime(end_date, utc=True) + pd.Timedelta(days=1)
            except Exception as e:
                warnings.append(f"INVALID_END_DATE: {e}")
                return None, warnings

        # Handle different sinks
        if sink_lower == "influxdb":
            # Query InfluxDB
            try:
                cli = self._ensure_influx_client()
            except Exception as e:
                warnings.append(f"INFLUXDB_NOT_CONFIGURED: {e}")
                return None, warnings

            # Build measurement name
            measurement = f"{symbol.upper()}-{asset.lower()}-{interval.lower()}"
            if self.cfg.influx_measure_prefix:
                measurement = f"{self.cfg.influx_measure_prefix}{measurement}"

            # Build query
            where_clauses = []
            if start_ts:
                where_clauses.append(f"time >= TIMESTAMP '{start_ts.isoformat().replace('+00:00', 'Z')}'")
            if end_ts:
                where_clauses.append(f"time < TIMESTAMP '{end_ts.isoformat().replace('+00:00', 'Z')}'")

            where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
            limit_str = f"LIMIT {max_rows}" if max_rows else ""

            query = f'SELECT * FROM "{measurement}" WHERE {where_str} ORDER BY time {limit_str}'

            try:
                result = cli.query(query)
                df = result.to_pandas() if hasattr(result, "to_pandas") else result

                if df is None or df.empty:
                    warnings.append("NO_DATA: No data found in InfluxDB for specified range")
                    return None, warnings

                # Normalize timestamp column
                if "time" in df.columns:
                    df = df.rename(columns={"time": "timestamp"})
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_localize(None)

                # Apply get_* filtering and smart ordering
                df = self._apply_get_filters_and_ordering(
                    df, asset, warnings,
                    get_first_n_rows, get_last_n_rows,
                    get_first_n_days, get_last_n_days,
                    get_first_n_minutes, get_last_n_minutes,
                    start_ts, end_ts
                )

                if df is None:
                    return None, warnings

                # Apply max_rows limit
                if max_rows and len(df) > max_rows:
                    df = df.head(max_rows)
                    warnings.append(f"MAX_ROWS_REACHED: Result limited to {max_rows} rows")

                return df, warnings

            except Exception as e:
                warnings.append(f"INFLUXDB_QUERY_ERROR: {e}")
                return None, warnings

        else:
            # Query CSV or Parquet files
            data_dir = os.path.join(self.cfg.root_dir, "data", asset, symbol, interval, sink_lower)

            if not os.path.isdir(data_dir):
                warnings.append(f"NO_DATA_DIR: Directory not found: {data_dir}")
                return None, warnings

            # Get all files for this series
            all_files = self._list_series_files(asset, symbol, interval, sink_lower)

            if not all_files:
                warnings.append(f"NO_FILES: No data files found in {data_dir}")
                return None, warnings

            # Filter files by date range based on filename
            # Filename format: YYYY-MM-DDT00-00-00Z-SYMBOL-asset-interval_partNN.ext
            relevant_files = []
            for f in all_files:
                fname = os.path.basename(f)
                # Extract date from filename
                date_match = re.match(r'^(\d{4}-\d{2}-\d{2})', fname)
                if date_match:
                    file_date = pd.to_datetime(date_match.group(1), utc=True)
                    # Include file if its date overlaps with our range
                    if start_ts and file_date < start_ts.normalize() - pd.Timedelta(days=1):
                        continue  # Too early
                    if end_ts and file_date > end_ts.normalize() + pd.Timedelta(days=1):
                        continue  # Too late
                    relevant_files.append(f)

            if not relevant_files:
                warnings.append(f"NO_FILES_IN_RANGE: No files found for date range {start_ts} to {end_ts}")
                return None, warnings

            # Read and concatenate files
            dfs = []
            total_rows = 0

            try:
                for file_path in relevant_files:
                    if sink_lower == "csv":
                        df_chunk = pd.read_csv(file_path, dtype=str)
                    else:  # parquet
                        df_chunk = pd.read_parquet(file_path)

                    if df_chunk.empty:
                        continue

                    # Normalize timestamp column
                    time_col = self._detect_time_col(df_chunk.columns)
                    if time_col:
                        # Parse with mixed format support (handles ISO8601 and other formats)
                        df_chunk[time_col] = pd.to_datetime(df_chunk[time_col], format='mixed', utc=True, errors='coerce').dt.tz_localize(None)

                        # Filter by timestamp range
                        if start_ts:
                            df_chunk = df_chunk[df_chunk[time_col] >= start_ts.tz_localize(None)]
                        if end_ts:
                            df_chunk = df_chunk[df_chunk[time_col] < end_ts.tz_localize(None)]

                    if not df_chunk.empty:
                        dfs.append(df_chunk)
                        total_rows += len(df_chunk)

                        # Check max_rows limit
                        if max_rows and total_rows >= max_rows:
                            warnings.append(f"MAX_ROWS_REACHED: Result limited to {max_rows} rows (more data available)")
                            break

                if not dfs:
                    warnings.append("NO_DATA: No data found in specified time range")
                    return None, warnings

                # Concatenate all chunks
                result_df = pd.concat(dfs, ignore_index=True)

                # Apply get_* filtering and smart ordering
                result_df = self._apply_get_filters_and_ordering(
                    result_df, asset, warnings,
                    get_first_n_rows, get_last_n_rows,
                    get_first_n_days, get_last_n_days,
                    get_first_n_minutes, get_last_n_minutes,
                    start_ts, end_ts
                )

                if result_df is None:
                    return None, warnings

                # Apply max_rows limit
                if max_rows and len(result_df) > max_rows:
                    result_df = result_df.head(max_rows)
                    warnings.append(f"MAX_ROWS_REACHED: Result limited to {max_rows} rows (more data available)")

                return result_df, warnings

            except Exception as e:
                warnings.append(f"READ_ERROR: {e}")
                return None, warnings

    def _apply_get_filters_and_ordering(
        self,
        df: pd.DataFrame,
        asset: str,
        warnings: List[str],
        get_first_n_rows: Optional[int],
        get_last_n_rows: Optional[int],
        get_first_n_days: Optional[int],
        get_last_n_days: Optional[int],
        get_first_n_minutes: Optional[int],
        get_last_n_minutes: Optional[int],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp]
    ) -> Optional[pd.DataFrame]:
        """Apply get_* filters and smart ordering to query results.

        LOGIC:
        1. First apply chronological filtering (get_last_* means most recent, get_first_* means oldest)
        2. Then apply smart ordering based on asset type

        For options: orders by expiration ASC, strike ASC, timestamp DESC
        For non-options: orders by timestamp DESC (most recent first)
        """
        if df is None or df.empty:
            return df

        # Detect time column
        time_col = self._detect_time_col(df.columns)
        if not time_col and any([get_first_n_days, get_last_n_days, get_first_n_minutes, get_last_n_minutes, get_first_n_rows, get_last_n_rows]):
            warnings.append("NO_TIMESTAMP_COLUMN: Cannot apply time-based filters without timestamp column")
            return None

        # Convert timestamp column to datetime
        if time_col:
            df[time_col] = pd.to_datetime(df[time_col], format='mixed', errors='coerce')

        # Normalize start_ts and end_ts to tz-naive for comparison
        if start_ts and hasattr(start_ts, 'tz') and start_ts.tz is not None:
            start_ts = start_ts.tz_localize(None)
        if end_ts and hasattr(end_ts, 'tz') and end_ts.tz is not None:
            end_ts = end_ts.tz_localize(None)

        # STEP 1: Apply chronological filtering FIRST
        if get_first_n_days:
            # Get first N calendar days from start (oldest data)
            # NOTE: Uses calendar days, not 24-hour periods
            if time_col:
                # Get the starting date
                if start_ts:
                    start_date = start_ts.normalize()
                else:
                    # From beginning of data
                    min_ts = df[time_col].min()
                    start_date = min_ts.normalize()

                # Calculate end of the Nth calendar day
                end_of_nth_day = start_date + pd.Timedelta(days=get_first_n_days)

                # Filter: from start_date (00:00:00) to end_of_nth_day (00:00:00 exclusive)
                df = df[(df[time_col] >= start_date) & (df[time_col] < end_of_nth_day)]

        elif get_last_n_days:
            # Get last N calendar days before end (most recent data)
            # NOTE: Uses calendar days, not 24-hour periods
            if time_col:
                # Get the ending date
                if end_ts:
                    # end_ts is already set to start of next day (exclusive upper bound)
                    # when coming from end_date parameter
                    end_date = end_ts.normalize()
                else:
                    # From end of data
                    max_ts = df[time_col].max()
                    end_date = max_ts.normalize() + pd.Timedelta(days=1)  # Next day at 00:00:00

                # Calculate start of the (N days back) calendar day
                start_of_nth_day = end_date - pd.Timedelta(days=get_last_n_days)

                # Filter: from start_of_nth_day (00:00:00 inclusive) to end_date (00:00:00 exclusive)
                df = df[(df[time_col] >= start_of_nth_day) & (df[time_col] < end_date)]

        elif get_first_n_minutes:
            # Get first N minutes from start (oldest data)
            # Always use actual min timestamp from data (already filtered by start_date)
            if time_col:
                min_ts = df[time_col].min()
                cutoff = min_ts + pd.Timedelta(minutes=get_first_n_minutes)
                df = df[df[time_col] <= cutoff]

        elif get_last_n_minutes:
            # Get last N minutes before end (most recent data)
            # Always use actual max timestamp from data (already filtered by end_date)
            if time_col:
                max_ts = df[time_col].max()
                cutoff = max_ts - pd.Timedelta(minutes=get_last_n_minutes)
                df = df[df[time_col] >= cutoff]

        elif get_first_n_rows:
            # Get first N rows by timestamp (oldest)
            if time_col:
                df = df.nsmallest(get_first_n_rows, time_col)
            else:
                df = df.head(get_first_n_rows)

        elif get_last_n_rows:
            # Get last N rows by timestamp (most recent)
            if time_col:
                df = df.nlargest(get_last_n_rows, time_col)
            else:
                df = df.tail(get_last_n_rows)

        if df.empty:
            return None

        # STEP 2: Apply smart ordering based on asset type
        # Make a copy to avoid SettingWithCopyWarning when modifying
        df = df.copy()

        if asset.lower() == "option":
            # Options: order by expiration ASC, strike ASC, timestamp DESC
            sort_cols = []
            if "expiration" in df.columns:
                # Convert expiration to datetime using .loc[] to avoid SettingWithCopyWarning
                df.loc[:, "expiration"] = pd.to_datetime(df["expiration"], format='mixed', errors='coerce')
                sort_cols.append("expiration")
            if "strike" in df.columns:
                sort_cols.append("strike")
            if time_col:
                sort_cols.append(time_col)

            if sort_cols:
                # Create ascending list (True for exp/strike, False for timestamp)
                ascending_list = [True] * (len(sort_cols) - 1) + [False] if time_col in sort_cols else [True] * len(sort_cols)
                df = df.sort_values(sort_cols, ascending=ascending_list).reset_index(drop=True)
        else:
            # Non-options: order by timestamp DESC (most recent first)
            if time_col:
                df = df.sort_values(time_col, ascending=False).reset_index(drop=True)

        return df if not df.empty else None

    def available_expiration_chains(
        self,
        symbol: str,
        interval: str,
        sink: str = "csv",
        existing_chain: bool = True,
        active_from_date: Optional[str] = None,
        active_to_date: Optional[str] = None
    ) -> Tuple[Optional[pd.DataFrame], List[str]]:
        """Get available option expiration chains for a given symbol, interval, and sink.

        This function queries local data storage (CSV, Parquet, or InfluxDB) to discover which
        expiration dates are available in the historical data. Useful for options analysis to
        understand which expiration chains exist in the dataset.

        Parameters
        ----------
        symbol : str
            The ticker symbol (e.g., 'SPY', 'AAPL').
        interval : str
            The time interval (e.g., 'tick', '5m', '1d').
        sink : str, optional
            The data sink to query ('csv', 'parquet', or 'influxdb'). Defaults to 'csv'.
        existing_chain : bool, optional
            If True (default), returns only expiration dates >= today (active/existing chains).
            If False, returns all historical expiration dates.
        active_from_date : str, optional
            Filter expiration dates >= this date (format: 'YYYY-MM-DD').
            If None, no lower bound filtering is applied.
        active_to_date : str, optional
            Filter expiration dates <= this date (format: 'YYYY-MM-DD').
            If None, no upper bound filtering is applied.

        Returns
        -------
        Tuple[Optional[pd.DataFrame], List[str]]
            - DataFrame with columns: ['expiration', 'count'] where count is the number of records for each expiration
            - List of warning messages

        Example Usage
        -------------
        # Get all active (future) expiration chains for SPY 5m options
        df, warnings = manager.available_expiration_chains(
            symbol='SPY',
            interval='5m',
            sink='csv',
            existing_chain=True
        )

        # Get expiration chains active in a specific date range
        df, warnings = manager.available_expiration_chains(
            symbol='TLRY',
            interval='tick',
            sink='influxdb',
            existing_chain=False,
            active_from_date='2025-09-01',
            active_to_date='2025-10-31'
        )
        """
        warnings = []
        asset = 'option'  # This function is only for options

        # Validate inputs
        self._validate_sink(sink)
        self._validate_interval(interval)

        sink_lower = sink.lower()
        today = pd.Timestamp.now().normalize()

        # Query all data (no filtering by dates initially)
        df, query_warnings = self.query_local_data(
            asset=asset,
            symbol=symbol,
            interval=interval,
            sink=sink,
            _allow_full_scan=True
        )
        warnings.extend(query_warnings)

        if df is None or df.empty:
            warnings.append("NO_DATA: No data found for the specified symbol, interval, and sink")
            return None, warnings

        # Check if expiration column exists
        if 'expiration' not in df.columns:
            warnings.append("NO_EXPIRATION_COLUMN: Data does not contain 'expiration' column (not options data?)")
            return None, warnings

        # Convert expiration to datetime
        df['expiration'] = pd.to_datetime(df['expiration'], errors='coerce')

        # Remove rows with invalid expiration dates
        valid_exp = df['expiration'].notna()
        if not valid_exp.all():
            invalid_count = (~valid_exp).sum()
            warnings.append(f"INVALID_EXPIRATIONS: Removed {invalid_count} rows with invalid expiration dates")
            df = df[valid_exp]

        if df.empty:
            warnings.append("NO_VALID_EXPIRATIONS: No valid expiration dates found")
            return None, warnings

        # Apply filtering based on existing_chain and active_from_date/active_to_date
        if existing_chain:
            df = df[df['expiration'] >= today]
            if df.empty:
                warnings.append("NO_ACTIVE_CHAINS: No expiration chains exist from today onwards")
                return None, warnings

        if active_from_date:
            try:
                from_dt = pd.to_datetime(active_from_date).normalize()
                df = df[df['expiration'] >= from_dt]
            except Exception as e:
                warnings.append(f"INVALID_FROM_DATE: Could not parse active_from_date '{active_from_date}': {e}")
                return None, warnings

        if active_to_date:
            try:
                to_dt = pd.to_datetime(active_to_date).normalize()
                df = df[df['expiration'] <= to_dt]
            except Exception as e:
                warnings.append(f"INVALID_TO_DATE: Could not parse active_to_date '{active_to_date}': {e}")
                return None, warnings

        if df.empty:
            warnings.append("NO_DATA_IN_RANGE: No expiration chains found in the specified date range")
            return None, warnings

        # Group by expiration and count
        result = df.groupby('expiration').size().reset_index(name='count')
        result = result.sort_values('expiration')

        return result, warnings

    def available_strikes_by_expiration(
        self,
        symbol: str,
        interval: str,
        sink: str = "csv",
        expirations: Optional[List[str]] = None
    ) -> Tuple[Optional[Dict[str, List[float]]], List[str]]:
        """Get available strike prices for each expiration chain.

        This function queries local data storage (CSV, Parquet, or InfluxDB) to discover which
        strike prices are available for each expiration date. Returns a mapping of expiration → strikes.

        Parameters
        ----------
        symbol : str
            The ticker symbol (e.g., 'SPY', 'AAPL').
        interval : str
            The time interval (e.g., 'tick', '5m', '1d').
        sink : str, optional
            The data sink to query ('csv', 'parquet', or 'influxdb'). Defaults to 'csv'.
        expirations : List[str], optional
            List of specific expiration dates to query (format: 'YYYY-MM-DD').
            If None, returns strikes for all available expirations.

        Returns
        -------
        Tuple[Optional[Dict[str, List[float]]], List[str]]
            - Dictionary mapping expiration dates (str) to sorted lists of strike prices (float)
            - List of warning messages

        Example Usage
        -------------
        # Get all strikes for all expirations
        strikes_map, warnings = manager.available_strikes_by_expiration(
            symbol='SPY',
            interval='5m',
            sink='csv'
        )

        # Get strikes for specific expirations
        strikes_map, warnings = manager.available_strikes_by_expiration(
            symbol='TLRY',
            interval='tick',
            sink='influxdb',
            expirations=['2025-11-21', '2025-12-19']
        )

        # Result format: {'2025-11-21': [1.0, 1.5, 2.0, ...], '2025-12-19': [1.5, 2.0, 2.5, ...]}
        """
        warnings = []
        asset = 'option'  # This function is only for options

        # Validate inputs
        self._validate_sink(sink)
        self._validate_interval(interval)

        # Query all data
        df, query_warnings = self.query_local_data(
            asset=asset,
            symbol=symbol,
            interval=interval,
            sink=sink,
            _allow_full_scan=True
        )
        warnings.extend(query_warnings)

        if df is None or df.empty:
            warnings.append("NO_DATA: No data found for the specified symbol, interval, and sink")
            return None, warnings

        # Check if required columns exist
        if 'expiration' not in df.columns:
            warnings.append("NO_EXPIRATION_COLUMN: Data does not contain 'expiration' column (not options data?)")
            return None, warnings

        if 'strike' not in df.columns:
            warnings.append("NO_STRIKE_COLUMN: Data does not contain 'strike' column (not options data?)")
            return None, warnings

        # Convert expiration to datetime and strike to float
        df['expiration'] = pd.to_datetime(df['expiration'], errors='coerce')
        df['strike'] = pd.to_numeric(df['strike'], errors='coerce')

        # Remove rows with invalid data
        valid_data = df['expiration'].notna() & df['strike'].notna()
        if not valid_data.all():
            invalid_count = (~valid_data).sum()
            warnings.append(f"INVALID_DATA: Removed {invalid_count} rows with invalid expiration or strike values")
            df = df[valid_data]

        if df.empty:
            warnings.append("NO_VALID_DATA: No valid expiration/strike data found")
            return None, warnings

        # Filter by specific expirations if provided
        if expirations:
            try:
                exp_dates = [pd.to_datetime(exp).normalize() for exp in expirations]
                df = df[df['expiration'].isin(exp_dates)]
                if df.empty:
                    warnings.append(f"NO_MATCHING_EXPIRATIONS: No data found for specified expirations: {expirations}")
                    return None, warnings
            except Exception as e:
                warnings.append(f"INVALID_EXPIRATIONS: Could not parse expiration dates '{expirations}': {e}")
                return None, warnings

        # Group by expiration and get unique strikes
        result = {}
        for expiration, group in df.groupby('expiration'):
            exp_str = expiration.strftime('%Y-%m-%d')
            strikes = sorted(group['strike'].unique().tolist())
            result[exp_str] = strikes

        # Sort by expiration date
        result = dict(sorted(result.items()))

        return result, warnings

    def get_storage_stats(
        self,
        asset: Optional[str] = None,
        symbol: Optional[str] = None,
        interval: Optional[str] = None,
        sink: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get storage statistics for local databases.

        Returns a DataFrame with columns:
        - asset: stock/option
        - symbol: ticker symbol
        - interval: tick/5m/1d/etc
        - sink: csv/parquet/influxdb
        - num_files: number of files (CSV/Parquet only, N/A for InfluxDB)
        - size_bytes: total size in bytes
        - size_mb: total size in MB
        - size_gb: total size in GB
        - first_date: earliest data date
        - last_date: latest data date
        - days_span: number of days covered

        Parameters:
        -----------
        asset : str, optional
            Filter by asset type (stock/option)
        symbol : str, optional
            Filter by symbol
        interval : str, optional
            Filter by interval (tick/5m/1d/etc)
        sink : str, optional
            Filter by sink (csv/parquet/influxdb)

        Returns:
        --------
        pd.DataFrame
            Storage statistics grouped by (asset, symbol, interval, sink)
        """
        import os
        from pathlib import Path

        results = []

        # Get available data first
        available = self.list_available_data(
            asset=asset,
            symbol=symbol,
            interval=interval,
            sink=sink,
        )

        # Apply filters if provided
        if asset:
            available = available[available['asset'] == asset]
        if symbol:
            available = available[available['symbol'] == symbol]
        if interval:
            available = available[available['interval'] == interval]
        if sink:
            available = available[available['sink'] == sink]

        # Group by (asset, symbol, interval, sink) and calculate sizes
        for (grp_asset, grp_symbol, grp_interval, grp_sink), group in available.groupby(
            ['asset', 'symbol', 'interval', 'sink']
        ):
            total_size = 0
            num_files = 0
            first_date = None
            last_date = None

            if grp_sink in ['csv', 'parquet']:
                # For file-based sinks, sum up file sizes by scanning the series directory
                try:
                    file_paths = self._list_series_files(grp_asset, grp_symbol, grp_interval, grp_sink)
                except Exception:
                    file_paths = []

                num_files = len(file_paths)
                for file_path in file_paths:
                    if os.path.exists(file_path):
                        try:
                            total_size += os.path.getsize(file_path)
                        except Exception:
                            pass  # Skip files we can't read

                # Get date range from metadata
                if not group.empty:
                    first_date = pd.to_datetime(group['first_datetime']).min()
                    last_date = pd.to_datetime(group['last_datetime']).max()

            elif grp_sink == 'influxdb':
                # For InfluxDB v3: try to get real disk size from data directory
                measurement = f"{grp_symbol}-{grp_asset}-{grp_interval}"

                # Try real disk size first
                real_size_found = False
                if self.cfg.influx_data_dir:
                    try:
                        total_size = 0
                        num_files = 0
                        for file_path in self._iter_influx_parquet_files(measurement):
                            try:
                                total_size += os.path.getsize(file_path)
                                num_files += 1
                            except Exception:
                                continue
                        if num_files > 0:
                            real_size_found = True
                    except Exception:
                        pass  # Fall back to estimation

                # Fall back to estimation if real size not available
                if not real_size_found:
                    try:
                        # Get first and last timestamp
                        query_first = f'''
                            SELECT MIN(time) AS first_ts FROM "{measurement}"
                        '''
                        query_last = f'''
                            SELECT MAX(time) AS last_ts FROM "{measurement}"
                        '''

                        df_first = self._influx_query_dataframe(query_first)
                        df_last = self._influx_query_dataframe(query_last)

                        first_val = self._extract_scalar_from_df(df_first, ["first_ts", "min"])
                        last_val = self._extract_scalar_from_df(df_last, ["last_ts", "max"])

                        co_first = self._coerce_timestamp(first_val)
                        co_last = self._coerce_timestamp(last_val)

                        if co_first is not None:
                            first_date = co_first
                        if co_last is not None:
                            last_date = co_last

                        # Count total points
                        query_count = f'''
                            SELECT COUNT(*) AS point_count FROM "{measurement}"
                        '''
                        count_df = self._influx_query_dataframe(query_count)
                        point_val = self._extract_scalar_from_df(count_df, ["point_count", "count"])
                        if point_val is not None:
                            point_count = int(point_val)

                            # Estimate size: assume ~500 bytes per point (conservative estimate)
                            # This includes timestamp, tags, fields, and overhead
                            # For options: more fields → ~800 bytes
                            # For stocks: fewer fields → ~300 bytes
                            bytes_per_point = 800 if grp_asset == 'option' else 300
                            total_size = point_count * bytes_per_point
                            num_files = None  # N/A for InfluxDB when using estimation
                    except Exception:
                        total_size = 0
                        num_files = None

                if first_date is None or last_date is None:
                    inferred_first, inferred_last = self._infer_first_last_from_sink(
                        grp_asset,
                        grp_symbol,
                        grp_interval,
                        "influxdb",
                    )
                    if first_date is None and inferred_first is not None:
                        first_date = inferred_first
                    if last_date is None and inferred_last is not None:
                        last_date = inferred_last

            # Calculate days span
            days_span = None
            if first_date is not None and last_date is not None:
                try:
                    # Normalize timezones
                    if hasattr(first_date, 'tz') and first_date.tz is not None:
                        first_date = first_date.tz_localize(None)
                    if hasattr(last_date, 'tz') and last_date.tz is not None:
                        last_date = last_date.tz_localize(None)
                    days_span = (last_date - first_date).days + 1
                except Exception:
                    days_span = None

            # Convert to different units
            size_mb = total_size / (1024 * 1024) if total_size > 0 else 0
            size_gb = total_size / (1024 * 1024 * 1024) if total_size > 0 else 0

            results.append({
                'asset': grp_asset,
                'symbol': grp_symbol,
                'interval': grp_interval,
                'sink': grp_sink,
                'num_files': num_files if grp_sink in ['csv', 'parquet'] else 'N/A',
                'size_bytes': total_size,
                'size_mb': round(size_mb, 2),
                'size_gb': round(size_gb, 3),
                'first_date': first_date,
                'last_date': last_date,
                'days_span': days_span
            })

        # Create DataFrame
        df = pd.DataFrame(results)

        # Sort by size descending
        if not df.empty:
            df = df.sort_values('size_bytes', ascending=False).reset_index(drop=True)

        return df

    def get_storage_summary(self) -> dict:
        """
        Get aggregated storage summary statistics.

        Returns a dictionary with:
        - total: Total storage across all databases
        - by_sink: Storage grouped by sink (csv/parquet/influxdb)
        - by_asset: Storage grouped by asset (stock/option)
        - by_interval: Storage grouped by interval (tick/5m/1d/etc)
        - top_symbols: Top 10 symbols by storage size

        Returns:
        --------
        dict
            Nested dictionary with summary statistics
        """
        # Get detailed stats
        stats = self.get_storage_stats()

        if stats.empty:
            return {
                'total': {'size_bytes': 0, 'size_mb': 0, 'size_gb': 0, 'series_count': 0},
                'by_sink': {},
                'by_asset': {},
                'by_interval': {},
                'top_symbols': []
            }

        # Total storage
        total_bytes = stats['size_bytes'].sum()
        total_mb = round(total_bytes / (1024 * 1024), 2)
        total_gb = round(total_bytes / (1024 * 1024 * 1024), 3)

        # By sink
        by_sink = {}
        for sink, group in stats.groupby('sink'):
            sink_bytes = group['size_bytes'].sum()
            by_sink[sink] = {
                'size_bytes': sink_bytes,
                'size_mb': round(sink_bytes / (1024 * 1024), 2),
                'size_gb': round(sink_bytes / (1024 * 1024 * 1024), 3),
                'series_count': len(group),
                'percentage': round(100 * sink_bytes / total_bytes, 1) if total_bytes > 0 else 0
            }

        # By asset
        by_asset = {}
        for asset, group in stats.groupby('asset'):
            asset_bytes = group['size_bytes'].sum()
            by_asset[asset] = {
                'size_bytes': asset_bytes,
                'size_mb': round(asset_bytes / (1024 * 1024), 2),
                'size_gb': round(asset_bytes / (1024 * 1024 * 1024), 3),
                'series_count': len(group),
                'percentage': round(100 * asset_bytes / total_bytes, 1) if total_bytes > 0 else 0
            }

        # By interval
        by_interval = {}
        for interval, group in stats.groupby('interval'):
            interval_bytes = group['size_bytes'].sum()
            by_interval[interval] = {
                'size_bytes': interval_bytes,
                'size_mb': round(interval_bytes / (1024 * 1024), 2),
                'size_gb': round(interval_bytes / (1024 * 1024 * 1024), 3),
                'series_count': len(group),
                'percentage': round(100 * interval_bytes / total_bytes, 1) if total_bytes > 0 else 0
            }

        # Top symbols by storage
        symbol_totals = stats.groupby('symbol')['size_bytes'].sum().sort_values(ascending=False)
        top_symbols = []
        for symbol, size_bytes in symbol_totals.head(10).items():
            top_symbols.append({
                'symbol': symbol,
                'size_bytes': size_bytes,
                'size_mb': round(size_bytes / (1024 * 1024), 2),
                'size_gb': round(size_bytes / (1024 * 1024 * 1024), 3),
                'percentage': round(100 * size_bytes / total_bytes, 1) if total_bytes > 0 else 0
            })

        return {
            'total': {
                'size_bytes': total_bytes,
                'size_mb': total_mb,
                'size_gb': total_gb,
                'series_count': len(stats)
            },
            'by_sink': by_sink,
            'by_asset': by_asset,
            'by_interval': by_interval,
            'top_symbols': top_symbols
        }

    # =========================================================================
    # (END)
    # LOCAL DB QUERY
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # COMMON UTILITIES
    # =========================================================================
    def clear_first_date_cache(self, asset: str, symbol: str, req_type: str) -> None:
        """Deletes a specific first-date cache entry from memory and persists the change to disk.

        This method is useful when you know the first available date for a symbol has changed (e.g., after
        historical data becomes available) and you want to force a fresh discovery on the next sync.

        Parameters
        ----------
        asset : str
            The asset type (e.g., 'stock', 'option', 'index').
        symbol : str
            The ticker symbol or root symbol whose cache entry should be cleared.
        req_type : str
            The request type (e.g., 'trade', 'quote', 'ohlc') that identifies the specific cache entry.

        Returns
        -------
        None
            The cache entry is removed if it exists, and the cache file is saved immediately.

        Example Usage
        -------------
        # Clear cached first date for SPY options to force re-discovery
        manager.clear_first_date_cache("option", "SPY", "trade")
        # Next sync will re-discover the first available date
        """
        key = self._cache_key(asset, symbol, req_type)
        if key in self._coverage_cache:
            del self._coverage_cache[key]
            self._save_cache_file()




    def _validate_interval(self, interval: str) -> None:
        """Validates that the requested bar interval is supported by the ThetaData client.

        This method checks the interval string against the allowed intervals defined in the client's
        Interval type. If invalid, it raises an error with a helpful message listing all valid options.

        Parameters
        ----------
        interval : str
            The interval string to validate (e.g., '1d', '5m', '1h', 'tick').

        Returns
        -------
        None
            Returns nothing if validation passes.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _sync_symbol() before starting any download to ensure interval is valid
        # - Various download methods to fail fast on invalid intervals
        """
        if interval not in _ALLOWED_INTERVALS:
            allowed = ", ".join(sorted(_ALLOWED_INTERVALS))
            raise ValueError(
                f"Unsupported interval '{interval}'. Allowed values are: {allowed}. "
                f"For daily bars use '1d'."
            )


    def _validate_sink(self, sink: str) -> None:
        """Validates that the requested output sink is supported.

        This method checks that the sink parameter is one of the supported output formats.
        If invalid, it raises an error listing all valid sink options.

        Parameters
        ----------
        sink : str
            The sink type to validate. Supported values: 'csv', 'parquet', 'influxdb'.

        Returns
        -------
        None
            Returns nothing if validation passes.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _sync_symbol() before starting downloads to ensure sink is valid
        # - Methods that write data to verify the output format is supported
        """
        s = (sink or "").strip().lower()
        if s not in ("csv", "parquet", "influxdb"):
            raise ValueError(f"Unsupported sink '{sink}'. Allowed: csv | parquet | influxdb")



    # ------------------------- CORE SYNC -----------------------------

    def _as_utc(self, x):
        """Converts an ISO8601 string or datetime object to a timezone-aware UTC datetime.

        This helper method normalizes various datetime representations to a consistent UTC datetime format,
        handling both timezone-naive and timezone-aware inputs.

        Parameters
        ----------
        x : str or datetime
            An ISO8601 formatted datetime string (with or without 'Z' suffix) or a datetime object.
            String format can be like '2024-01-15T10:30:00Z' or '2024-01-15T10:30:00+00:00'.

        Returns
        -------
        datetime
            A timezone-aware datetime object in UTC. If input was timezone-naive, UTC is assumed.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _sync_symbol() for normalizing start/end times
        # - _compute_resume_start_datetime() for time conversions
        # - Various date handling methods throughout the manager
        """
        if isinstance(x, str):
            x = dt.fromisoformat(x.replace("Z", "+00:00"))
        if x.tzinfo is None:
            x = x.replace(tzinfo=self.UTC)
        return x.astimezone(self.UTC)

    def _floor_to_interval_et(self, ts_utc: dt, minutes: int) -> dt:
        """Floors a UTC timestamp to the nearest interval boundary in Eastern Time, then converts back to UTC.

        This method is used to align timestamps to bar boundaries (e.g., 5-minute, 15-minute intervals) according
        to Eastern Time market hours, which is important for consistent bar alignment.

        Parameters
        ----------
        ts_utc : datetime
            A timezone-aware datetime in UTC that needs to be floored to an interval boundary.
        minutes : int
            The interval size in minutes. For example, 5 for 5-minute bars, 60 for hourly bars.

        Returns
        -------
        datetime
            A timezone-aware datetime in UTC, floored to the specified interval boundary in Eastern Time.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _sync_symbol() when computing resume start times for intraday bars
        # - _force_start_hms_from_max_ts() (nested function) for bar alignment
        """
        et = ts_utc.astimezone(self.ET).replace(second=0, microsecond=0)
        et = et.replace(minute=(et.minute // minutes) * minutes)
        return et.astimezone(self.UTC)

    # === >>> DATE PARAM HELPERS — BEGIN
    def _iso_date_only(self, s: str) -> str:
        """Extracts the date portion from an ISO datetime string, returning only 'YYYY-MM-DD'.

        This helper handles various datetime string formats and extracts just the date component,
        discarding any time information.

        Parameters
        ----------
        s : str
            A date or datetime string in formats like 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM:SS',
            or 'YYYY-MM-DD HH:MM:SS'.

        Returns
        -------
        str
            The date portion in 'YYYY-MM-DD' format.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _td_ymd() to normalize date strings before formatting
        # - Various methods that need to extract date from datetime strings
        """
        s = str(s)
        if "T" in s:
            s = s.split("T", 1)[0]
        elif " " in s:
            s = s.split(" ", 1)[0]
        return s
    
    def _td_ymd(self, day_iso: str) -> str:
        """Converts an ISO date string to ThetaData's YYYYMMDD format required for API date parameters.

        This method normalizes various date formats to the compact YYYYMMDD format expected by ThetaData API
        endpoints, with validation to ensure the result is exactly 8 digits.

        Parameters
        ----------
        day_iso : str
            An ISO date string like 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'. The time portion is ignored.

        Returns
        -------
        str
            A date string in 'YYYYMMDD' format (8 digits, no separators).

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _download_and_store_options() when building API request parameters
        # - _download_and_store_equity_or_index() for date parameter formatting
        # - _expirations_that_traded() for daily expiration queries
        """
        d = self._iso_date_only(day_iso).replace("-", "")
        if len(d) != 8 or not d.isdigit():
            raise ValueError(f"Bad day_iso '{day_iso}' → '{d}' (expected YYYYMMDD)")
        return d

    def _greeks_version_for_expiration(self, expiration: str, day_ymd: str) -> str:
        """Return Greeks version for 0DTE vs non-0DTE expirations."""
        exp = (expiration or "").replace("-", "").strip()
        if len(exp) == 6 and exp.isdigit():
            exp = f"20{exp}"
        if len(exp) == 8 and exp.isdigit() and day_ymd and exp == day_ymd:
            return "1"
        return "latest"
    # === <<< DATE PARAM HELPERS — END

    
    
    def _iso_stamp(self, ts) -> str:
        """Convert a timestamp to a filename-safe ISO format string in UTC.

        This method takes various timestamp representations (datetime objects, pandas Timestamps, or
        ISO strings) and converts them to a consistent 'YYYY-MM-DDTHH-MM-SSZ' format using hyphens
        instead of colons, making the result safe for use in file and directory names on all operating
        systems (Windows, Linux, macOS).

        Parameters
        ----------
        ts : datetime, pandas.Timestamp, str, or None
            The timestamp to convert. Accepts timezone-aware or naive datetime objects, pandas
            Timestamps, ISO format strings, or None. If None, uses the current UTC time.

        Returns
        -------
        str
            A filename-safe ISO format timestamp string in the form "YYYY-MM-DDTHH-MM-SSZ"
            (e.g., "2024-03-15T14-30-00Z"). Colons are replaced with hyphens to ensure
            cross-platform filename compatibility.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _make_file_basepath() to generate timestamp-prefixed filenames
        # - File naming operations throughout the manager

        from datetime import datetime, timezone
        stamp = manager._iso_stamp(datetime(2024, 3, 15, 14, 30, 0, tzinfo=timezone.utc))
        # Returns: "2024-03-15T14-30-00Z"

        stamp = manager._iso_stamp(None)
        # Returns: current UTC time like "2024-03-20T10-45-32Z"
        """
        if ts is None:
            return dt.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        try:
            # Usa la tua utility per ottenere un datetime UTC aware
            ts = self._as_utc(ts)
        except Exception:
            # Fallback filename-safe se era una stringa bizzarra
            return str(ts).replace(":", "-").replace(" ", "T")
        return ts.strftime("%Y-%m-%dT%H-%M-%SZ")

    def _min_ts_from_df(self, df) -> Optional[str]:
        """Best-effort extraction of the minimum timestamp column from a DataFrame."""
        for col in ("timestamp","TIMESTAMP","datetime","DATETIME","QUOTE_DATETIME"):
            if col in df.columns:
                try:
                    s = df[col]
                    # fast path: if already string ISO
                    if s.dtype == object:
                        # find first non-empty then min
                        vals = [x for x in s.values if isinstance(x, str) and x]
                        if vals:
                            m = min(self._as_utc(v) for v in vals)
                            return self._iso_stamp(m)
                    # general path
                    ts = s.min()
                    return self._iso_stamp(ts)
                except Exception:
                    continue
        return None



    def _ensure_list(self, data: Any, keys=("data", "results", "items", "expirations", "dates", "contracts")) -> List[Any]:
        if data is None:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in keys:
                v = data.get(k)
                if isinstance(v, list):
                    return v
            return [data]
        return [data]

    def _normalize_date_str(self, s: str) -> Optional[str]:
        if not s:
            return None
        s = str(s)
        if len(s) == 8 and s.isdigit():
            return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
        return s

    def _parse_date(self, d):
        """Ritorna date da str 'YYYY-MM-DD' o 'YYYYMMDD', da datetime/date."""

        if isinstance(d, date) and not isinstance(d, datetime):
            return d
        if isinstance(d, datetime):
            return d.date()
        if isinstance(d, str):
            s = d.strip()
            if "-" in s:
                return datetime.strptime(s, "%Y-%m-%d").date()
            return datetime.strptime(s, "%Y%m%d").date()
        raise ValueError(f"Formato data non supportato: {type(d)}={d!r}")
    
    def _iter_days(self, start, end):
        """Genera stringhe 'YYYY-MM-DD' da start a end (inclusivi)."""

        sd = self._parse_date(start)
        ed = self._parse_date(end)
        if sd > ed:
            sd, ed = ed, sd
        cur = sd
        while cur <= ed:
            yield cur.isoformat()
            cur += timedelta(days=1)

    def _tail_csv_last_n_lines(self, path: str, n: int = 64) -> list[str]:
        """
        Read the last N non-empty lines from a CSV file without loading the entire file.

        This method efficiently reads from the end of large CSV files using backward chunked
        reading, which is much faster than loading and tailing the entire file. It's used for
        extracting the most recent timestamps and performing boundary deduplication.

        Parameters
        ----------
        path : str
            Absolute path to the CSV file.
        n : int, optional
            Default: 64
            Maximum number of lines to return.

        Returns
        -------
        list[str]
            List of up to N non-empty lines from the end of the file, ordered from newest
            (most recent) to oldest. Returns empty list if file doesn't exist or is empty.

        Example Usage
        -------------
        # Called by _last_csv_timestamp and boundary deduplication operations
        last_lines = manager._tail_csv_last_n_lines(
            path="data/stock/AAPL/5m/csv/2024-03-15T00-00-00Z-AAPL-stock-5m_part01.csv",
            n=64
        )
        # Returns: ["2024-03-15T20:00:00Z,150.23,150.45,...", ...]

        Notes
        -----
        - Uses 4KB block-based backward reading for efficiency on large files.
        - Strips carriage returns and newlines from each line.
        - Skips empty lines automatically.
        - Does not parse CSV structure; returns raw text lines.
        - Optimal for extracting timestamps without pandas overhead.
        """
        if not os.path.exists(path):
            return []
        out = []
        block = 4096
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell()
            buf = b""
            while pos > 0 and len(out) < n:
                take = block if pos >= block else pos
                f.seek(pos - take)
                chunk = f.read(take)
                pos -= take
                buf = chunk + buf
                while b"\n" in buf and len(out) < n:
                    buf, line = buf.rsplit(b"\n", 1)
                    s = line.decode("utf-8", errors="ignore").rstrip("\r\n")
                    if s:
                        out.append(s)
            if buf and len(out) < n:
                s = buf.decode("utf-8", errors="ignore").rstrip("\r\n")
                if s:
                    out.append(s)
        return out  # newest-first
    
    def _tail_one_line(self, path: str) -> Optional[str]:
        """Efficiently read the last line of a text file (used for CSV).

        Works even if the file does not end with a newline or is a single-line file.
        Uses only b"\\n" as the line separator (CRLF is handled by stripping).
        """
        with open(path, "rb") as f:
            try:
                f.seek(-2, os.SEEK_END)
                while True:
                    b = f.read(1)
                    if b == b"\n":
                        break
                    f.seek(-2, os.SEEK_CUR)
            except OSError:
                f.seek(0)
            last = f.readline().decode("utf-8", errors="ignore").rstrip("\r\n")
            return last or None

    def _parse_csv_first_col_as_dt(self, line: str):
        """Parse the first column of a CSV line as an ISO datetime string and return a UTC-aware datetime.

        This helper method extracts the first comma-separated value from a CSV line, interprets it as
        an ISO 8601 formatted timestamp, and converts it to a timezone-aware datetime object in UTC.
        Returns None if parsing fails, making it safe for handling malformed or non-timestamp data.

        Parameters
        ----------
        line : str
            A single line from a CSV file where the first column contains an ISO 8601 timestamp
            (e.g., "2024-03-15T14:30:00Z,150.23,100,...").

        Returns
        -------
        datetime or None
            A timezone-aware datetime object in UTC timezone representing the parsed timestamp.
            Returns None if the line is empty, malformed, or the first column cannot be parsed as a date.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _tail_csv_last_n_lines() to extract timestamps from CSV tail lines
        # - _compute_intraday_window_et() to determine resume start times
        # - Other methods that need to parse timestamps from CSV data quickly

        line = "2024-03-15T14:30:00Z,150.23,100,..."
        ts = manager._parse_csv_first_col_as_dt(line)
        # Returns: datetime(2024, 3, 15, 14, 30, 0, tzinfo=timezone.utc)
        """
        first = line.split(",", 1)[0].strip()
        try:
            return dt.fromisoformat(first.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None
    
    
    def _extract_days_from_df(self, df, fallback_day_iso: str) -> tuple[str, str]:
        """Return (first_day, last_day) as 'YYYY-MM-DD'. Fallback to provided day if needed."""

        tcol = None
        for c in ("created", "timestamp", "date"):
            if c in df.columns:
                tcol = c
                break
        if tcol in ("created", "timestamp"):
            ts = pd.to_datetime(df[tcol], errors="coerce", utc=True)
            days = ts.dt.date.astype(str).dropna()
        elif tcol == "date":
            days = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str).dropna()
        else:
            days = pd.Series([fallback_day_iso], dtype=str)
        return days.min(), days.max()

    def _detect_time_col(self, cols):
        """Best-effort time column detection for intraday options bars."""
        for c in ("datetime","timestamp","ts","QUOTE_DATETIME","TRADE_DATETIME","QUOTE_UNIXTIME","TRADE_UNIXTIME"):
            if c in cols: return c
        return None
    
    async def _maybe_await(self, x):
        """Conditionally await a value if it's awaitable, otherwise return it directly.

        This utility method provides safe handling of values that may or may not be coroutines or
        async functions. If the value is awaitable (like an async function result), it awaits it.
        Otherwise, it returns the value immediately. This enables writing code that can handle both
        sync and async operations uniformly.

        Parameters
        ----------
        x : Any
            The value to potentially await. Can be any Python object, including coroutines, futures,
            or regular values.

        Returns
        -------
        Any
            If x is awaitable (inspect.isawaitable returns True), returns the result of awaiting x.
            Otherwise, returns x unchanged.

        Example Usage
        -------------
        # This is an internal helper method called by methods that may receive either
        # synchronous values or async coroutines as parameters

        # With async value:
        result = await manager._maybe_await(some_async_function())
        # Awaits and returns the result

        # With sync value:
        result = await manager._maybe_await(42)
        # Returns: 42 immediately
        """
        return await x if inspect.isawaitable(x) else x




    # =========================================================================
    # (END)
    # COMMON UTILITIES
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # DATA COHERENCE & POST-HOC VALIDATION
    # =========================================================================

    async def check_and_recover_coherence(
        self,
        symbol: str,
        asset: str,
        interval: str,
        sink: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        auto_recover: bool = True,
        enrich_greeks: bool = False
    ):
        """Check data coherence and optionally recover missing data.

        This method performs post-hoc validation to identify missing data,
        gaps in coverage, and inconsistencies. It can be called after initial
        downloads to verify completeness, or periodically to check for data integrity.

        Parameters
        ----------
        symbol : str
            Symbol to check (e.g., "AAPL", "SPY").
        asset : str
            Asset type ("stock", "option", "index").
        interval : str
            Time interval ("1d", "5m", "1h", "tick", etc.).
        sink : str
            Storage sink ("csv", "parquet", "influx").
        start_date : Optional[str], optional
            Start date for check (YYYY-MM-DD). If None, uses earliest available locally.
        end_date : Optional[str], optional
            End date for check (YYYY-MM-DD). If None, uses latest available locally.
        auto_recover : bool, optional
            If True, automatically attempt to recover missing data (default True).
        enrich_greeks : bool, optional
            For options, whether to enrich with Greeks during recovery (default False).

        Returns
        -------
        CoherenceReport
            Report containing all issues found and recovery status.

        Example Usage
        -------------
        # Check EOD data for AAPL
        report = await manager.check_and_recover_coherence(
            symbol="AAPL",
            asset="stock",
            interval="1d",
            sink="parquet",
            start_date="2024-01-01",
            end_date="2024-12-31",
            auto_recover=True
        )

        if report.is_coherent:
            print("Data is complete and coherent!")
        else:
            print(f"Found {len(report.issues)} issues:")
            for issue in report.issues:
                print(f"  - {issue.description}")
        """
        from .coherence import CoherenceChecker, IncoherenceRecovery

        # Initialize checker
        checker = CoherenceChecker(self)

        # Log coherence check start with ALL parameters
        self.logger.log_info(
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(start_date or "", end_date or ""),
            message=f"COHERENCE_CHECK_START: checking data completeness for {symbol} {asset}/{interval}",
            details={
                "sink": sink,
                "start_date": start_date,
                "end_date": end_date,
                "validation_enabled": self.cfg.enable_data_validation,
                "validation_strict_mode": self.cfg.validation_strict_mode,
                "tick_eod_volume_tolerance": self.cfg.tick_eod_volume_tolerance,
                "intraday_bucket_tolerance": self.cfg.intraday_bucket_tolerance,
                "enable_bucket_analysis": getattr(self.cfg, 'enable_bucket_analysis', True)
            }
        )

        # Perform coherence check
        report = await checker.check(
            symbol=symbol,
            asset=asset,
            interval=interval,
            sink=sink,
            start_date=start_date,
            end_date=end_date
        )

        # Log coherence check end
        self.logger.log_info(
            symbol=symbol,
            asset=asset,
            interval=interval,
            date_range=(start_date or "", end_date or ""),
            message=f"COHERENCE_CHECK_END: is_coherent={report.is_coherent}, issues_found={len(report.issues)}",
            details={
                "is_coherent": report.is_coherent,
                "issues_count": len(report.issues),
                "missing_days": len(report.missing_days),
                "intraday_gaps": len(report.intraday_gaps),
                "tick_volume_mismatches": len(report.tick_volume_mismatches)
            }
        )

        # Auto-recover if enabled and issues found
        if not report.is_coherent and auto_recover:
            recovery = IncoherenceRecovery(self)

            # Log recovery start
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(start_date or "", end_date or ""),
                message=f"RECOVERY_START: attempting to recover {len(report.issues)} issues",
                details={
                    "issues_count": len(report.issues),
                    "missing_days": len(report.missing_days),
                    "intraday_gaps": len(report.intraday_gaps),
                    "tick_volume_mismatches": len(report.tick_volume_mismatches),
                    "enrich_greeks": enrich_greeks
                }
            )

            recovery_result = await recovery.recover(
                report=report,
                enrich_greeks=enrich_greeks
            )

            # Log recovery end
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(start_date or "", end_date or ""),
                message=f"RECOVERY_END: successful={recovery_result.successful}, failed={recovery_result.failed}",
                details={
                    "total_issues": recovery_result.total_issues,
                    "successful": recovery_result.successful,
                    "failed": recovery_result.failed,
                    "recovery_details": recovery_result.details
                }
            )

            # Log recovery summary
            if recovery_result.total_issues > 0:
                success_rate = recovery_result.successful / recovery_result.total_issues
                message = (
                    f"Recovery completed: {recovery_result.successful}/{recovery_result.total_issues} "
                    f"issues resolved ({success_rate:.1%})"
                )

                if recovery_result.failed > 0:
                    self.logger.log_failure(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=(start_date or "", end_date or ""),
                        message=message,
                        details={'recovery_details': recovery_result.details}
                    )
                else:
                    self.logger.log_resolution(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=(start_date or "", end_date or ""),
                        message=message,
                        details={'recovery_details': recovery_result.details}
                    )

            # Re-check after recovery
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(start_date or "", end_date or ""),
                message="COHERENCE_CHECK_START: re-checking after recovery",
                details={"check_type": "post_recovery"}
            )

            report = await checker.check(
                symbol=symbol,
                asset=asset,
                interval=interval,
                sink=sink,
                start_date=start_date,
                end_date=end_date
            )

            # Log post-recovery check result
            self.logger.log_info(
                symbol=symbol,
                asset=asset,
                interval=interval,
                date_range=(start_date or "", end_date or ""),
                message=f"COHERENCE_CHECK_END: post-recovery check, is_coherent={report.is_coherent}, remaining_issues={len(report.issues)}",
                details={
                    "is_coherent": report.is_coherent,
                    "remaining_issues": len(report.issues),
                    "check_type": "post_recovery"
                }
            )

        return report

    # =========================================================================
    # (END)
    # DATA COHERENCE & POST-HOC VALIDATION
    # =========================================================================
