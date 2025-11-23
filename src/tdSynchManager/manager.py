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

from .client import Interval, ThetaDataV3Client, ResilientThetaClient
from .config import DiscoverPolicy, ManagerConfig, Task, config_from_env
from .logger import DataConsistencyLogger
from .validator import DataValidator, ValidationResult
from .retry import retry_with_policy
from .download_retry import download_with_retry_and_validation
from .influx_verification import verify_influx_write, write_influx_with_verification, InfluxWriteResult

try:  # optional at runtime
    ipy_display = importlib.import_module("IPython.display").display
except Exception:  # pragma: no cover
    ipy_display = None

try:  # optional dependency, only needed for InfluxDB sinks
    InfluxDBClient3 = importlib.import_module("influxdb_client_3").InfluxDBClient3
except Exception:  # pragma: no cover
    InfluxDBClient3 = None

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
    from tdSynchManager.client import ThetaDataV3Client
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
        from tdSynchManager.client import ThetaDataV3Client

        cfg = ManagerConfig(root_dir="./data", max_concurrency=5)
        async with ThetaDataV3Client(base_url="http://localhost:25503/v3") as client:
            manager = ThetaSyncManager(cfg, client)
        """
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
            # Intraday OHLC: validate candle completeness
            validation_result = DataValidator.validate_intraday_completeness(
                df=df,
                interval=interval,
                date_iso=day_iso,
                asset=asset
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
            return True
        else:
            # Non-strict mode: warnings logged but allow save
            return True

    async def _get_eod_volume_for_validation(
        self,
        symbol: str,
        asset: str,
        date_iso: str,
        sink: str
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Get EOD volumes for tick validation.

        First checks local storage (DB), then downloads from API if not found.
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
            (total_volume, call_volume, put_volume). For non-options, call and put are None.
        """
        import io

        # First, try to read EOD from local storage
        try:
            files = self._list_series_files(asset, symbol, "1d", sink.lower())
            eod_file = None
            for f in files:
                if date_iso in f:
                    eod_file = f
                    break

            if eod_file:
                if sink.lower() == "csv":
                    eod_df = pd.read_csv(eod_file)
                elif sink.lower() == "parquet":
                    eod_df = pd.read_parquet(eod_file)
                else:
                    eod_df = None

                if eod_df is not None and not eod_df.empty and 'volume' in eod_df.columns:
                    total_volume = eod_df['volume'].sum()

                    # For options, try to separate by call/put
                    if asset == "option" and 'right' in eod_df.columns:
                        call_volume = eod_df[eod_df['right'].isin(['C', 'call'])]['volume'].sum()
                        put_volume = eod_df[eod_df['right'].isin(['P', 'put'])]['volume'].sum()
                        return float(total_volume), float(call_volume), float(put_volume)

                    return float(total_volume), None, None
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

            eod_df = pd.read_csv(io.StringIO(csv_txt))

            if 'volume' in eod_df.columns:
                total_volume = eod_df['volume'].sum()

                if asset == "option" and 'right' in eod_df.columns:
                    call_volume = eod_df[eod_df['right'].isin(['C', 'call'])]['volume'].sum()
                    put_volume = eod_df[eod_df['right'].isin(['P', 'put'])]['volume'].sum()
                    return float(total_volume), float(call_volume), float(put_volume)

                return float(total_volume), None, None

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
        from tdSynchManager.client import ThetaDataV3Client

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

        # --- helper: calcola l'HMS di partenza allineato al bucket ---
        def _force_start_hms_from_max_ts(_max_ts_utc, interval, overlap_seconds=60, bars_back=None):
            """
            - tick/secondi: usa solo overlap_seconds.
            - minuti/ore  : floor in ET al bucket e arretra di N barre.
            Ritorna 'HH:MM:SS' in ET.
            """
            iv = (interval or "").strip().lower()
        
            if iv == "tick" or iv.endswith("s"):
                et = _max_ts_utc.tz_convert("America/New_York")
                et2 = (et - pd.Timedelta(seconds=int(overlap_seconds or 60))).replace(microsecond=0)
                return et2.strftime("%H:%M:%S")
        
            m = 1
            if iv.endswith("m") and iv[:-1].isdigit():
                m = max(1, int(iv[:-1]))
            elif iv.endswith("h") and iv[:-1].isdigit():
                m = max(1, int(iv[:-1]) * 60)
        
            n = int(bars_back if bars_back is not None else getattr(self.cfg, "resume_bars_back", 1) or 1)
            n = max(1, n)
        
            floored_utc = self._floor_to_interval_et(_max_ts_utc.to_pydatetime(), m)  # <-- usa il tuo helper
            start_et = floored_utc.astimezone(self.ET) - pd.Timedelta(minutes=m * n)
            return start_et.strftime("%H:%M:%S")

    
        first_date_iso = _norm_ymd(first_date)
    
        # Discover earliest and latest days across ALL series files (handles rotations / multiple files)
        series_first_iso, series_last_iso, series_files = self._series_earliest_and_latest_day(
            task.asset, symbol, interval, sink_lower
        )

        # ---------------------------
        # 0) Pre-history: fill only missing head, stop at first existing file (+ head overlap)
        # ---------------------------
        if interval == "1d" and task.asset in ("stock", "index") and first_date_iso:
            # elenco file e earliest/latest calcolati su TUTTI i file della serie
            series_first_iso, series_last_iso, series_files = self._series_earliest_and_latest_day(
                task.asset, symbol, interval, sink_lower
            )
        
            if series_first_iso and first_date_iso < series_first_iso:
        
                req_start = dt.fromisoformat(first_date_iso).date()
                first_file_day = dt.fromisoformat(series_first_iso).date()
        
                # vogliamo riempire fino al giorno precedente al primo file...
                natural_end = first_file_day - timedelta(days=1)
                # ... ma consentiamo un piccolo overlap che includa AL MASSIMO il giorno del primo file
                head_ov = max(0, int(getattr(self.cfg, "eod_head_overlap_days", 1)))
                capped_end = min(first_file_day, natural_end + timedelta(days=head_ov))
        
                # niente da fare se l'intervallo si svuota
                if req_start <= capped_end:
                    batch_days = max(1, int(getattr(self.cfg, "eod_batch_days", 30)))
                    cur = req_start
                    while cur <= capped_end:
                        chunk_end = min(capped_end, cur + timedelta(days=batch_days - 1))
                        start_iso = cur.isoformat()
                        end_iso   = chunk_end.isoformat()
                        print(f"[EOD-PREHIST] {task.asset} {symbol} {start_iso}..{end_iso}")
                        try:
                            await self._download_and_store_equity_or_index(
                                asset=task.asset,
                                symbol=symbol,
                                interval=interval,
                                day_iso=start_iso,
                                sink=task.sink,
                                range_end_iso=end_iso,   # inclusivo
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            print(f"[WARN] prehistory {task.asset} {symbol} {interval} {start_iso}..{end_iso}: {e}")
                        cur = chunk_end + timedelta(days=1)
        
                # ricalcola earliest/latest dopo la pre-history
                series_first_iso, series_last_iso, series_files = self._series_earliest_and_latest_day(
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
        # 1) Backfill unexpected gaps (bounded strictly to pre-resume)
        # ---------------------------
        if interval == "1d" and task.asset in ("stock", "index"):
            gap_end_iso = (resume_start_day - timedelta(days=1)).isoformat()
    
            # choose a start for gap-scan: if we have files, start at earliest file day; else at first_date
            gap_start_iso = None
            if series_first_iso:
                gap_start_iso = series_first_iso
            elif first_date_iso:
                gap_start_iso = first_date_iso
    
            missing_days = []
            if gap_start_iso and gap_start_iso <= gap_end_iso:
                try:
                    missing_days = self._missing_1d_days_csv(
                        task.asset,
                        symbol,
                        interval,
                        sink_lower,
                        gap_start_iso,
                        gap_end_iso,
                    )
                except Exception as e:
                    print(f"[WARN] gap-scan {task.asset} {symbol} {interval}: {e}")
                    missing_days = []
    
            for day_iso in sorted(set(missing_days)):
                try:
                    await self._download_and_store_equity_or_index(
                        asset=task.asset,
                        symbol=symbol,
                        interval=interval,
                        day_iso=day_iso,
                        sink=task.sink,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[WARN] backfill {task.asset} {symbol} {interval} {day_iso}: {e}")
    
        # ---------------------------
        # 2) Main loop
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
    
            # Se skip mode e c'è retrograde, gestisci prima il retrograde
            if _strong_skip_policy and _first_day_db and start_date_et < dt.fromisoformat(_first_day_db).date():
                # Scarica solo fino al giorno prima del primo presente
                retro_end = dt.fromisoformat(_first_day_db).date() - timedelta(days=1)
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
                
                # Dopo il retrograde, salta all'ultimo giorno
                if _last_day_db:
                    cur_date = dt.fromisoformat(_last_day_db).date()
                    print(f"[SKIP-MODE][EOD] jump to last_day: {cur_date}")
            
            # Continua dal cur_date (che potrebbe essere stato aggiornato)
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
    
        # Options or intraday: day-by-day
        while cur_date <= end_date:
            day_iso = cur_date.isoformat()

            # >>> WEEKEND SKIP 
            if dt.fromisoformat(day_iso).weekday() >= 5:
                cur_date += timedelta(days=1)
                continue

            # >>> SALTO POST-RETRO (UNIVERSALE) 
            if _edge_jump_after_retro and _first_day_db and day_iso >= _first_day_db:
                if _last_day_db and day_iso < _last_day_db:
                    # Per 1d salta a last_db+1 (giorno completo), per intraday a last_db (può mancare)
                    if interval == "1d":
                        jump_target = dt.fromisoformat(_last_day_db).date() + timedelta(days=1)
                        print(f"[SKIP-MODE][JUMP] {day_iso} → {_last_day_db}+1 (daily complete)")
                    else:
                        jump_target = dt.fromisoformat(_last_day_db).date()
                        print(f"[SKIP-MODE][JUMP] {day_iso} → {_last_day_db} (intraday may be incomplete)")
                    cur_date = jump_target
                    _edge_jump_after_retro = False
                    continue

            # >>> CHECK COMPLETEZZA PRIMO GIORNO (UNIVERSALE) <
            if _first_day_db and day_iso == _first_day_db:
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
                    cur_date += timedelta(days=1)
                    continue

            # >>> EARLY-SKIP MIDDLE-DAY (UNIVERSALE) <
            if self._skip_existing_middle_day(
                asset=task.asset, symbol=symbol, interval=interval, sink=task.sink, day_iso=day_iso,
                first_last_hint=(_first_day_db, _last_day_db)
            ):
                print(f"[SKIP-MODE] skip middle day={day_iso}")
                cur_date += timedelta(days=1)
                continue


            # >>> MILD-SKIP: controlla presenza dati per ogni giorno intermedio
            if _mild_skip_policy and _first_day_db and _last_day_db:
                if _first_day_db < day_iso < _last_day_db:
                    # QUI SI che controlliamo se c'è già dato
                    has_data = False
                    
                    if sink_lower == "influxdb":
                        prefix = (self.cfg.influx_measure_prefix or "")
                        meas = f"{prefix}{symbol}-{task.asset if task.asset != 'option' else 'option'}-{interval}"
                        try:
                            has_data = self._influx_day_has_any(meas, day_iso)
                        except Exception:
                            has_data = False
                    
                    elif sink_lower in ("csv", "parquet"):
                        day_files = self._list_day_files(task.asset, symbol, interval, sink_lower, day_iso)
                        has_data = len(day_files) > 0
                    
                    if has_data:
                        print(f"[MILD-SKIP] skip middle-day with existing data: {day_iso}")
                        cur_date += timedelta(days=1)
                        continue
                        
            # calcola sempre sink_lower nel loop (può non essere definito qui)
            sink_lower = (task.sink or "").lower()

            # >>> MILD-SKIP: salta last_day se interval=1d (giorno completo)
            if _mild_skip_policy and _last_day_db and day_iso == _last_day_db:
                if interval == "1d":
                    print(f"[MILD-SKIP] skip last_day (1d complete): {day_iso}")
                    cur_date += timedelta(days=1)
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

            cur_date += timedelta(days=1)

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
            Output format: "csv" or "parquet".
        enrich_greeks : bool
            If True, fetch and merge the **full** Greeks set in addition to IV:
            - EOD: /option/history/greeks/eod over all expirations.
            - Intraday: /option/history/greeks/all aligned to the requested interval.

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
          3) Implied Volatility: keep a single 'implied_volatility' from '/option/history/greeks/eod' (no bid/mid/ask, no interval IV).
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
        • Errors during enrichment are caught and logged as warnings; the base OHLC file is still produced
          when possible.
        """


    
        sink_lower = sink.lower()
        stop_before_part = None

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
                df = pd.read_csv(io.StringIO(csv_txt))
                if df is None or df.empty:
                    raise ValueError("Empty DataFrame from option_history_eod")

                # 2) (optional) EOD Greeks for ALL expirations and merge
                if enrich_greeks:
                    csv_g, _ = await self.client.option_history_greeks_eod(
                        symbol=symbol,
                        expiration="*",
                        start_date=day_iso,
                        end_date=day_iso,
                        rate_type="sofr",
                        format_type="csv",
                    )
                    if csv_g:
                        dg = pd.read_csv(io.StringIO(csv_g))
                        if dg is not None and not dg.empty:
                            # Align strike columns
                            if "strike_price" in dg.columns and "strike" not in dg.columns:
                                dg = dg.rename(columns={"strike_price": "strike"})
                            if "strike_price" in df.columns and "strike" not in df.columns:
                                df = df.rename(columns={"strike_price": "strike"})

                            # Find join keys
                            candidate_keys = [
                                ["option_symbol"],
                                ["root", "expiration", "strike", "right"],
                                ["symbol", "expiration", "strike", "right"],
                            ]
                            on_cols = None
                            for keys in candidate_keys:
                                if all(k in df.columns for k in keys) and all(k in dg.columns for k in keys):
                                    on_cols = keys
                                    break
                            if on_cols is not None:
                                dup_cols = [c for c in dg.columns if c in df.columns and c not in on_cols]
                                dg = dg.drop(columns=dup_cols).drop_duplicates(subset=on_cols)
                                df = df.merge(dg, on=on_cols, how="left")

                # 2b) last_day_OI: Previous day OI
                try:
                    d = dt.fromisoformat(day_iso)
                    prev = d - timedelta(days=1)
                    # Weekend normalization
                    if prev.weekday() == 5:  # Sat -> Fri
                        prev = prev - timedelta(days=1)
                    elif prev.weekday() == 6:  # Sun -> Fri
                        prev = prev - timedelta(days=2)

                    cur_ymd = dt.fromisoformat(day_iso).strftime("%Y%m%d")
                    csv_oi, _ = await self.client.option_history_open_interest(
                        symbol=symbol,
                        expiration="*",
                        date=cur_ymd,
                        strike="*",
                        right="both",
                        format_type="csv",
                    )
                    if csv_oi:
                        doi = pd.read_csv(io.StringIO(csv_oi))
                        if doi is not None and not doi.empty:
                            oi_col = next((c for c in ["open_interest", "oi", "OI"] if c in doi.columns), None)
                            if oi_col:
                                doi = doi.rename(columns={oi_col: "last_day_OI"})

                                ts_col = next((c for c in ["timestamp", "ts", "time"] if c in doi.columns), None)
                                if ts_col:
                                    doi = doi.rename(columns={ts_col: "timestamp_oi"})
                                if "timestamp_oi" in doi.columns:
                                    _eff = pd.to_datetime(doi["timestamp_oi"], errors="coerce")
                                    doi["effective_date_oi"] = _eff.dt.tz_localize("UTC").dt.tz_convert("America/New_York").dt.date.astype(str)
                                else:
                                    doi["effective_date_oi"] = prev.date().isoformat()

                                candidate_keys = [
                                    ["option_symbol"],
                                    ["root", "expiration", "strike", "right"],
                                    ["symbol", "expiration", "strike", "right"],
                                ]
                                on_cols = None
                                for keys in candidate_keys:
                                    if all(k in df.columns for k in keys) and all(k in doi.columns for k in keys):
                                        on_cols = keys
                                        break
                                if on_cols is not None:
                                    keep = on_cols + ["last_day_OI"]
                                    if "timestamp_oi" in doi.columns:
                                        keep += ["timestamp_oi"]
                                    keep += ["effective_date_oi"]
                                    doi = doi[keep].drop_duplicates(subset=on_cols)
                                    df = df.merge(doi, on=on_cols, how="left")
                except Exception as e:
                    print(f"[WARN] EOD OI merge {symbol} {day_iso}: {e}")

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


            # 3) persist + written rows counting
            st = self._day_parts_status("option", symbol, interval, sink_lower, day_iso)
            base_path = st.get("base_path") or self._make_file_basepath("option", symbol, interval, f"{day_iso}T00-00-00Z", sink_lower)

            wrote = 0
            if sink_lower == "csv":
                await self._append_csv_text(base_path, df.to_csv(index=False))
                wrote = len(df)
            elif sink_lower == "parquet":
                wrote = self._append_parquet_df(base_path, df)
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

                wrote, write_success = await write_influx_with_verification(
                    write_func=lambda df_write: self._append_influx_df(base_path, df_write),
                    verify_func=lambda df_verify: verify_influx_write(
                        influx_client=influx_client,
                        measurement=measurement,
                        df_original=df_verify,
                        key_cols=key_cols,
                        time_col='__ts_utc'
                    ),
                    df=df,
                    retry_policy=self.cfg.retry_policy,
                    logger=self.logger,
                    context={
                        'symbol': symbol,
                        'asset': 'option',
                        'interval': interval,
                        'date_range': (day_iso, day_iso),
                        'sink': sink,
                        'measurement': measurement
                    }
                )

                if not write_success:
                    print(f"[ALERT] InfluxDB write failed after retries for option {symbol} EOD {day_iso}")
                if wrote == 0 and len(df) > 0:
                    print("[ALERT] Influx ha scritto 0 punti a fronte di righe in input. "
                          "Potrebbe essere tutto NaN lato fields o un cutoff troppo aggressivo.")
            else:
                raise ValueError(f"Unsupported sink: {sink_lower}")
            
            print(f"[SUMMARY] option {symbol} 1d day={day_iso} rows={len(df)} wrote={wrote} sink={sink_lower}")


        
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
            return
    
        # 2) fetch TICK **oppure** OHLC+enrichment per expiration e concatena
        dfs = []
        greeks_trade_list: List[pd.DataFrame] = []
        iv_trade_list: List[pd.DataFrame] = []
        greeks_bar_list: List[pd.DataFrame] = []
        iv_bar_list: List[pd.DataFrame] = []

        if interval == "tick":
            # TICK: usa trade+quote pairing per ogni expiration (v3)
            # /option/history/trade_quote   date=YYYYMMDD, strike="*", right="both"
            
            # --- ACCUMULATORS (tick): collect greeks/IV per expiration; merge once after concat ---
            greeks_trade_list: List[pd.DataFrame] = []
            iv_trade_list: List[pd.DataFrame] = []
            # --- /ACCUMULATORS ---
            
            for exp in expirations:
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

                    csv_tq, _ = await self.client.option_history_trade_quote(**tq_kwargs)
                    if not csv_tq:
                        continue                 
                        
                    df = pd.read_csv(io.StringIO(csv_tq))
                    if df is None or df.empty:
                        continue

                    # ### >>> TICK — CANONICAL TIMESTAMP FROM trade_timestamp — BEGIN
                    # Create the canonical 'timestamp' from 'trade_timestamp' for tick data.
                    if "timestamp" not in df.columns:
                        if "trade_timestamp" not in df.columns:
                            raise RuntimeError("Expected 'trade_timestamp' in tick TQ response.")
                        df["timestamp"] = pd.to_datetime(df["trade_timestamp"], errors="coerce")
                        # Ensure ET-naive timestamps (manager expects ET-naive everywhere).
                        if getattr(df["timestamp"].dtype, "tz", None) is not None:
                            df["timestamp"] = (
                                df["timestamp"]
                                .dt.tz_convert(ZoneInfo("America/New_York"))
                                .dt.tz_localize(None)
                            )
                    # ### <<< TICK — CANONICAL TIMESTAMP FROM trade_timestamp — END


                    # A) Per-trade Greeks (ALL) — opt-in, minimal diff
                    if enrich_tick_greeks:
                        try:
                            csv_tg, _ = await self.client.option_history_all_trade_greeks(
                                symbol=symbol,
                                expiration=exp,
                                date=ymd,
                                strike="*",
                                right="both",
                                format_type="csv",
                            )
                            if csv_tg:
                                dg = pd.read_csv(io.StringIO(csv_tg))
                                if dg is not None and not dg.empty:

                                    # --- normalizza nomi per join robusta ---
                                    # unifica strike
                                    if "strike_price" in dg.columns and "strike" not in dg.columns:
                                        dg = dg.rename(columns={"strike_price": "strike"})
                                    if "strike_price" in df.columns and "strike" not in df.columns:
                                        df = df.rename(columns={"strike_price": "strike"})
                                    
                                    # usa trade_timestamp per join a livello trade
                                    if ("trade_timestamp" in df.columns
                                        and "trade_timestamp" not in dg.columns
                                        and "timestamp" in dg.columns):
                                        dg = dg.rename(columns={"timestamp": "trade_timestamp"})
                                    # --- /normalizza ---

                                    # collect once: postpone the join to after the main concat
                                    greeks_trade_list.append(dg)

                        except Exception as e:
                            print(f"[WARN] tick Greeks merge {symbol} {day_iso} exp={exp}: {e}")

                    # B) Per-trade IV — opt-in
                    if enrich_tick_greeks:
                        try:
                            csv_tiv, _ = await self.client.option_history_trade_implied_volatility(
                                symbol=symbol,
                                expiration=exp,
                                date=ymd,
                                strike="*",
                                right="both",
                                format_type="csv",
                            )
                            if csv_tiv:
                                divt = pd.read_csv(io.StringIO(csv_tiv))
                                if divt is not None and not divt.empty:
                                    # prefer a clear name to avoid confusion with bar-level IV
                                    if "implied_volatility" in divt.columns and "trade_iv" not in divt.columns:
                                        divt = divt.rename(columns={"implied_volatility": "trade_iv"})

                                    # --- normalize columns for robust join (Trade IV on tick) ---
                                    # Unify strike naming
                                    if "strike_price" in divt.columns and "strike" not in divt.columns:
                                        divt = divt.rename(columns={"strike_price": "strike"})
                                    if "strike_price" in df.columns and "strike" not in df.columns:
                                        df = df.rename(columns={"strike_price": "strike"})

                                    # Use trade-level timestamp for tick joins
                                    if ("trade_timestamp" in df.columns
                                        and "trade_timestamp" not in divt.columns
                                        and "timestamp" in divt.columns):
                                        divt = divt.rename(columns={"timestamp": "trade_timestamp"})
                                    # --- /normalize ---
                                    
                                    # collect once: postpone the join to after the main concat
                                    iv_trade_list.append(divt)

                        except Exception as e:
                            print(f"[WARN] tick IV merge {symbol} {day_iso} exp={exp}: {e}")
                        
                    dfs.append(df)
                except Exception as e:
                    # Solo logga se NON è un 472 (no data)
                    if "472" not in str(e) and "No data found" not in str(e):
                        print(f"[WARN] option tick {symbol} {interval} {day_iso} exp={exp}: {e}")
                    # continue to next expiration
        else:
            
            for exp in expirations:
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
                        continue
                    df = pd.read_csv(io.StringIO(csv_ohlc))
                    if df is None or df.empty:
                        continue

                    if enrich_greeks:
                        kwargs = {
                            "symbol": symbol,
                            "expiration": exp,
                            "date": ymd,
                            "interval": interval,
                            "strike": "*",
                            "right": "both",
                            "rate_type": "sofr",
                            "format_type": "csv",
                        }
                        if bar_start_et is not None:
                            kwargs["start_time"] = bar_start_et
                        
                        csv_gr_all, _ = await self._td_get_with_retry(
                            lambda: self.client.option_history_all_greeks(**kwargs),
                            label=f"greeks/all {symbol} {exp} {ymd} {interval}"
                        )

                        if csv_gr_all:
                            dg = pd.read_csv(io.StringIO(csv_gr_all))
                            if dg is not None and not dg.empty:
                                # collect once: postpone the join to after the main concat
                                greeks_bar_list.append(dg)

                    # IV bars (bid/mid/ask) — collect once, merge later
                    try:
                        # Build kwargs to avoid passing None to start_time (causes total_seconds error in SDK)
                        iv_kwargs = {
                            "symbol": symbol,
                            "expiration": exp,
                            "date": ymd,
                            "interval": interval,
                            "rate_type": "sofr",
                            "format_type": "csv"
                        }
                        if bar_start_et is not None:
                            iv_kwargs["start_time"] = bar_start_et

                        csv_iv, _ = await self._td_get_with_retry(
                                        lambda: self.client.option_history_implied_volatility(**iv_kwargs),
                                        label=f"greeks/iv {symbol} {exp} {ymd} {interval}"
                                    )
                        if csv_iv:
                            div = pd.read_csv(io.StringIO(csv_iv))
                            if div is not None and not div.empty:
                                # collect once: postpone the join to after the main concat
                                iv_bar_list.append(div)
                    except Exception as e:
                        print(f"[WARN] IV bars collect {symbol} {interval} {day_iso} exp={exp}: {e}")

                    dfs.append(df)

                except Exception as e:
                    print(f"[WARN] option intraday {symbol} {interval} {day_iso} exp={exp}: {e}")

        if not dfs:
            return

        df_all = pd.concat(dfs, ignore_index=True)

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
        
            # 2) IV bar (endpoint implied_volatility)
            if iv_bar_list:
                div_all = pd.concat(iv_bar_list, ignore_index=True)
                if "implied_volatility" in div_all.columns and "bar_iv" not in div_all.columns:
                    div_all = div_all.rename(columns={"implied_volatility": "bar_iv"})
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
            if "implied_volatility" in divt_all.columns and "trade_iv" not in divt_all.columns:
                divt_all = divt_all.rename(columns={"implied_volatility": "trade_iv"})
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
                
        # last_day_OI (stesso valore su tutte le righe del contratto)
        try:
            d = dt.fromisoformat(day_iso)
            prev = d - timedelta(days=1)
            if prev.weekday() == 5:
                prev = prev - timedelta(days=1)
            elif prev.weekday() == 6:
                prev = prev - timedelta(days=2)
                
            cur_ymd = dt.fromisoformat(day_iso).strftime("%Y%m%d")
            
            # >>> Current-Day Fix: Per-Expiration Fallback <
            today_utc = dt.now(timezone.utc).date().isoformat()
            is_current_day = (day_iso == today_utc)
            
            doi = None
            if is_current_day and expirations:
                # Current day: must use per-expiration (expiration="*" fails with 400)
                print(f"[OI-FETCH] current-day mode: per-expiration ({len(expirations)} exps)")
                oi_dfs = []
                for exp in expirations:
                    try:
                        csv_oi_exp, _ = await self.client.option_history_open_interest(
                            symbol=symbol,
                            expiration=exp,
                            date=cur_ymd,
                            strike="*",
                            right="both",
                            format_type="csv",
                        )
                        if csv_oi_exp:
                            oi_dfs.append(pd.read_csv(io.StringIO(csv_oi_exp)))
                    except Exception as e:
                        print(f"[WARN] OI exp={exp} day={day_iso}: {e}")
                
                if oi_dfs:
                    doi = pd.concat(oi_dfs, ignore_index=True)
            else:
                # Historical: single call works
                csv_oi, _ = await self.client.option_history_open_interest(
                    symbol=symbol,
                    expiration="*",
                    date=cur_ymd,
                    strike="*",
                    right="both",
                    format_type="csv",
                )
                if csv_oi:
                    doi = pd.read_csv(io.StringIO(csv_oi))
            
            if doi is not None and not doi.empty:
                oi_col = next((c for c in ["open_interest", "oi", "OI"] if c in doi.columns), None)
                if oi_col:
                    doi = doi.rename(columns={oi_col: "last_day_OI"})
                    
                    # OI timestamp + semantic date
                    ts_col = next((c for c in ["timestamp", "ts", "time"] if c in doi.columns), None)
                    if ts_col:
                        doi = doi.rename(columns={ts_col: "timestamp_oi"})
                    if "timestamp_oi" in doi.columns:
                        _eff = pd.to_datetime(doi["timestamp_oi"], errors="coerce")
                        doi["effective_date_oi"] = _eff.dt.tz_localize("UTC").dt.tz_convert("America/New_York").dt.date.astype(str)
                    else:
                        doi["effective_date_oi"] = prev.date().isoformat()
                    
                    # Merge
                    candidate_keys = [
                        ["option_symbol"],
                        ["root", "expiration", "strike", "right"],
                        ["symbol", "expiration", "strike", "right"],
                    ]
                    on_cols = next((keys for keys in candidate_keys if all(k in df_all.columns for k in keys) and all(k in doi.columns for k in keys)), None)
                    if on_cols:
                        keep = on_cols + ["last_day_OI"]
                        if "timestamp_oi" in doi.columns:
                            keep += ["timestamp_oi"]
                        keep += ["effective_date_oi"]
                        doi = doi[keep].drop_duplicates(subset=on_cols)
                        df_all = df_all.merge(doi, on=on_cols, how="left")
                        
        except Exception as e:
            print(f"[WARN] intraday OI merge {symbol} {interval} {day_iso}: {e}")

    
        # --- GLOBAL ORDER (stabile per part temporali) ---
        time_candidates = ["trade_timestamp","timestamp","bar_timestamp","datetime","created","last_trade"]
        tcol = next((c for c in time_candidates if c in df_all.columns), None)
        if tcol:
            # Parse as ET-naive; if any tz-aware values exist, convert to ET and drop tz.
            tmp = pd.to_datetime(df_all[tcol], errors="coerce")
            if getattr(tmp.dtype, "tz", None) is not None:
                tmp = tmp.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
            df_all[tcol] = tmp
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
        series_first_iso, series_last_iso, _ = self._series_earliest_and_latest_day("option", symbol, interval, sink_lower)
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
                    head = pd.read_csv(path, nrows=0)
                    keep = [c for c in use if c in head.columns]
                    df = pd.read_csv(path, usecols=keep) if keep else pd.read_csv(path)
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
                    ts = pd.to_datetime(d[tcol], errors="coerce")
                    if getattr(ts.dtype, "tz", None) is not None:
                        ts = ts.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
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
            # Normalizza left side (df_all) a ET-naive
            ts_all = pd.to_datetime(df_all[tcol_df], errors="coerce")
            if getattr(ts_all.dtype, "tz", None) is not None:
                ts_all = ts_all.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
            
            cutoff_naive = pd.to_datetime(cutoff).tz_localize(None) if hasattr(cutoff, 'tz') else cutoff
            
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
                            ts_chunk = ts_chunk.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
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
        
        ts = pd.to_datetime(df_all["timestamp"], errors="coerce")   # NO utc=True
        if getattr(ts.dtype, "tz", None) is not None:
            ts = ts.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
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
            await self._append_csv_text(base_path, df_all.to_csv(index=False))
            wrote = len(df_all)
        elif sink_lower == "parquet":
            wrote = self._append_parquet_df(base_path, df_all)
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

            wrote, write_success = await write_influx_with_verification(
                write_func=lambda df_write: self._append_influx_df(base_path, df_write),
                verify_func=lambda df_verify: verify_influx_write(
                    influx_client=influx_client,
                    measurement=measurement,
                    df_original=df_verify,
                    key_cols=key_cols,
                    time_col='__ts_utc'
                ),
                df=df_all,
                retry_policy=self.cfg.retry_policy,
                logger=self.logger,
                context={
                    'symbol': symbol,
                    'asset': 'option',
                    'interval': interval,
                    'date_range': (day_iso, day_iso),
                    'sink': sink,
                    'measurement': measurement
                }
            )

            if not write_success:
                print(f"[ALERT] InfluxDB write failed after retries for option {symbol} {interval} {day_iso}")
            if wrote == 0 and len(df_all) > 0:
                print("[ALERT] Influx ha scritto 0 punti a fronte di righe in input. "
                      "Potrebbe essere tutto NaN lato fields o un cutoff troppo aggressivo.")

        else:
            raise ValueError(f"Unsupported sink: {sink_lower}")

        print(f"[SUMMARY] option {symbol} {interval} day={day_iso} rows={len(df_all)} wrote={wrote} sink={sink_lower}")


        
                       
    
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
        df_day = pd.read_csv(io.StringIO(csv_txt))
        if df_day is None or df_day.empty:
            return
    
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
            tcol = None
            for c in ("created", "timestamp", "date"):
                if c in df_day.columns:
                    tcol = c
                    break
    
            # Build list of 'YYYY-MM-DD' for each row
            if tcol in ("created", "timestamp"):
                ts = pd.to_datetime(df_day[tcol], errors="coerce", utc=True)
                day_list = ts.dt.date.astype(str)
            elif tcol == "date":
                # If 'date' exists but not parsed, coerce to string 'YYYY-MM-DD'
                day_list = pd.to_datetime(df_day["date"], errors="coerce").dt.date.astype(str)
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
                        last_chunk = pd.read_csv(target_path).tail(100)
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
                ts = pd.to_datetime(df_day[tcol], errors="coerce")
                if getattr(ts.dtype, "tz", None) is not None:
                    ts = ts.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
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
            await self._append_csv_text(base_path, csv_txt)
            wrote = len(df_day)
        elif sink_lower == "parquet":
            self._write_parquet_from_csv(base_path, csv_txt)
            wrote = len(df_day)
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

            wrote, write_success = await write_influx_with_verification(
                write_func=lambda df_write: self._append_influx_df(base_path, df_write),
                verify_func=lambda df_verify: verify_influx_write(
                    influx_client=influx_client,
                    measurement=measurement,
                    df_original=df_verify,
                    key_cols=key_cols,
                    time_col='__ts_utc'
                ),
                df=df_day,
                retry_policy=self.cfg.retry_policy,
                logger=self.logger,
                context={
                    'symbol': symbol,
                    'asset': asset,
                    'interval': interval,
                    'date_range': (day_iso, day_iso),
                    'sink': sink,
                    'measurement': measurement
                }
            )

            if not write_success:
                print(f"[ALERT] InfluxDB write failed after retries for {asset} {symbol} {interval} {day_iso}")
        else:
            raise ValueError(f"Unsupported sink: {sink_lower}")

        print(f"[SUMMARY] {asset} {symbol} {interval} day={day_iso} rows={len(df_day)} wrote={wrote} sink={sink_lower}")

        
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
        return await self.client.option_history_all_greeks(
            symbol=symbol,
            expiration=expiration,
            date=ymd,
            interval=interval,
            strike=strike,
            right=right,
            rate_type=rate_type,
            annual_dividend=annual_dividend,
            rate_value=rate_value,
            start_time=None,
            end_time=None,
            format_type=fmt,
        )




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
    
                    series_first_iso, series_last_iso, series_files = self._series_earliest_and_latest_day(
                        task.asset, symbol, interval, sink_lower
                    )
    
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
                candidates.append(datetime.combine(nd, datetime.min.time(), tzinfo=timezone.utc))
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
                    should_start_dt = datetime.combine(nd, datetime.min.time(), tzinfo=timezone.utc)
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
            first_existing, last_existing, _ = self._series_earliest_and_latest_day(
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
                
                try:
                    cli = self._ensure_influx_client()
                    
                    # MIN(time)
                    q_min = f'SELECT MIN(time) AS t FROM "{meas}"'
                    t = cli.query(q_min)
                    df_min = t.to_pandas() if hasattr(t, "to_pandas") else t
                    first_ts = None
                    if df_min is not None and len(df_min) and "t" in df_min.columns:
                        v = df_min.iloc[0]["t"]
                        first_ts = pd.to_datetime(v, utc=True, errors="coerce") if pd.notna(v) else None
                    
                    # MAX(time)
                    q_max = f'SELECT MAX(time) AS t FROM "{meas}"'
                    t = cli.query(q_max)
                    df_max = t.to_pandas() if hasattr(t, "to_pandas") else t
                    last_ts = None
                    if df_max is not None and len(df_max) and "t" in df_max.columns:
                        v = df_max.iloc[0]["t"]
                        last_ts = pd.to_datetime(v, utc=True, errors="coerce") if pd.notna(v) else None
                    
                    first_day = first_ts.tz_convert("America/New_York").date().isoformat() if first_ts else None
                    last_day = last_ts.tz_convert("America/New_York").date().isoformat() if last_ts else None
                    
                    return first_day, last_day
                except Exception as e:
                    print(f"[SINK-GLOBAL][WARN] Influx query failed for {meas}: {e}")
                    return None, None
            
            elif sink_lower in ("csv", "parquet"):
                first, last, _ = self._series_earliest_and_latest_day(asset, symbol, interval, sink_lower)
                return first, last
            
                return None, None

                
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
            lines = self._tail_csv_last_n_lines(path, n=64)
            for ln in lines:  # newest-first
                dt = self._parse_csv_first_col_as_dt(ln)
                if dt is not None:
                    if interval == "1d":
                        day = dt.date()
                        return datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc), path
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
                                last_dt = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
                            return last_dt, path
            except Exception:
                pass
            return None, None
    
        return None, None


    def _missing_1d_days_csv(self, asset: str, symbol: str, interval: str, sink: str,
                             first_day: str, last_day: str) -> list[str]:
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
            The sink type (e.g., 'csv', 'parquet').
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

        base = self._find_existing_series_base(asset, symbol, interval, sink)
        if not base:
            return []
        # raccogli tutti i part
        files = []
        p0 = self._pick_latest_part(base, "csv")
        if p0:
            # elenca da base + _partNN
            base_no_ext = base[:-4]  # rimuove ".csv"
            # Aggiungi il legacy base se esiste
            p_base = f"{base_no_ext}.csv"
            if os.path.exists(p_base):
                files.append(p_base)

            # Aggiungi TUTTI i _partNN esistenti (no early break su buchi)
            for j in range(1, 1000):
                pj = f"{base_no_ext}_part{j:02d}.csv"
                if os.path.exists(pj):
                    files.append(pj)
                    
        else:
            if os.path.exists(base):
                files.append(base)
    
        if not files:
            return []
    
        # estrai giorni osservati
        observed_days = set()
        for f in files:
            try:
                head = pd.read_csv(f, nrows=0)
                cols = head.columns.tolist()
                time_col = "created" if "created" in cols else ("timestamp" if "timestamp" in cols else ("last_trade" if "last_trade" in cols else None))
                if not time_col:
                    continue
                s = pd.read_csv(f, usecols=[time_col])[time_col]
                s = pd.to_datetime(s, errors="coerce", utc=True).dt.normalize().dropna()
                observed_days.update(s.dt.date.astype(str).tolist())
            except Exception:
                continue
    
        if not observed_days:
            return []
    
        observed = pd.DatetimeIndex(pd.to_datetime(sorted(observed_days))).normalize()
        expected = pd.bdate_range(first_day, last_day, freq="C")  # Mon-Fri (festività escluse)
        missing = expected.difference(observed)
        return [d.date().isoformat() for d in missing]
                

    def _debug_log_resume(self, task, symbol: str, interval: str, sink: str,
                          latest_path: Optional[str],
                          resume_anchor_dt: Optional[datetime],
                          computed_start_dt: Optional[datetime]) -> None:
        """
        Print a compact, explicit resume snapshot:
          1) requested start date from task,
          2) latest file path used for resume,
          3) last saved day/timestamp and the computed resume start.
        """
        # 1) "data di partenza indicata nel task"
        requested_start = getattr(task, "first_date", None) or getattr(task, "start_date", None)
        # Allow str/None; pretty print
        requested_start_s = str(requested_start) if requested_start is not None else "None"
    
        # 2) "il nome file con i dati più recenti"
        latest_path_s = latest_path or "None"
    
        # 3) "l'ultima data utile ... e da dove dovrebbe ripartire"
        last_saved_s = resume_anchor_dt.isoformat() if resume_anchor_dt else "None"
        will_start_s  = computed_start_dt.isoformat() if computed_start_dt else "None"
    
        # Expected "should start" (esplicito nel log, non altero la logica)
        should_start_dt = None
        try:
            if resume_anchor_dt is not None:
                if interval == "1d":
                    # ripartenza dal giorno successivo alle 00:00:00Z
                    next_day = (resume_anchor_dt.astimezone(timezone.utc).date() + timedelta(days=1))
                    should_start_dt = datetime.combine(next_day, datetime.min.time(), tzinfo=timezone.utc)
                else:
                    # intraday: piccolo overlap configurato
                    ov = max(0, getattr(self.cfg, "overlap_seconds", 60))
                    should_start_dt = resume_anchor_dt - timedelta(seconds=ov)
        except Exception:
            pass
        should_start_s = should_start_dt.isoformat() if should_start_dt else "None"
    
        # Stampa finale (3 righe, chiare)
        print(f"[RESUME-DEBUG] asset={getattr(task,'asset',None)} symbol={symbol} interval={interval} sink={sink}")
        print(f"[RESUME-DEBUG] requested_start={requested_start_s}  latest_file={latest_path_s}")
        print(f"[RESUME-DEBUG] resume_anchor={last_saved_s}  should_start_from={should_start_s}  computed_start={will_start_s}")


   

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
                        head = pd.read_csv(path, nrows=0)
                        cols = list(head.columns)
                        tcol = next((c for c in ["trade_timestamp","timestamp","bar_timestamp","datetime","created","last_trade"] if c in cols), None)
                        if not tcol:
                            return None
                        s = pd.read_csv(path, usecols=[tcol])[tcol]
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
        stop_before_part: int | None = None
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
            with open(target, "a", encoding="utf-8", newline="") as f:
                if write_header:
                    f.write(header + "\n")
                if chunk:
                    f.write("\n".join(chunk) + "\n")
    
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
                                    tail_ts = pd.to_datetime(tail_df[ts_col], errors="coerce")
                                    if getattr(tail_ts.dtype, "tz", None) is not None:
                                        tail_ts = tail_ts.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
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



    def _write_parquet_from_csv(self, base_path: str, csv_text: str) -> None:
        """
        Convert CSV text to DataFrame and persist into capped Parquet parts
        (..._partNN.parquet) using the same binary-search chunking as _append_parquet_df.
        """
        df_new = pd.read_csv(io.StringIO(csv_text))
        if df_new.empty:
            return

        # Normalizza tipi chiave come nel writer Parquet
        if "timestamp" in df_new.columns:
            df_new["timestamp"] = pd.to_datetime(df_new["timestamp"], errors="coerce")
            if getattr(df_new["timestamp"].dtype, "tz", None) is not None:
                df_new["timestamp"] = df_new["timestamp"].dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
                        
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
        written = self._append_parquet_df(base_path, df_new)
        return written


        
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
    
        # Annuncio una tantum se tabella vuota/nuova
        if not hasattr(self, "_influx_seen_measurements"):
            self._influx_seen_measurements = set()
        if measurement not in self._influx_seen_measurements:
            if self._influx_last_timestamp(measurement) is None:
                print(f"[INFLUX] creating new table {measurement}")
            self._influx_seen_measurements.add(measurement)
    
        # 1) Individua la colonna tempo e normalizza ET-naive -> UTC-aware
        t_candidates = ["timestamp","trade_timestamp","bar_timestamp","datetime","created","last_trade","date"]
        tcol = next((c for c in t_candidates if c in df_new.columns), None)
        if not tcol:
            raise RuntimeError("No time column found for Influx write.")
    
        s = pd.to_datetime(df_new[tcol], errors="coerce")
        # se già tz-aware, portala a ET naive per coerenza e poi rilocalizza
        if getattr(s.dtype, "tz", None) is not None:
            s = s.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
        s = s.dt.tz_localize(ZoneInfo("America/New_York"), nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert("UTC")
    
        df = df_new.copy()
        df["__ts_utc"] = s
    
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
    
        # 3) HARD DEDUPE: __ts_utc + tag se presenti (+ sequence se presente)
        tag_keys = [c for c in ["symbol","expiration","right","strike"] if c in df.columns]
        key_cols = ["__ts_utc"] + tag_keys + (["sequence"] if "sequence" in df.columns else [])
        before = len(df)
        df = df.dropna(subset=["__ts_utc"]).drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)
        dropped = before - len(df)
        print(f"[INFLUX][DEDUP] measurement={measurement} keys={key_cols} kept={len(df)}/{before} dropped={dropped}")
    
        # 4) (opzionale) esponi timestamp extra come campi numerici ns UTC
        _extra_ts_cols = ["underlying_timestamp", "timestamp_oi", "effective_date_oi"]
        for _c in _extra_ts_cols:
            if _c in df.columns:
                _ts = pd.to_datetime(df[_c], errors="coerce")
                if getattr(_ts.dtype, "tz", None) is not None:
                    _ts = _ts.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
                _ts = _ts.dt.tz_localize(ZoneInfo("America/New_York"), nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert("UTC")
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
    
        # 6) Scrittura (nessun cutoff): scriviamo tutto ciò che resta dopo il dedup
        batch = int(max(1, getattr(self.cfg, "influx_write_batch", 5000)))
        written = 0
        total = len(lines)
        for i in range(0, total, batch):
            chunk = lines[i:i+batch]
            try:
                ret = cli.write(record=chunk)
                warn = getattr(ret, "warnings", None) if ret is not None else None
                errs = getattr(ret, "errors", None)   if ret is not None else None
                stats = getattr(ret, "stats", None)   if ret is not None else None

                print(f"[INFLUX][WRITE] measurement={measurement} "
                      f"batch={i//batch+1}/{(total+batch-1)//batch} points={len(chunk)} "
                      f"status={'ok' if not errs else 'partial/error'}")

                if stats is not None:
                    print(f"[INFLUX][STATS] {stats}")
                if warn:
                    try:
                        print(f"[INFLUX][WARN] {warn}")
                    except Exception:
                        pass

                written += len(chunk)
            except Exception as e:
                # Check for InfluxDB auth errors (401/403)
                err_msg = str(e).lower()
                if 'unauthorized' in err_msg or '401' in err_msg or '403' in err_msg or 'forbidden' in err_msg:
                    print(f"[INFLUX][FATAL] Authentication/authorization failed: {e}")
                    raise InfluxDBAuthError(f"InfluxDB write failed - auth error: {e}") from e
                print(f"[INFLUX][ERROR] write failed batch={i//batch+1}: {type(e).__name__}: {e}")
    
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

        
    
    def _series_earliest_and_latest_day(self, asset: str, symbol: str, interval: str, sink_lower: str):
        """
        Determine the earliest and latest calendar days covered by a time series.

        This method scans all canonical part files for a given series and extracts the date range
        by inspecting filenames (which encode the starting day). It's used for coverage tracking
        and determining what data already exists before downloading.

        Parameters
        ----------
        asset : str
            Asset type: "option", "stock", or "index".
        symbol : str
            Ticker symbol (case-insensitive matching).
        interval : str
            Timeframe (e.g., "5m", "1m", "1d").
        sink_lower : str
            Sink format: "csv" or "parquet".

        Returns
        -------
        tuple[str or None, str or None, list]
            Three-element tuple: (earliest_day, latest_day, files)
            - earliest_day: ISO date "YYYY-MM-DD" from earliest filename prefix, or None.
            - latest_day: ISO date "YYYY-MM-DD" from latest filename prefix, or None.
            - files: List of absolute file paths for all canonical part files, sorted by name.
            Returns (None, None, []) if no files exist.

        Example Usage
        -------------
        # Called by _get_first_last_day_from_sink and coverage tracking methods
        earliest, latest, files = manager._series_earliest_and_latest_day(
            asset="stock", symbol="AAPL", interval="5m", sink_lower="parquet"
        )
        # Returns: ("2020-01-02", "2024-03-15", [list of file paths])

        Notes
        -----
        - Uses _list_series_files to get canonical part files only (excludes legacy base files).
        - Extracts dates from filename format: "YYYY-MM-DDT00-00-00Z-SYMBOL-asset-interval_partNN.ext"
        - Both earliest and latest are derived from filename prefixes (not file contents).
        - For daily-part files, each file represents exactly one calendar day.
        """
    
        files = self._list_series_files(asset, symbol, interval, sink_lower)
        if not files:
            return None, None, []
    
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

                
        return earliest, latest, files


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
                head = pd.read_csv(f, nrows=0)
                cols = head.columns.tolist()
                tcol = "created" if "created" in cols else ("timestamp" if "timestamp" in cols else ("last_trade" if "last_trade" in cols else None))
                if not tcol:
                    continue
                s = pd.read_csv(f, usecols=[tcol])[tcol]
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
        Return the last timestamp of the CSV **as naive ET string** ("YYYY-MM-DD HH:MM:SS").
        ThetaData intraday history CSVs carry ET times without timezone info.
        """

        if not os.path.exists(path):
            return None
        try:
            head = pd.read_csv(path, nrows=0)
        except Exception:
            return None
    
        cols = list(head.columns)
        time_candidates = ["trade_timestamp","timestamp","bar_timestamp","datetime","created","last_trade"]
        tcol = next((c for c in time_candidates if c in cols), None)
        if not tcol:
            return None
    
        try:
            s = pd.read_csv(path, usecols=[tcol])[tcol]
            # Parse **without** assuming UTC; treat values as naive ET.
            ts = pd.to_datetime(s, errors="coerce")
    
            # If any timezone-aware values slip in, convert to ET and drop tz.
            if getattr(ts.dtype, "tz", None) is not None:
                try:
                    ts = ts.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
                except Exception:
                    ts = ts.dt.tz_localize(None)
    
            ts = ts.dropna()
            if ts.empty:
                return None
            # Return as naive ET string (no 'Z')
            return ts.iloc[-1].strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


    
    
    ### >>> REALTIME INTRADAY WINDOW (inclusive ET) — HELPERS [BEGIN]
    def _first_timestamp_in_csv(self, path: str):
        """Read a small chunk and return the earliest timestamp as pandas.Timestamp(UTC) or None."""

        try:
            df = pd.read_csv(path, nrows=500)
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
            head = pd.read_csv(path, nrows=0)
            cols = [c for c in usecols if c in head.columns]
            df = pd.read_csv(path, usecols=cols) if cols else pd.read_csv(path)
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

        self._influx = InfluxDBClient3(host=host, token=token, database=database)
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

    def _influx_last_timestamp(self, measurement: str):
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
        except Exception:
            return None
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
        """True se esiste almeno una riga in DB per quel giorno ET."""
        s, e = self._influx__et_day_bounds_to_utc(day_iso)
        return self._influx__first_ts_between(measurement, s, e) is not None

    def _influx_first_ts_for_et_day(self, measurement: str, day_iso: str):
        """Primo ts del giorno ET in UTC, oppure None."""
        s, e = self._influx__et_day_bounds_to_utc(day_iso)
        return self._influx__first_ts_between(measurement, s, e)


    
    def _influx_last_ts_between(self, measurement: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp):
        """
        Ritorna l'ultimo timestamp (tz-aware UTC) presente in [start_utc, end_utc) per il measurement,
        oppure None se vuoto. Usa SQL Influx v3.
        """
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
        """
        Screen for abnormal OI concentration by (symbol, expiration).
    
        Rules
        -----
        - Pull prior-day OI for the requested trading date via /option/history/open_interest (expiration="*").
        - Aggregate either by strike (C+P summed) or by contract (C/P separate).
        - For each expiration, flag rows where share >= threshold_pct AND
          contract_oi >= min_contract_oi AND chain_oi >= min_chain_oi AND distinct contracts >= min_chain_contracts.
    
        Returns
        -------
        pd.DataFrame with columns:
            ['symbol','expiration','strike','right','contract_oi','chain_oi','share_pct',
             'contracts_in_chain','rank_in_expiration','source_url']
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
    
            df = pd.read_csv(io.StringIO(csv_txt))
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
        """
        Screen for abnormal daily VOLUME concentration by (symbol, expiration).
    
        Rules
        -----
        - Pull EOD daily rows for all expirations via /option/history/eod (expiration="*").
        - Aggregate either by strike (C+P summed) or by contract (C/P separate).
        - For each expiration, flag rows where share >= threshold_pct AND
          contract_volume >= min_contract_volume AND chain_volume >= min_chain_volume
          AND distinct contracts >= min_chain_contracts.
    
        Returns
        -------
        pd.DataFrame with columns:
            ['symbol','expiration','strike','right','contract_volume','chain_volume','share_pct',
             'contracts_in_chain','rank_in_expiration','source_url']
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
    
            df = pd.read_csv(io.StringIO(csv_txt))
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
        """
        Audit per giorno: duplicazioni su chiave e contabilità strike×expiration.
        - Key = timestamp + symbol + expiration + strike (+ right se presente).
        - Conta le coppie uniche (strike, expiration) per giorno.
        - Costruisce la matrice di overlap tra TUTTI i part del giorno:
            * Diagonale  = duplicati INTRA-file (stesso part) sulla chiave
            * Off-diagonale = intersezione tra chiavi di file diversi (inter-file)
        Ritorna: (results_by_day: dict, summary_df: pd.DataFrame)
    
        Notes
        -----
        * Timestamps are normalized to naive UTC to avoid aware/naive mismatches.
        * Intra-file duplicate count ignores rows with NaN in any key column.
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
                    head = pd.read_csv(path, nrows=0)
                    keep = [c for c in usecols if c in head.columns]
                    df = pd.read_csv(path, usecols=keep) if keep else pd.read_csv(path)
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
                    fl = self._get_first_last_days_from_sink(task.asset, symbol, interval, task.sink)
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
        """
        Verifica la presenza di duplicati nel sink specificato.
        
        Args:
            asset: "option", "stock", o "index"
            symbol: simbolo (es. "TLRY", "AAPL")
            interval: timeframe (es. "tick", "1m", "1d")
            sink: "influxdb", "csv", o "parquet"
            day_iso: giorno specifico (es. "2025-11-07"), o None per tutto
            sample_limit: numero max di chiavi duplicate da restituire come esempio
            
        Returns:
            dict con:
                - total_rows: numero totale righe
                - unique_rows: numero righe uniche
                - duplicates: numero duplicati
                - duplicate_rate: percentuale duplicati
                - duplicate_keys: lista primi N esempi di chiavi duplicate
                - key_columns: colonne usate come chiave
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
                    df = pd.read_csv(f)
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
                        earliest, latest, _ = self._series_earliest_and_latest_day(
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
                        df_chunk = pd.read_csv(file_path)
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
    
    def _et_hms_from_iso_utc(iso: str, minus_seconds: int = 0) -> str:
        """
        Build ET 'HH:MM:SS' from either:
        - UTC ISO (ending with 'Z' or explicit offset)  -> convert to ET, minus overlap
        - naive ET string ("YYYY-MM-DD HH:MM:SS")      -> treat as ET, minus overlap
        """
        try:
            txt = str(iso).strip()
            if ("Z" in txt) or (len(txt) >= 6 and txt[-6] in "+-"):
                # UTC input -> convert to ET
                dt = dt.fromisoformat(txt.replace("Z", "+00:00")).astimezone(ZoneInfo("America/New_York"))
            else:
                # Naive ET input -> attach ET tz without shifting the clock
                dt = dt.fromisoformat(txt).replace(tzinfo=ZoneInfo("America/New_York"))
    
            if minus_seconds and int(minus_seconds) > 0:
                dt = dt - timedelta(seconds=int(minus_seconds))
            return dt.strftime("%H:%M:%S")
        except Exception:
            return "00:00:00"

    
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

    def _tail_csv_last_n_lines(path: str, n: int = 64) -> list[str]:
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

        # Perform coherence check
        report = await checker.check(
            symbol=symbol,
            asset=asset,
            interval=interval,
            sink=sink,
            start_date=start_date,
            end_date=end_date
        )

        # Auto-recover if enabled and issues found
        if not report.is_coherent and auto_recover:
            recovery = IncoherenceRecovery(self)

            recovery_result = await recovery.recover(
                report=report,
                enrich_greeks=enrich_greeks
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
            report = await checker.check(
                symbol=symbol,
                asset=asset,
                interval=interval,
                sink=sink,
                start_date=start_date,
                end_date=end_date
            )

        return report

    # =========================================================================
    # (END)
    # DATA COHERENCE & POST-HOC VALIDATION
    # =========================================================================
