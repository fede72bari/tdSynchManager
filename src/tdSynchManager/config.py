from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from typing import Any, List, Mapping, Optional, Union, get_args, get_origin

__all__ = ["ManagerConfig", "DiscoverPolicy", "Task", "RetryPolicy", "config_from_env"]


@dataclass
class RetryPolicy:
    """Retry policy configuration for data download and consistency recovery.

    Attributes
    ----------
    max_attempts : int
        Maximum number of retry attempts for failed downloads (default 3).
    delay_seconds : float
        Delay in seconds between retry attempts (default 15.0).
    truncated_response_delay : float
        Specific delay for truncated/incomplete responses (default 60.0).
    truncated_max_attempts : int
        Maximum retries specifically for truncated responses (default 2).
    session_closed_max_attempts : int
        Maximum reconnection attempts when session is closed (default 1).
    """
    max_attempts: int = 3
    delay_seconds: float = 15.0
    truncated_response_delay: float = 60.0
    truncated_max_attempts: int = 2
    session_closed_max_attempts: int = 1


@dataclass
class ManagerConfig:
    """Global configuration for ThetaSyncManager.

    Attributes
    ----------
    root_dir : str
        Root folder for outputs and internal cache.
    max_concurrency : int
        Maximum number of concurrent sync jobs.
    max_file_mb : int
        Per-file size cap (MB) for CSV/Parquet sinks before rotating to _partNN.
    overlap_seconds : int
        Safety overlap applied when resuming from the last saved timestamp.
    cache_dir_name : str
        Subfolder (under root_dir) for internal cache files.
    cache_file_name : str
        JSON filename for coverage cache.
    option_check_first_k_expirations : int
        How many oldest expirations to probe when discovering option first date.
    discovery_request_type : str
        Request type used for discovery ("trade" or "quote").
    """

    root_dir: str
    max_concurrency: int = 8
    max_file_mb: int = 200
    overlap_seconds: int = 60
    cache_dir_name: str = ".theta_cache"
    cache_file_name: str = "coverage.json"
    option_check_first_k_expirations: int = 3
    discovery_request_type: str = "trade"
    eod_batch_days: int = 30
    eod_head_overlap_days: int = 1
    eod_resume_overlap_days: int = 1

    # --- InfluxDB (minimal) ---
    influx_url: Optional[str] = None
    influx_org: Optional[str] = None          # InfluxDB v2/v3 (org name)
    influx_bucket: Optional[str] = None       # bucket (db)
    influx_token: Optional[str] = None        # auth token
    influx_precision: str = "ns"              # "ns","us","ms","s"
    influx_measure_prefix: str = ""           # optional, e.g., "td_"
    influx_write_batch: int = 5000            # batch size for writes
    influx_data_dir: Optional[str] = None     # v3 Core: data directory (for real disk size calculation)

    # --- OI (Open Interest) Caching ---
    enable_oi_caching: bool = True            # Enable OI caching for intraday (works with any sink)
    oi_cache_dir: str = ".oi_cache"           # Directory for cached OI files (relative to root_dir)

    # --- Data Consistency & Validation ---
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    enable_data_validation: bool = True       # Enable validation before saving
    validation_strict_mode: bool = True       # True = "all or nothing", False = save partial with log
    log_verbose_console: bool = True          # Print logs to console in addition to log tables
    log_verbosity: int = 3                    # 0=none, 1=warnings+errors, 2=operations, 3=debug
    eod_check_max_attempts: int = 1           # Additional retry attempts for EOD tick data check
    tick_eod_volume_tolerance: float = 0.01   # Tolerance for tick vs EOD volume comparison (1%)
    intraday_bucket_tolerance: float = 0.0    # Allowed fraction of missing time buckets (0 = require 100%)
    tick_segment_minutes: int = 30            # Bucket size for tick volume analysis (default 30 minutes)
    enable_tick_bucket_analysis: bool = True  # Enable granular tick bucket analysis on daily volume FAIL
    greeks_iv_oi_failure_retry_passes: int = 2  # Extra delayed passes for option enrichment failures (OI/Greeks/IV)
    greeks_iv_oi_failure_delay_seconds: float = 30.0  # Delay between retry passes
    skip_day_on_greeks_iv_oi_failure: bool = False  # Skip entire option day (1d + intraday) if any OI/Greeks/IV missing after retries
    skip_dimension_on_missing_data: Optional[str] = None  # "full_day" or "candle_or_tick_row" (None => legacy skip_day_on_greeks_iv_oi_failure)
    greeks_version: Optional[str] = "latest"  # "latest" (real TTE) or "1" (fixed .15 DTE for 0DTE)


@dataclass
class DiscoverPolicy:
    """
    Discovery policy for finding the first date with available data.

    Attributes
    ----------
    mode : str
        - "auto": use cache if present, discover if missing.
        - "force": ignore cache and re-discover (if supported in your build).
        - "mild_skip": skip global discovery but keep per-day middle checks
                       (legacy 'skip' behavior).
        - "skip": strong skip -- no global discovery AND bypass per-day middle checks;
                  resume starts from last-known timestamp minus overlap_seconds.
    request_type : str
        "trade" or "quote"; determines which list/dates tree we use.
    check_first_k_expirations : Optional[int]
        If not None, overrides ManagerConfig.option_check_first_k_expirations.
    enable_option_binary_fallback : bool
        Enable binary-search fallback on /option/list/contracts/{request_type}
        if expirations->dates yields nothing.
    binary_search_start : str
        Fallback lower bound (inclusive) date in ISO format.
    binary_search_end : Optional[str]
        Fallback upper bound (inclusive); defaults to today if None.
    """

    mode: str = "auto"
    request_type: str = "trade"   # "trade" | "quote"
    check_first_k_expirations: Optional[int] = None
    enable_option_binary_fallback: bool = True
    binary_search_start: str = "2010-01-01"
    binary_search_end: Optional[str] = None


@dataclass
class Task:
    """A synchronization job definition.

    Attributes
    ----------
    asset : str
        One of {"stock", "index", "option"}.
    symbols : List[str]
        List of roots/symbols to sync (e.g., ["AAPL"], ["SPY"], ["ES"]).
    intervals : List[str]
        Bars timeframe identifiers (e.g., ["1m"], ["10m"], ["1h"]).
    sink : str
        Storage sink {"parquet", "csv", "influxdb"}.
    refresh_seconds : Optional[int]
        Informational (external scheduler can call manager periodically).
    enrich_bar_greeks : bool
        If True and asset=="option", also persist per-interval Greeks as companion file.
    enrich_tick_greeks : bool
        If True and asset=="option", enrich tick data with Greeks at trade-level granularity.
    first_date_override : Optional[str]
        Optional ISO date (YYYY-MM-DD) to force the start coverage for this task.
    end_date_override : Optional[str]
        Optional ISO date (YYYY-MM-DD) to force the end coverage for this task (instead of today).
    discover_policy : DiscoverPolicy
        Controls discovery behavior (auto/force/skip, request type, fallbacks).
    ignore_existing: bool
        Ignore last datetime in the database and restart from the given date.
    use_api_date_discovery : bool
        If True (default), query ThetaData API for available trading dates to avoid
        querying non-existent dates. Set to False for fast updates (skip->last_date to now)
        to avoid downloading full historical date list (useful for options with many expirations).
    """

    asset: str
    symbols: List[str]
    intervals: List[str]
    sink: str
    refresh_seconds: Optional[int] = None
    enrich_bar_greeks: bool = False
    enrich_tick_greeks: bool = False
    first_date_override: Optional[str] = None
    end_date_override: Optional[str] = None
    discover_policy: DiscoverPolicy = field(default_factory=DiscoverPolicy)
    ignore_existing: bool = False
    use_api_date_discovery: bool = True


_TRUE_VALUES = {"1", "true", "t", "y", "yes", "on"}


def _strip_optional(tp: Any) -> Any:
    """Extract the inner type from an Optional type hint.

    This helper function analyzes a type annotation and strips away the Optional wrapper
    to reveal the underlying base type. For example, Optional[int] becomes int.
    Returns the original type unchanged if it's not an Optional.

    Parameters
    ----------
    tp : Any
        The type annotation to analyze. Can be any Python type hint including
        Optional[T], Union types, or plain types like str, int, etc.

    Returns
    -------
    Any
        The inner type without the Optional wrapper. If the input is Optional[int],
        returns int. If the input is not Optional, returns the type unchanged.

    Example Usage
    -------------
    This is an internal helper function used by _cast_value() to determine the actual
    type that needs to be cast when parsing environment variable values. For instance,
    when a config field is defined as Optional[int], this function extracts int so
    that the string value can be properly converted to an integer.
    """
    origin = get_origin(tp)
    if origin is Union:
        args = [arg for arg in get_args(tp) if arg is not type(None)]
        if len(args) == 1:
            return args[0]
    return tp


def _cast_value(field_def, raw: str) -> Any:
    """Convert a raw string value to the appropriate Python type based on field definition.

    This helper function takes a string value (typically from an environment variable)
    and converts it to the correct Python type according to the dataclass field definition.
    It handles bool, int, float, and str types. For boolean values, it recognizes common
    truthy strings like "1", "true", "yes", etc. Returns the converted value.

    Parameters
    ----------
    field_def : Field
        A dataclass field definition containing type information. The function uses
        field_def.type to determine how to cast the raw string value.
    raw : str
        The raw string value to convert. This is typically a string read from an
        environment variable that needs to be converted to the proper type.

    Returns
    -------
    Any
        The converted value in the appropriate type (bool, int, float, or str).
        For booleans, returns True if raw matches any value in _TRUE_VALUES
        ({"1", "true", "t", "y", "yes", "on"}), False otherwise.

    Example Usage
    -------------
    This is an internal helper function used by config_from_env() when parsing
    environment variables. For example, if a ManagerConfig field is defined as
    max_concurrency: int = 8, and the environment has TDSYNCH_MAX_CONCURRENCY="16",
    this function converts the string "16" to the integer 16.
    """
    base = _strip_optional(field_def.type)
    if base is bool:
        return str(raw).strip().lower() in _TRUE_VALUES
    if base is int:
        return int(raw)
    if base is float:
        return float(raw)
    return raw


def config_from_env(
    *,
    env: Mapping[str, str] | None = None,
    prefix: str = "TDSYNCH_",
    overrides: Mapping[str, Any] | None = None,
    **explicit: Any,
) -> ManagerConfig:
    """Build and return a ManagerConfig instance by reading values from environment variables.

    This function constructs a complete ManagerConfig object by reading environment variables,
    applying overrides, and merging explicit keyword arguments. It provides a flexible way to
    configure the ThetaData synchronization manager from multiple sources with clear precedence
    rules: explicit kwargs override overrides, which override environment variables. Returns a
    fully populated ManagerConfig instance ready to use.

    Parameters
    ----------
    env : Mapping[str, str] | None, optional
        Default: None (uses os.environ)
        A dictionary-like mapping of environment variable names to their string values.
        When None, the function reads from the system's os.environ. This parameter allows
        you to pass a custom environment mapping for testing or special configuration scenarios.

    prefix : str, optional
        Default: "TDSYNCH_"
        The prefix string prepended to ManagerConfig field names when looking up environment
        variables. For example, with prefix="TDSYNCH_", the field root_dir is read from
        TDSYNCH_ROOT_DIR. You can customize this to match your environment variable naming
        convention or to support multiple configuration namespaces.

    overrides : Mapping[str, Any] | None, optional
        Default: None
        A dictionary of field names to values that should override any values found in the
        environment. These have higher priority than environment variables but lower priority
        than explicit keyword arguments. Use this to programmatically override specific
        configuration values without modifying environment variables.

    **explicit : Any
        Keyword arguments representing specific ManagerConfig fields (e.g., root_dir="/path",
        max_concurrency=16). These have the highest priority and will override both environment
        variables and overrides. Use these for values that must be guaranteed at runtime.

    Returns
    -------
    ManagerConfig
        A fully initialized ManagerConfig instance with all fields populated according to
        the precedence rules (explicit > overrides > environment > defaults).

    Raises
    ------
    ValueError
        If the required root_dir field cannot be determined from any of the sources
        (env, overrides, or explicit kwargs). The error message indicates which environment
        variable or parameter should be set.

    Example Usage
    -------------
    # Basic usage with environment variables:
    # Set TDSYNCH_ROOT_DIR=/data/theta in your environment
    config = config_from_env()

    # With explicit root_dir override:
    config = config_from_env(root_dir="/custom/path", max_concurrency=4)

    # With custom environment mapping:
    custom_env = {"TDSYNCH_ROOT_DIR": "/test/path", "TDSYNCH_MAX_CONCURRENCY": "12"}
    config = config_from_env(env=custom_env)

    # With overrides dictionary:
    config = config_from_env(overrides={"max_file_mb": 500}, max_concurrency=16)

    This function is typically called once at application startup to initialize the
    manager configuration from environment variables, making it easy to deploy with
    different settings in different environments (dev, staging, production).
    """

    env_mapping = env or os.environ
    values: dict[str, Any] = {}
    if overrides:
        values.update(overrides)
    values.update(explicit)

    for field_def in fields(ManagerConfig):
        if field_def.name in values:
            continue
        key = f"{prefix}{field_def.name}".upper()
        if key in env_mapping:
            raw = env_mapping[key]
            if raw == "" and _strip_optional(field_def.type) is not str:
                # Treat empty string as None for non-str optional types.
                values[field_def.name] = None
            else:
                values[field_def.name] = _cast_value(field_def, raw)

    root_dir = values.get("root_dir")
    if not root_dir:
        raise ValueError(
            f"ManagerConfig requires 'root_dir'. Set '{prefix}ROOT_DIR' or pass root_dir=..."
        )

    return ManagerConfig(**values)
