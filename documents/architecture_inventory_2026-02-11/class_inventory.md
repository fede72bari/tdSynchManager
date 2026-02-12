# Class And Dependency Inventory

Generated (UTC): 2026-02-11T10:33:37.054436+00:00
Scanned root: `d:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src`

## Summary

- Files scanned: 18
- Classes: 26
- Class methods: 309
- Top-level functions: 42

## Classes

### src/clients/ThetaDataV3Client.py

File imports:
- `from __future__ import annotations`
- `from console_log import log_console`
- `asyncio`
- `json`
- `os`
- `from typing import Any`
- `from typing import Dict`
- `from typing import Optional`
- `from typing import Tuple`
- `from typing import Literal`
- `from urllib.parse import urlencode`
- `aiohttp`

- Class `ThetaDataV3HTTPError` (line 19)
  - Bases: Exception
  - Method count: 1
  - Uses imported aliases: (none detected)
  - Methods:
    - `__init__` (line 22)

- Class `ThetaDataV3Client` (line 61)
  - Bases: (none)
  - Method count: 58
  - Uses imported aliases: Any, Dict, Literal, Optional, Tuple, aiohttp, asyncio, json, os, urlencode
  - Methods:
    - `__init__` (line 68)
    - `async __aenter__` (line 135)
    - `async __aexit__` (line 172)
    - `async _make_request` (line 214)
    - `async stock_list_symbols` (line 300)
    - `async stock_list_dates` (line 345)
    - `async stock_snapshot_ohlc` (line 414)
    - `async stock_snapshot_trade` (line 464)
    - `async stock_snapshot_quote` (line 512)
    - `async stock_history_eod` (line 569)
    - `async stock_history_ohlc` (line 636)
    - `async stock_history_trade` (line 744)
    - `async stock_history_quote` (line 846)
    - `async stock_history_trade_quote` (line 967)
    - `async stock_at_time_trade` (line 1129)
    - `async stock_at_time_quote` (line 1231)
    - `async option_list_symbols` (line 1357)
    - `async option_list_dates` (line 1408)
    - `async option_list_expirations` (line 1524)
    - `async option_list_strikes` (line 1588)
    - `async option_list_contracts` (line 1650)
    - `async option_snapshot_ohlc` (line 1754)
    - `async option_snapshot_trade` (line 1834)
    - `async option_snapshot_quote` (line 1922)
    - `async option_snapshot_open_interest` (line 2017)
    - `async option_snapshot_implied_volatility` (line 2098)
    - `async option_snapshot_all_greeks` (line 2179)
    - `async option_snapshot_first_order_greeks` (line 2262)
    - `async option_snapshot_second_order_greeks` (line 2328)
    - `async option_snapshot_third_order_greeks` (line 2392)
    - `async option_history_eod` (line 2464)
    - `async option_history_ohlc` (line 2605)
    - `async option_history_trade` (line 2716)
    - `async option_history_quote` (line 2821)
    - `async option_history_trade_quote` (line 2933)
    - `async option_history_open_interest` (line 3047)
    - `async option_history_greeks_eod` (line 3135)
    - `async option_history_all_greeks` (line 3266)
    - `async option_history_all_trade_greeks` (line 3337)
    - `async option_history_greeks` (line 3408)
    - `async option_history_trade_greeks` (line 3511)
    - `async option_history_greeks_second_order` (line 3643)
    - `async option_history_trade_greeks_second_order` (line 3731)
    - `async option_history_greeks_third_order` (line 3840)
    - `async option_history_trade_greeks_third_order` (line 3907)
    - `async option_history_implied_volatility` (line 4005)
    - `async option_history_trade_implied_volatility` (line 4150)
    - `async option_at_time_trade` (line 4254)
    - `async option_at_time_quote` (line 4341)
    - `async index_list_symbols` (line 4451)
    - `async index_list_dates` (line 4509)
    - `async index_snapshot_ohlc` (line 4575)
    - `async index_snapshot_price` (line 4617)
    - `async index_history_eod` (line 4665)
    - `async index_history_ohlc` (line 4720)
    - `async index_history_price` (line 4809)
    - `async index_at_time_price` (line 4898)
    - `async calendar_on_date` (line 4979)

- Class `ResilientThetaClient` (line 6711)
  - Bases: (none)
  - Method count: 41
  - Uses imported aliases: aiohttp, asyncio
  - Methods:
    - `__init__` (line 6765)
    - `async _execute_with_reconnect` (line 6786)
    - `async _reconnect` (line 6862)
    - `async stock_list_symbols` (line 6891)
    - `async stock_list_dates` (line 6894)
    - `async stock_history_eod` (line 6897)
    - `async stock_history_ohlc` (line 6900)
    - `async stock_history_trade_quote` (line 6903)
    - `async stock_snapshot_trade_quote` (line 6906)
    - `async stock_snapshot_ohlc` (line 6909)
    - `async stock_snapshot_quote` (line 6912)
    - `async option_list_symbols` (line 6916)
    - `async option_list_dates` (line 6919)
    - `async option_list_expirations` (line 6922)
    - `async option_list_strikes` (line 6925)
    - `async option_list_contracts` (line 6928)
    - `async option_history_eod` (line 6931)
    - `async option_history_ohlc` (line 6934)
    - `async option_history_greeks_eod` (line 6937)
    - `async option_history_all_greeks` (line 6941)
    - `async option_history_implied_volatility` (line 6944)
    - `async option_history_open_interest` (line 6947)
    - `async option_history_trade_quote` (line 6950)
    - `async option_history_all_trade_greeks` (line 6953)
    - `async option_history_trade_implied_volatility` (line 6956)
    - `async option_snapshot_all_greeks` (line 6960)
    - `async option_snapshot_trade_quote` (line 6963)
    - `async option_snapshot_ohlc` (line 6966)
    - `async option_snapshot_quote` (line 6969)
    - `async index_list_dates` (line 6973)
    - `async index_history_eod` (line 6976)
    - `async index_history_ohlc` (line 6979)
    - `async index_history_price` (line 6982)
    - `async index_snapshot_ohlc` (line 6985)
    - `async index_snapshot_price` (line 6988)
    - `async calendar_on_date` (line 6992)
    - `base_url` (line 6997)
    - `api_key` (line 7001)
    - `async __aenter__` (line 7005)
    - `async __aexit__` (line 7008)
    - `async close` (line 7011)

### src/tdSynchManager/coherence.py

File imports:
- `from console_log import log_console`
- `from contextlib import contextmanager`
- `os`
- `re`
- `from dataclasses import dataclass`
- `from dataclasses import field`
- `from datetime import datetime`
- `from datetime import timedelta`
- `from typing import TYPE_CHECKING`
- `from typing import Dict`
- `from typing import List`
- `from typing import Optional`
- `from typing import Tuple`
- `from .logger import DataConsistencyLogger`
- `from .validator import DataValidator`
- `from .validator import ValidationResult`

- Class `CoherenceIssue` (line 31)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `CoherenceReport` (line 55)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `RecoveryResult` (line 100)
  - Bases: (none)
  - Method count: 1
  - Uses imported aliases: (none detected)
  - Methods:
    - `add_result` (line 119)

- Class `CoherenceChecker` (line 137)
  - Bases: (none)
  - Method count: 12
  - Uses imported aliases: DataValidator, Dict, List, Optional, Tuple, ValidationResult, datetime, log_console, os, timedelta
  - Methods:
    - `__init__` (line 144)
    - `async check` (line 157)
    - `async _check_eod_completeness` (line 252)
    - `async _check_intraday_completeness` (line 303)
    - `async _check_tick_completeness` (line 413)
    - `_generate_date_range` (line 594)
    - `async _read_intraday_day` (line 620)
    - `async _read_tick_day` (line 714)
    - `async _get_eod_volumes` (line 742)
    - `async _segment_intraday_problems` (line 778)
    - `async _check_missing_enrichment_files` (line 886)
    - `async _segment_tick_problems` (line 921)

- Class `IncoherenceRecovery` (line 1020)
  - Bases: (none)
  - Method count: 8
  - Uses imported aliases: DataValidator, contextmanager, datetime, log_console, os, re, timedelta
  - Methods:
    - `__init__` (line 1023)
    - `_suspend_validation` (line 1037)
    - `async recover` (line 1049)
    - `async _recover_eod_day` (line 1120)
    - `async _influx_intraday_gap_still_missing` (line 1246)
    - `async _recover_intraday_gap` (line 1303)
    - `async _recover_tick_day` (line 1409)
    - `async _recover_missing_enrichment_file` (line 1471)

### src/tdSynchManager/config.py

File imports:
- `from __future__ import annotations`
- `os`
- `from dataclasses import dataclass`
- `from dataclasses import field`
- `from dataclasses import fields`
- `from typing import Any`
- `from typing import List`
- `from typing import Mapping`
- `from typing import Optional`
- `from typing import Union`
- `from typing import get_args`
- `from typing import get_origin`

- Class `RetryPolicy` (line 11)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `ManagerConfig` (line 35)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `DiscoverPolicy` (line 103)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `Task` (line 138)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

### src/tdSynchManager/exceptions.py

- Class `TdSynchError` (line 12)
  - Bases: Exception
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `SessionClosedError` (line 17)
  - Bases: TdSynchError
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `TruncatedResponseError` (line 22)
  - Bases: TdSynchError
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `InfluxDBAuthError` (line 27)
  - Bases: TdSynchError
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `ValidationError` (line 32)
  - Bases: TdSynchError
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

### src/tdSynchManager/influx_retry.py

File imports:
- `from console_log import log_console`
- `time`
- `os`
- `json`
- `from pathlib import Path`
- `from typing import List`
- `from typing import Optional`
- `from typing import Dict`
- `from typing import Any`
- `from datetime import datetime`

- Class `InfluxWriteRetry` (line 15)
  - Bases: (none)
  - Method count: 5
  - Uses imported aliases: Any, Dict, List, Optional, Path, datetime, json, log_console, os, time
  - Methods:
    - `__init__` (line 18)
    - `write_with_retry` (line 54)
    - `_save_failed_batch` (line 127)
    - `get_stats` (line 157)
    - `print_summary` (line 161)

### src/tdSynchManager/influx_verification.py

File imports:
- `from console_log import log_console`
- `from dataclasses import dataclass`
- `from datetime import datetime`
- `from datetime import date`
- `from typing import List`
- `from typing import Tuple`
- `from typing import Optional`
- `from typing import Set`
- `numpy as np`
- `pandas as pd`

- Class `InfluxWriteResult` (line 17)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

### src/tdSynchManager/logger.py

File imports:
- `from console_log import log_console`
- `json`
- `os`
- `time`
- `uuid`
- `from datetime import datetime`
- `from datetime import timezone`
- `from pathlib import Path`
- `from typing import Any`
- `from typing import Dict`
- `from typing import Optional`
- `from typing import Tuple`
- `pandas as pd`

- Class `DataConsistencyLogger` (line 17)
  - Bases: (none)
  - Method count: 18
  - Uses imported aliases: Any, Dict, Optional, Path, Tuple, datetime, json, log_console, os, pd, time, timezone, uuid
  - Methods:
    - `__init__` (line 47)
    - `log_missing_data` (line 72)
    - `log_retry_attempt` (line 126)
    - `log_info` (line 179)
    - `log_error` (line 202)
    - `log_warning` (line 247)
    - `log_resolution` (line 289)
    - `log_failure` (line 337)
    - `log_influx_failure` (line 386)
    - `log_session_closed` (line 443)
    - `_log_event` (line 497)
    - `_persist_log` (line 573)
    - `_safe_read_parquet` (line 670)
    - `_quarantine_corrupt_parquet` (line 677)
    - `_atomic_write_parquet` (line 688)
    - `get_logs` (line 712)
    - `display_logs` (line 757)
    - `print_logs` (line 769)

### src/tdSynchManager/manager.py

File imports:
- `from __future__ import annotations`
- `asyncio`
- `datetime`
- `glob`
- `importlib`
- `inspect`
- `io`
- `json`
- `math`
- `os`
- `re`
- `requests`
- `tempfile`
- `uuid`
- `from datetime import datetime as dt`
- `from datetime import timedelta`
- `from datetime import timezone`
- `from pathlib import Path`
- `from typing import Any`
- `from typing import Dict`
- `from typing import Iterable`
- `from typing import List`
- `from typing import Optional`
- `from typing import Tuple`
- `from typing import get_args`
- `from urllib.parse import urlencode`
- `from zoneinfo import ZoneInfo`
- `aiohttp`
- `numpy as np`
- `pandas as pd`
- `pyarrow as pa`
- `pyarrow.parquet as pq`
- `from clients.ThetaDataV3Client import Interval`
- `from clients.ThetaDataV3Client import ThetaDataV3Client`
- `from clients.ThetaDataV3Client import ResilientThetaClient`
- `from console_log import log_console`
- `from console_log import set_log_verbosity`
- `from console_log import set_log_context`
- `from console_log import reset_log_context`
- `from .config import DiscoverPolicy`
- `from .config import ManagerConfig`
- `from .config import Task`
- `from .config import config_from_env`
- `from .logger import DataConsistencyLogger`
- `from .validator import DataValidator`
- `from .validator import ValidationResult`
- `from .retry import retry_with_policy`
- `from .download_retry import download_with_retry_and_validation`
- `from .influx_retry import InfluxWriteRetry`

- Class `ThetaSyncManager` (line 158)
  - Bases: (none)
  - Method count: 149
  - Uses imported aliases: Any, DataConsistencyLogger, DataValidator, Dict, DiscoverPolicy, InfluxWriteRetry, Iterable, List, ManagerConfig, Optional, Path, ResilientThetaClient, Task, Tuple, ZoneInfo, aiohttp, asyncio, datetime, download_with_retry_and_validation, dt, glob, inspect, io, json, log_console, math, np, os, pa, pd, pq, re, requests, reset_log_context, set_log_context, set_log_verbosity, tempfile, timedelta, timezone, uuid
  - Methods:
    - `__init__` (line 293)
    - `show_logs` (line 377)
    - `async _validate_downloaded_data` (line 489)
    - `async _get_eod_volume_for_validation` (line 720)
    - `async run` (line 873)
    - `async _spawn_sync` (line 917)
    - `async _is_trading_day` (line 951)
    - `async _fetch_available_dates_from_api` (line 1006)
    - `async _sync_symbol` (line 1249)
    - `async _download_and_store_options` (line 1813)
    - `async _download_and_store_equity_or_index` (line 4566)
    - `async _expirations_that_traded` (line 4995)
    - `async _fetch_option_all_greeks_by_date` (line 5110)
    - `async _td_get_with_retry` (line 5205)
    - `async _resolve_first_date` (line 5287)
    - `async _discover_equity_first_date` (line 5327)
    - `async _discover_option_first_date` (line 5411)
    - `async _binary_search_first_date_option` (line 5530)
    - `_extract_first_date_from_any` (line 5605)
    - `_extract_expirations_as_dates` (line 5621)
    - `_today_market_date` (line 5643)
    - `async _compute_resume_start_datetime` (line 5663)
    - `_skip_existing_middle_day` (line 5831)
    - `_get_first_last_day_from_sink` (line 5947)
    - `_probe_existing_last_ts_with_source` (line 6154)
    - `_missing_1d_days_csv` (line 6203)
    - `_compute_intraday_window_et` (line 6350)
    - `async _write_df_to_sink` (line 6442)
    - `async _append_csv_text` (line 6482)
    - `_append_parquet_df` (line 6677)
    - `_write_parquet_from_csv` (line 6943)
    - `_normalize_ts_to_utc` (line 6975)
    - `_normalize_expiration_value` (line 7026)
    - `_ensure_expiration_column` (line 7043)
    - `_fill_expiration_from_option_symbol` (line 7103)
    - `_require_expiration` (line 7147)
    - `_add_dte_column` (line 7197)
    - `_format_dt_columns_isoz` (line 7208)
    - `_normalize_df_types` (line 7228)
    - `_ensure_ts_utc_column` (line 7316)
    - `_pick_time_column` (line 7339)
    - `async _append_influx_df` (line 7370)
    - `_make_file_basepath` (line 7703)
    - `_get_dates_in_eod_batch_files` (line 7744)
    - `_list_series_files` (line 7804)
    - `_list_day_files` (line 7860)
    - `_missing_data_policy` (line 7929)
    - `_missing_enrichment_path` (line 7943)
    - `_list_missing_enrichment_files` (line 7951)
    - `_normalize_recovery_keys` (line 7989)
    - `_influx_existing_keys_between` (line 8013)
    - `_influx_split_recovery_keys` (line 8068)
    - `_filter_df_against_influx_existing` (line 8113)
    - `_recovery_key_columns` (line 8160)
    - `_filter_df_by_keys` (line 8173)
    - `_record_missing_enrichment_rows` (line 8192)
    - `_update_missing_enrichment_recovery` (line 8356)
    - `_pending_missing_enrichment_rows` (line 8402)
    - `_list_day_part_files` (line 8416)
    - `_day_parts_status` (line 8431)
    - `_find_existing_series_base` (line 8483)
    - `_find_existing_daily_base_for_day` (line 8503)
    - `_sort_and_deduplicate_eod_batch` (line 8521)
    - `_pick_latest_part` (line 8626)
    - `_next_part_path` (line 8640)
    - `_ensure_under_cap` (line 8650)
    - `_purge_day_files` (line 8666)
    - `_sink_dir_name` (line 8678)
    - `_csv_has_day` (line 8715)
    - `_last_csv_day` (line 8783)
    - `_last_csv_timestamp` (line 8824)
    - `_first_timestamp_in_csv` (line 8857)
    - `_first_timestamp_in_parquet` (line 8879)
    - `_max_file_timestamp` (line 8898)
    - `_read_minimal_frame` (line 8918)
    - `_ensure_influx_client` (line 8946)
    - `_influx_query_dataframe` (line 8979)
    - `_extract_scalar_from_df` (line 8999)
    - `_coerce_timestamp` (line 9017)
    - `_pick_timestamp_from_dataframe` (line 9034)
    - `_infer_first_last_from_sink` (line 9068)
    - `_iter_influx_parquet_files` (line 9106)
    - `_list_influx_tables` (line 9164)
    - `_influx_measurement_from_base` (line 9228)
    - `_influx_measurement_exists` (line 9241)
    - `_influx_available_dates_measurement` (line 9303)
    - `_influx_available_dates__ensure_state` (line 9306)
    - `_influx_available_dates__load_existing` (line 9316)
    - `_influx_available_dates_note_days` (line 9339)
    - `_influx_available_dates_get_all` (line 9374)
    - `_influx_available_dates_first_last_day` (line 9400)
    - `_influx_available_dates_has_day` (line 9414)
    - `_influx_available_dates_bootstrap_from_main` (line 9424)
    - `_influx_last_timestamp` (line 9516)
    - `async _expected_option_combos_for_day` (line 9556)
    - `_influx__et_day_bounds_to_utc` (line 9605)
    - `_influx__first_ts_between` (line 9614)
    - `_influx_day_has_any` (line 9647)
    - `_influx_first_ts_for_et_day` (line 9651)
    - `_influx_last_ts_between` (line 9658)
    - `_get_oi_cache_path` (line 9702)
    - `_check_oi_cache` (line 9709)
    - `_load_oi_from_cache` (line 9742)
    - `_save_oi_to_cache` (line 9792)
    - `_get_cached_first_date` (line 9838)
    - `_set_cached_first_date` (line 9851)
    - `_load_cache_file` (line 9863)
    - `_save_cache_file` (line 9899)
    - `_touch_cache` (line 9941)
    - `_cache_key` (line 10011)
    - `async screen_option_oi_concentration` (line 10052)
    - `async screen_option_volume_concentration` (line 10236)
    - `generate_duplicate_report` (line 10411)
    - `duplication_and_strike_checks` (line 10748)
    - `check_duplicates_in_sink` (line 11070)
    - `check_duplicates_multi_day` (line 11160)
    - `_check_duplicates_influx` (line 11461)
    - `_check_duplicates_file` (line 11598)
    - `list_available_data` (line 11738)
    - `query_local_data` (line 11989)
    - `_apply_get_filters_and_ordering` (line 12326)
    - `available_expiration_chains` (line 12471)
    - `available_strikes_by_expiration` (line 12606)
    - `get_storage_stats` (line 12724)
    - `get_storage_summary` (line 12933)
    - `clear_first_date_cache` (line 13036)
    - `_validate_interval` (line 13070)
    - `_validate_sink` (line 13100)
    - `_as_utc` (line 13130)
    - `_floor_to_interval_et` (line 13160)
    - `_floor_to_interval_seconds_et` (line 13188)
    - `_parse_interval_spec` (line 13196)
    - `_align_start_time_from_last_ts` (line 13213)
    - `_iso_date_only` (line 13275)
    - `_td_ymd` (line 13305)
    - `_greeks_version_for_expiration` (line 13333)
    - `_iso_stamp` (line 13345)
    - `_min_ts_from_df` (line 13391)
    - `_ensure_list` (line 13413)
    - `_normalize_date_str` (line 13426)
    - `_parse_date` (line 13434)
    - `_iter_days` (line 13448)
    - `_tail_csv_last_n_lines` (line 13460)
    - `_tail_one_line` (line 13524)
    - `_parse_csv_first_col_as_dt` (line 13543)
    - `_extract_days_from_df` (line 13580)
    - `_detect_time_col` (line 13597)
    - `async _maybe_await` (line 13603)
    - `async check_and_recover_coherence` (line 13651)

### src/tdSynchManager/output_manager.py

File imports:
- `from console_log import log_console`
- `sys`
- `time`
- `from typing import Optional`

- Class `AutoClearOutputManager` (line 19)
  - Bases: (none)
  - Method count: 9
  - Uses imported aliases: log_console, time
  - Methods:
    - `__init__` (line 51)
    - `__enter__` (line 75)
    - `__exit__` (line 79)
    - `_should_clear` (line 85)
    - `_do_clear` (line 91)
    - `print` (line 120)
    - `clear` (line 141)
    - `reset` (line 146)
    - `get_stats` (line 155)

### src/tdSynchManager/tick_bucket_analysis.py

File imports:
- `from console_log import log_console`
- `from dataclasses import dataclass`
- `from dataclasses import field`
- `from datetime import datetime`
- `from datetime import timedelta`
- `from typing import TYPE_CHECKING`
- `from typing import Dict`
- `from typing import List`
- `from typing import Optional`
- `pandas as pd`

- Class `BucketAnalysisResult` (line 20)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `DayBucketAnalysisReport` (line 50)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

### src/tdSynchManager/validator.py

File imports:
- `from console_log import log_console`
- `from dataclasses import dataclass`
- `from datetime import datetime`
- `from datetime import timedelta`
- `from typing import Any`
- `from typing import Dict`
- `from typing import List`
- `from typing import Optional`
- `from typing import Tuple`
- `pandas as pd`

- Class `ValidationResult` (line 14)
  - Bases: (none)
  - Method count: 0
  - Uses imported aliases: (none detected)
  - Methods:

- Class `DataValidator` (line 34)
  - Bases: (none)
  - Method count: 7
  - Uses imported aliases: List, Optional, Tuple, datetime, log_console, pd, timedelta
  - Methods:
    - `validate_eod_completeness` (line 38)
    - `validate_intraday_completeness` (line 123)
    - `validate_tick_vs_eod_volume` (line 288)
    - `validate_required_columns` (line 500)
    - `_expected_candles_for_interval` (line 588)
    - `_find_time_gaps` (line 621)
    - `_compute_date_ranges` (line 668)

## Top-Level Functions By File

### src/clients/ThetaDataV3Client.py
- `extract_error_text` (line 5060)
- `async test_all_endpoints` (line 5118)
- `async run_comprehensive_tests` (line 5836)
- `extract_error_text` (line 5876)
- `async test_all_endpoints` (line 5934)
- `async run_comprehensive_tests` (line 6668)

### src/console_log.py
- `set_log_verbosity` (line 51)
- `get_log_verbosity` (line 60)
- `set_log_context` (line 65)
- `reset_log_context` (line 83)
- `_get_raw_print` (line 88)
- `_format_symbol_type_tf` (line 94)
- `_infer_context` (line 110)
- `_detect_log_type` (line 191)
- `_normalize_log_type` (line 218)
- `_min_verbosity_for` (line 227)
- `log_console` (line 235)

### src/tdSynchManager/__init__.py
- `_detect_version` (line 15)
- `new_manager` (line 55)

### src/tdSynchManager/config.py
- `_strip_optional` (line 188)
- `_cast_value` (line 222)
- `config_from_env` (line 263)

### src/tdSynchManager/credentials.py
- `get_project_root` (line 13)
- `load_credentials` (line 28)
- `get_influx_credentials` (line 86)
- `get_thetadata_credentials` (line 105)

### src/tdSynchManager/download_retry.py
- `async download_with_retry_and_validation` (line 13)

### src/tdSynchManager/influx_retry.py
- `recover_failed_batches` (line 187)
- `delete_failed_batch` (line 275)
- `list_failed_batches` (line 356)

### src/tdSynchManager/influx_verification.py
- `async verify_influx_write` (line 40)
- `async write_influx_with_verification` (line 272)

### src/tdSynchManager/manager.py
- `_print_shell_version_banner` (line 56)
- `install_td_server_error_logger` (line 96)

### src/tdSynchManager/output_manager.py
- `get_global_output_manager` (line 176)
- `set_global_output_manager` (line 184)
- `smart_print` (line 190)

### src/tdSynchManager/retry.py
- `async retry_with_policy` (line 14)

### src/tdSynchManager/tick_bucket_analysis.py
- `async analyze_tick_buckets` (line 88)
- `async _fetch_intraday_bars` (line 283)
- `async _load_tick_data` (line 399)
- `async _compare_buckets` (line 470)
