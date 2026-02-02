"""
Test to trace the exact flow of effective_date_oi timezone handling.

This reproduces the exact steps that happen in the codebase to see
where the timezone information is lost.
"""
from console_log import log_console

import pandas as pd
import numpy as np

log_console("=" * 80)
log_console("TEST: Trace timezone handling flow")
log_console("=" * 80)

# Step 1: Create effective_date_oi exactly as in manager.py line 2065
day_iso = "2026-01-05"
effective_date_oi_value = pd.to_datetime(day_iso).tz_localize("America/New_York")

log_console(f"\nStep 1: Create timezone-aware timestamp")
log_console(f"  day_iso: {day_iso}")
log_console(f"  effective_date_oi: {effective_date_oi_value}")
log_console(f"  Type: {type(effective_date_oi_value)}")
log_console(f"  Timezone: {effective_date_oi_value.tzinfo}")
log_console(f"  Nanoseconds: {effective_date_oi_value.value}")

# Step 2: Create a DataFrame with this value (simulating doi DataFrame)
df = pd.DataFrame({
    "option_symbol": ["AAL_260109C18500", "AAL_260109P18500"],
    "last_day_OI": [100, 200],
    "effective_date_oi": [effective_date_oi_value, effective_date_oi_value]
})

log_console(f"\nStep 2: Create DataFrame")
log_console(f"  effective_date_oi dtype: {df['effective_date_oi'].dtype}")
log_console(f"  Has timezone: {getattr(df['effective_date_oi'].dtype, 'tz', None) is not None}")
if hasattr(df['effective_date_oi'].dtype, 'tz'):
    log_console(f"  Timezone: {df['effective_date_oi'].dtype.tz}")
log_console(f"  Sample value: {df['effective_date_oi'].iloc[0]}")

# Step 3: Simulate merge (manager.py line 2078-2095)
# Create another DataFrame to merge with
df_main = pd.DataFrame({
    "option_symbol": ["AAL_260109C18500", "AAL_260109P18500", "AAL_260109C19000"],
    "close": [0.5, 1.2, 0.8],
    "volume": [100, 200, 50]
})

df_merged = df_main.merge(
    df[["option_symbol", "last_day_OI", "effective_date_oi"]],
    how="left",
    on=["option_symbol"],
    suffixes=("", "_oi_dup")
)

log_console(f"\nStep 3: After merge")
log_console(f"  effective_date_oi dtype: {df_merged['effective_date_oi'].dtype}")
log_console(f"  Has timezone: {getattr(df_merged['effective_date_oi'].dtype, 'tz', None) is not None}")
if hasattr(df_merged['effective_date_oi'].dtype, 'tz'):
    log_console(f"  Timezone: {df_merged['effective_date_oi'].dtype.tz}")
log_console(f"  Sample value: {df_merged['effective_date_oi'].iloc[0]}")
log_console(f"  Has NaN: {df_merged['effective_date_oi'].isna().any()}")

# Step 4: Simulate copy (like in _ensure_ts_utc_column)
df_copy = df_merged.copy()

log_console(f"\nStep 4: After copy()")
log_console(f"  effective_date_oi dtype: {df_copy['effective_date_oi'].dtype}")
log_console(f"  Has timezone: {getattr(df_copy['effective_date_oi'].dtype, 'tz', None) is not None}")
if hasattr(df_copy['effective_date_oi'].dtype, 'tz'):
    log_console(f"  Timezone: {df_copy['effective_date_oi'].dtype.tz}")

# Step 5: Simulate the InfluxDB conversion logic (manager.py line 6492-6535)
log_console(f"\nStep 5: InfluxDB timestamp conversion")

_c = "effective_date_oi"
log_console(f"  Column '{_c}' dtype: {df_copy[_c].dtype}")
log_console(f"  is_datetime64_any_dtype: {pd.api.types.is_datetime64_any_dtype(df_copy[_c])}")

if pd.api.types.is_datetime64_any_dtype(df_copy[_c]):
    _ts = df_copy[_c]
    tz_info = getattr(_ts.dtype, "tz", None)
    log_console(f"  Column is datetime, timezone: {tz_info}")

    if tz_info is not None:
        log_console(f"  [CORRECT PATH] Timezone-aware, converting to UTC")
        _ts_utc = _ts.dt.tz_convert("UTC")
        log_console(f"  After tz_convert: {_ts_utc.iloc[0]}")
    else:
        log_console(f"  [WRONG PATH] No timezone, localizing to UTC")
        _ts_utc = _ts.dt.tz_localize("UTC")
        log_console(f"  After tz_localize: {_ts_utc.iloc[0]}")

    # Convert to nanoseconds
    ns = _ts_utc.astype("int64").astype("float64")
    log_console(f"  Nanoseconds: {int(ns.iloc[0])}")
else:
    log_console(f"  [FALLBACK PATH] NOT datetime type, parsing...")
    _ts = pd.to_datetime(df_copy[_c], errors="coerce")
    log_console(f"  After pd.to_datetime: {_ts.iloc[0]}")
    log_console(f"  Dtype: {_ts.dtype}")

    if getattr(_ts.dtype, "tz", None) is None:
        _ts_utc = _ts.dt.tz_localize("UTC")
        log_console(f"  Localized to UTC: {_ts_utc.iloc[0]}")
    else:
        _ts_utc = _ts.dt.tz_convert("UTC")
        log_console(f"  Converted to UTC: {_ts_utc.iloc[0]}")

    ns = _ts_utc.astype("int64").astype("float64")
    log_console(f"  Nanoseconds: {int(ns.iloc[0])}")

log_console("\n" + "=" * 80)
log_console("VERIFICATION")
log_console("=" * 80)

expected_nano = 1767589200000000000  # 2026-01-05 00:00:00 ET
actual_nano = int(ns.iloc[0])
wrong_nano = 1767571200000000000  # 2026-01-05 00:00:00 UTC (wrong)

log_console(f"\nExpected (CORRECT): {expected_nano}")
log_console(f"Wrong (UTC misinterp): {wrong_nano}")
log_console(f"Actual: {actual_nano}")

if actual_nano == expected_nano:
    log_console("\n[PASS] Timezone preserved correctly!")
elif actual_nano == wrong_nano:
    log_console("\n[FAIL] Timezone was lost - interpreted as UTC instead of ET!")
    log_console("This means the timestamp lost its timezone info somewhere in the flow.")
else:
    log_console(f"\n[UNKNOWN] Unexpected nanosecond value!")
    diff_hours = (actual_nano - expected_nano) / 1e9 / 3600
    log_console(f"Difference from expected: {diff_hours} hours")

log_console("\n" + "=" * 80)
