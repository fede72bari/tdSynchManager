"""
Test to trace the exact flow of effective_date_oi timezone handling.

This reproduces the exact steps that happen in the codebase to see
where the timezone information is lost.
"""

import pandas as pd
import numpy as np

print("=" * 80)
print("TEST: Trace timezone handling flow")
print("=" * 80)

# Step 1: Create effective_date_oi exactly as in manager.py line 2065
day_iso = "2026-01-05"
effective_date_oi_value = pd.to_datetime(day_iso).tz_localize("America/New_York")

print(f"\nStep 1: Create timezone-aware timestamp")
print(f"  day_iso: {day_iso}")
print(f"  effective_date_oi: {effective_date_oi_value}")
print(f"  Type: {type(effective_date_oi_value)}")
print(f"  Timezone: {effective_date_oi_value.tzinfo}")
print(f"  Nanoseconds: {effective_date_oi_value.value}")

# Step 2: Create a DataFrame with this value (simulating doi DataFrame)
df = pd.DataFrame({
    "option_symbol": ["AAL_260109C18500", "AAL_260109P18500"],
    "last_day_OI": [100, 200],
    "effective_date_oi": [effective_date_oi_value, effective_date_oi_value]
})

print(f"\nStep 2: Create DataFrame")
print(f"  effective_date_oi dtype: {df['effective_date_oi'].dtype}")
print(f"  Has timezone: {getattr(df['effective_date_oi'].dtype, 'tz', None) is not None}")
if hasattr(df['effective_date_oi'].dtype, 'tz'):
    print(f"  Timezone: {df['effective_date_oi'].dtype.tz}")
print(f"  Sample value: {df['effective_date_oi'].iloc[0]}")

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

print(f"\nStep 3: After merge")
print(f"  effective_date_oi dtype: {df_merged['effective_date_oi'].dtype}")
print(f"  Has timezone: {getattr(df_merged['effective_date_oi'].dtype, 'tz', None) is not None}")
if hasattr(df_merged['effective_date_oi'].dtype, 'tz'):
    print(f"  Timezone: {df_merged['effective_date_oi'].dtype.tz}")
print(f"  Sample value: {df_merged['effective_date_oi'].iloc[0]}")
print(f"  Has NaN: {df_merged['effective_date_oi'].isna().any()}")

# Step 4: Simulate copy (like in _ensure_ts_utc_column)
df_copy = df_merged.copy()

print(f"\nStep 4: After copy()")
print(f"  effective_date_oi dtype: {df_copy['effective_date_oi'].dtype}")
print(f"  Has timezone: {getattr(df_copy['effective_date_oi'].dtype, 'tz', None) is not None}")
if hasattr(df_copy['effective_date_oi'].dtype, 'tz'):
    print(f"  Timezone: {df_copy['effective_date_oi'].dtype.tz}")

# Step 5: Simulate the InfluxDB conversion logic (manager.py line 6492-6535)
print(f"\nStep 5: InfluxDB timestamp conversion")

_c = "effective_date_oi"
print(f"  Column '{_c}' dtype: {df_copy[_c].dtype}")
print(f"  is_datetime64_any_dtype: {pd.api.types.is_datetime64_any_dtype(df_copy[_c])}")

if pd.api.types.is_datetime64_any_dtype(df_copy[_c]):
    _ts = df_copy[_c]
    tz_info = getattr(_ts.dtype, "tz", None)
    print(f"  Column is datetime, timezone: {tz_info}")

    if tz_info is not None:
        print(f"  [CORRECT PATH] Timezone-aware, converting to UTC")
        _ts_utc = _ts.dt.tz_convert("UTC")
        print(f"  After tz_convert: {_ts_utc.iloc[0]}")
    else:
        print(f"  [WRONG PATH] No timezone, localizing to UTC")
        _ts_utc = _ts.dt.tz_localize("UTC")
        print(f"  After tz_localize: {_ts_utc.iloc[0]}")

    # Convert to nanoseconds
    ns = _ts_utc.astype("int64").astype("float64")
    print(f"  Nanoseconds: {int(ns.iloc[0])}")
else:
    print(f"  [FALLBACK PATH] NOT datetime type, parsing...")
    _ts = pd.to_datetime(df_copy[_c], errors="coerce")
    print(f"  After pd.to_datetime: {_ts.iloc[0]}")
    print(f"  Dtype: {_ts.dtype}")

    if getattr(_ts.dtype, "tz", None) is None:
        _ts_utc = _ts.dt.tz_localize("UTC")
        print(f"  Localized to UTC: {_ts_utc.iloc[0]}")
    else:
        _ts_utc = _ts.dt.tz_convert("UTC")
        print(f"  Converted to UTC: {_ts_utc.iloc[0]}")

    ns = _ts_utc.astype("int64").astype("float64")
    print(f"  Nanoseconds: {int(ns.iloc[0])}")

print("\n" + "=" * 80)
print("VERIFICATION")
print("=" * 80)

expected_nano = 1767589200000000000  # 2026-01-05 00:00:00 ET
actual_nano = int(ns.iloc[0])
wrong_nano = 1767571200000000000  # 2026-01-05 00:00:00 UTC (wrong)

print(f"\nExpected (CORRECT): {expected_nano}")
print(f"Wrong (UTC misinterp): {wrong_nano}")
print(f"Actual: {actual_nano}")

if actual_nano == expected_nano:
    print("\n[PASS] Timezone preserved correctly!")
elif actual_nano == wrong_nano:
    print("\n[FAIL] Timezone was lost - interpreted as UTC instead of ET!")
    print("This means the timestamp lost its timezone info somewhere in the flow.")
else:
    print(f"\n[UNKNOWN] Unexpected nanosecond value!")
    diff_hours = (actual_nano - expected_nano) / 1e9 / 3600
    print(f"Difference from expected: {diff_hours} hours")

print("\n" + "=" * 80)
