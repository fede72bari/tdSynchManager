"""
Test the InfluxDB timestamp conversion logic for timezone-aware columns.

This verifies that timezone-aware timestamps in columns like effective_date_oi
are preserved correctly when converting to nanoseconds for InfluxDB storage.
"""

import pandas as pd
import numpy as np

print("=" * 80)
print("TEST: InfluxDB timestamp conversion for timezone-aware columns")
print("=" * 80)

# Create a test DataFrame with timezone-aware effective_date_oi
day_iso = "2026-01-05"
effective_date_oi_et = pd.to_datetime(day_iso).tz_localize("America/New_York")

print(f"\nOriginal effective_date_oi (ET timezone-aware):")
print(f"  Value: {effective_date_oi_et}")
print(f"  Timezone: {effective_date_oi_et.tzinfo}")
print(f"  Nanoseconds: {int(effective_date_oi_et.value)}")

# Create DataFrame
df = pd.DataFrame({
    "effective_date_oi": [effective_date_oi_et, effective_date_oi_et, effective_date_oi_et]
})

print(f"\nDataFrame column dtype: {df['effective_date_oi'].dtype}")
print(f"Has timezone: {getattr(df['effective_date_oi'].dtype, 'tz', None) is not None}")

print("\n" + "-" * 80)
print("Simulating InfluxDB conversion (NEW CODE):")
print("-" * 80)

# Simulate the NEW conversion logic (after fix)
_c = "effective_date_oi"

# Check if already a datetime column with timezone info
if pd.api.types.is_datetime64_any_dtype(df[_c]):
    print("Column is already datetime64 type")
    _ts = df[_c]
    # If already timezone-aware, just convert to UTC
    if getattr(_ts.dtype, "tz", None) is not None:
        print("Column has timezone info, converting to UTC")
        _ts = _ts.dt.tz_convert("UTC")
    else:
        # No timezone, assume UTC
        print("Column has no timezone, localizing to UTC")
        _ts = _ts.dt.tz_localize("UTC")
else:
    # Not a datetime column, try to parse
    print("Column is not datetime64 type, parsing")
    _ts = pd.to_datetime(df[_c], errors="coerce")
    if getattr(_ts.dtype, "tz", None) is None:
        _ts = _ts.dt.tz_localize("UTC")
    else:
        _ts = _ts.dt.tz_convert("UTC")

print(f"\nAfter conversion:")
print(f"  Timezone: {_ts.dtype.tz}")
print(f"  Sample value: {_ts.iloc[0]}")

# Convert to nanoseconds
try:
    ns = _ts.astype("int64", copy=False).astype("float64")
except Exception:
    ns = _ts.to_numpy(dtype="datetime64[ns]").astype("int64").astype("float64")

ns[_ts.isna()] = np.nan

print(f"  Nanoseconds: {int(ns.iloc[0])}")

print("\n" + "=" * 80)
print("VALIDATION:")
print("=" * 80)

expected_nano = 1767589200000000000  # 2026-01-05 00:00:00 ET = 2026-01-05 05:00:00 UTC
actual_nano = int(ns.iloc[0])

print(f"\nExpected nanoseconds: {expected_nano}")
print(f"Actual nanoseconds:   {actual_nano}")
print(f"Match: {expected_nano == actual_nano}")

# Verify the stored value converts back correctly
stored_ts = pd.to_datetime(actual_nano, unit='ns', utc=True)
stored_ts_et = stored_ts.tz_convert('America/New_York')

print(f"\nStored timestamp (UTC): {stored_ts}")
print(f"Stored timestamp (ET):  {stored_ts_et}")
print(f"Extracted date (ET):    {stored_ts_et.date()}")

if expected_nano == actual_nano and stored_ts_et.date().isoformat() == day_iso:
    print("\n[PASS] Conversion is CORRECT - timezone preserved!")
else:
    print("\n[FAIL] Conversion is WRONG!")
    if expected_nano != actual_nano:
        diff_hours = (actual_nano - expected_nano) / 1e9 / 3600
        print(f"  Difference: {diff_hours} hours")

print("\n" + "=" * 80)
