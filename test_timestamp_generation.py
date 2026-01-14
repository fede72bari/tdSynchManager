"""
Test what timestamp is generated for effective_date_oi.

This should show the CORRECT timestamp after our timezone fix.
"""

import pandas as pd
from datetime import datetime

print("=" * 80)
print("TEST: Timestamp generation for effective_date_oi")
print("=" * 80)

# Test date
day_iso = "2026-01-05"

print(f"\nTest date: {day_iso}")
print("\n" + "-" * 80)

# OLD method (WRONG - causes timezone shift)
print("OLD method (WRONG):")
print(f"  Code: effective_date_oi = day_iso")
print(f"  Result: '{day_iso}' (string)")
print(f"  InfluxDB interprets as UTC: 2026-01-05T00:00:00Z")
print(f"  Convert to ET: 2026-01-04T19:00:00-05:00")
print(f"  Date in ET: 2026-01-04 [FAIL]")

# NEW method (CORRECT - timezone aware)
print("\nNEW method (CORRECT):")
print(f"  Code: pd.to_datetime(day_iso).tz_localize('America/New_York')")

ts = pd.to_datetime(day_iso).tz_localize("America/New_York")
print(f"  Result: {ts}")
print(f"  Type: {type(ts)}")
print(f"  Timezone: {ts.tzinfo}")

# Convert to UTC (what InfluxDB stores)
ts_utc = ts.tz_convert('UTC')
print(f"  In UTC: {ts_utc}")

# Convert to nanosecond timestamp (what InfluxDB stores)
ts_nano = int(ts.value)
print(f"  Nanoseconds: {ts_nano}")

# Convert back to verify
ts_back = pd.to_datetime(ts_nano, unit='ns', utc=True)
print(f"  Convert back from nano: {ts_back}")

# Convert to ET to verify date
ts_et = ts_back.tz_convert('America/New_York')
print(f"  In ET timezone: {ts_et}")
print(f"  Date in ET: {ts_et.date()} [PASS]")

print("\n" + "=" * 80)
print("EXPECTED vs ACTUAL")
print("=" * 80)

expected_nano = 1736060400000000000  # 2026-01-05T00:00:00-05:00 in nano
actual_nano = ts_nano

print(f"\nExpected nanoseconds: {expected_nano}")
print(f"Actual nanoseconds:   {actual_nano}")
print(f"Match: {expected_nano == actual_nano}")

if expected_nano == actual_nano:
    print("\n[PASS] Timestamp generation is CORRECT")
else:
    print(f"\n[FAIL] Timestamp mismatch!")
    print(f"  Difference: {actual_nano - expected_nano} nanoseconds")
    print(f"  Difference: {(actual_nano - expected_nano) / 1e9 / 3600} hours")

print("\n" + "=" * 80)
