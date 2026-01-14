"""
Decode the timestamps seen in InfluxDB to understand the issue.
"""

import pandas as pd

print("=" * 80)
print("DECODE: Timestamps from InfluxDB")
print("=" * 80)

# What the user is seeing in InfluxDB (WRONG)
wrong_ts_nano = 1767571200000000000

print(f"\nWRONG timestamp from InfluxDB: {wrong_ts_nano}")
wrong_ts = pd.to_datetime(wrong_ts_nano, unit='ns', utc=True)
print(f"  In UTC: {wrong_ts}")

wrong_ts_et = wrong_ts.tz_convert('America/New_York')
print(f"  In ET:  {wrong_ts_et}")
print(f"  Date:   {wrong_ts_et.date()} [This is what we see - WRONG]")

# What we SHOULD see (CORRECT)
correct_ts_nano = 1767589200000000000

print(f"\nCORRECT timestamp (expected): {correct_ts_nano}")
correct_ts = pd.to_datetime(correct_ts_nano, unit='ns', utc=True)
print(f"  In UTC: {correct_ts}")

correct_ts_et = correct_ts.tz_convert('America/New_York')
print(f"  In ET:  {correct_ts_et}")
print(f"  Date:   {correct_ts_et.date()} [This is what we want - CORRECT]")

# Difference
diff_nano = correct_ts_nano - wrong_ts_nano
diff_hours = diff_nano / 1e9 / 3600

print(f"\nDifference:")
print(f"  Nanoseconds: {diff_nano}")
print(f"  Hours:       {diff_hours}")
print(f"  Days:        {diff_hours / 24}")

# What generates the WRONG timestamp?
print("\n" + "=" * 80)
print("ANALYSIS: What generates the WRONG timestamp?")
print("=" * 80)

# If we use string "2026-01-05" directly
day_iso = "2026-01-05"

# Pandas interprets string as UTC
ts_string_as_utc = pd.to_datetime(day_iso, utc=True)
print(f"\nString '{day_iso}' interpreted as UTC:")
print(f"  Result: {ts_string_as_utc}")
print(f"  Nanoseconds: {int(ts_string_as_utc.value)}")
print(f"  Match WRONG: {int(ts_string_as_utc.value) == wrong_ts_nano}")

# If we localize to ET (CORRECT)
ts_localized_et = pd.to_datetime(day_iso).tz_localize('America/New_York')
print(f"\nString '{day_iso}' localized to ET:")
print(f"  Result: {ts_localized_et}")
print(f"  Nanoseconds: {int(ts_localized_et.value)}")
print(f"  Match CORRECT: {int(ts_localized_et.value) == correct_ts_nano}")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)

if int(ts_string_as_utc.value) == wrong_ts_nano:
    print("\n[FOUND] The WRONG timestamp matches pd.to_datetime(day_iso, utc=True)")
    print("This suggests the code is NOT using .tz_localize('America/New_York')")
    print("OR there's a conversion happening somewhere that's removing the timezone info")
else:
    print("\n[MYSTERY] The WRONG timestamp doesn't match expected patterns")
    print("Need to investigate further")

print("\n" + "=" * 80)
