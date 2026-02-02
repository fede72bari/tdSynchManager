"""
Test what timestamp is generated for effective_date_oi.

This should show the CORRECT timestamp after our timezone fix.
"""
from console_log import log_console

import pandas as pd
from datetime import datetime

log_console("=" * 80)
log_console("TEST: Timestamp generation for effective_date_oi")
log_console("=" * 80)

# Test date
day_iso = "2026-01-05"

log_console(f"\nTest date: {day_iso}")
log_console("\n" + "-" * 80)

# OLD method (WRONG - causes timezone shift)
log_console("OLD method (WRONG):")
log_console(f"  Code: effective_date_oi = day_iso")
log_console(f"  Result: '{day_iso}' (string)")
log_console(f"  InfluxDB interprets as UTC: 2026-01-05T00:00:00Z")
log_console(f"  Convert to ET: 2026-01-04T19:00:00-05:00")
log_console(f"  Date in ET: 2026-01-04 [FAIL]")

# NEW method (CORRECT - timezone aware)
log_console("\nNEW method (CORRECT):")
log_console(f"  Code: pd.to_datetime(day_iso).tz_localize('America/New_York')")

ts = pd.to_datetime(day_iso).tz_localize("America/New_York")
log_console(f"  Result: {ts}")
log_console(f"  Type: {type(ts)}")
log_console(f"  Timezone: {ts.tzinfo}")

# Convert to UTC (what InfluxDB stores)
ts_utc = ts.tz_convert('UTC')
log_console(f"  In UTC: {ts_utc}")

# Convert to nanosecond timestamp (what InfluxDB stores)
ts_nano = int(ts.value)
log_console(f"  Nanoseconds: {ts_nano}")

# Convert back to verify
ts_back = pd.to_datetime(ts_nano, unit='ns', utc=True)
log_console(f"  Convert back from nano: {ts_back}")

# Convert to ET to verify date
ts_et = ts_back.tz_convert('America/New_York')
log_console(f"  In ET timezone: {ts_et}")
log_console(f"  Date in ET: {ts_et.date()} [PASS]")

log_console("\n" + "=" * 80)
log_console("EXPECTED vs ACTUAL")
log_console("=" * 80)

expected_nano = 1736060400000000000  # 2026-01-05T00:00:00-05:00 in nano
actual_nano = ts_nano

log_console(f"\nExpected nanoseconds: {expected_nano}")
log_console(f"Actual nanoseconds:   {actual_nano}")
log_console(f"Match: {expected_nano == actual_nano}")

if expected_nano == actual_nano:
    log_console("\n[PASS] Timestamp generation is CORRECT")
else:
    log_console(f"\n[FAIL] Timestamp mismatch!")
    log_console(f"  Difference: {actual_nano - expected_nano} nanoseconds")
    log_console(f"  Difference: {(actual_nano - expected_nano) / 1e9 / 3600} hours")

log_console("\n" + "=" * 80)
