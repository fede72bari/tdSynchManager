#!/usr/bin/env python3
"""
Example: Get storage statistics for local ThetaSync databases.

This script demonstrates how to configure the manager from environment variables,
instantiate it, and print detailed plus aggregated storage statistics. Adjust the
paths in the "Local environment defaults" block or set/export the variables before
running the script.
"""

import os
import sys

sys.path.insert(0, 'src')

try:  # Notebook-friendly display
    from IPython.display import display as _ipython_display
except Exception:  # pragma: no cover
    _ipython_display = None

from tdSynchManager.client import ThetaDataV3Client
from tdSynchManager.config import config_from_env
from tdSynchManager.manager import ThetaSyncManager

# ---------------------------------------------------------------------------
# Local environment defaults (edit as needed for your workstation). These
# are only applied when the corresponding environment variables are unset.
# ---------------------------------------------------------------------------
DEFAULT_ROOT_DIR = os.environ.get("TDSYNCH_ROOT_DIR") or r"C:\Users\Federico\Downloads"
DEFAULT_INFLUX_DIR = os.environ.get("TDSYNCH_INFLUX_DATA_DIR") or r"C:\Users\Federico\Downloads\data\influxdb3"
DEFAULT_CLIENT_BASE_URL = os.environ.get("THETADATA_BASE_URL") or "http://localhost:25503/v3"

os.environ.setdefault("TDSYNCH_ROOT_DIR", DEFAULT_ROOT_DIR)
if DEFAULT_INFLUX_DIR:
    os.environ.setdefault("TDSYNCH_INFLUX_DATA_DIR", DEFAULT_INFLUX_DIR)

# Optional InfluxDB API overrides (uncomment + edit if you want API queries)
# os.environ.setdefault("TDSYNCH_INFLUX_URL", "http://localhost:8181")
# os.environ.setdefault("TDSYNCH_INFLUX_BUCKET", "default")
# os.environ.setdefault("TDSYNCH_INFLUX_TOKEN", "replace-with-your-token")

# Create configuration and manager
cfg = config_from_env()
theta_client = ThetaDataV3Client(base_url=DEFAULT_CLIENT_BASE_URL)
manager = ThetaSyncManager(cfg, client=theta_client)


def _show_df(df):
    """Display a DataFrame using IPython.display when available."""
    if _ipython_display:
        _ipython_display(df)
    else:
        print(df.to_string())


def _print_view(title: str, df, columns=None) -> None:
    """Pretty-print a filtered DataFrame, handling empty frames and missing columns."""
    print(f"\n{title}")
    if df.empty:
        print("   (no data)")
        return
    view = df
    if columns:
        cols = [col for col in columns if col in df.columns]
        if cols:
            view = df[cols]
    _show_df(view)


print("=" * 80)
print("STORAGE STATISTICS - DETAILED VIEW")
print("=" * 80)

stats = manager.get_storage_stats()
print(f"\nFound {len(stats)} data series")
print("\nDetailed statistics (sorted by size):")
_print_view("All series:", stats)

print("\n" + "=" * 80)
print("FILTERED VIEWS")
print("=" * 80)

_print_view(
    "1. Options only:",
    manager.get_storage_stats(asset='option'),
    ['symbol', 'interval', 'sink', 'size_mb', 'days_span'],
)
_print_view(
    "2. InfluxDB only:",
    manager.get_storage_stats(sink='influxdb'),
    ['symbol', 'asset', 'interval', 'size_mb'],
)
_print_view(
    "3. TLRY only:",
    manager.get_storage_stats(symbol='TLRY'),
    ['interval', 'sink', 'size_mb', 'days_span'],
)
_print_view(
    "4. Tick data only:",
    manager.get_storage_stats(interval='tick'),
    ['symbol', 'asset', 'sink', 'size_mb'],
)

print("\n" + "=" * 80)
print("STORAGE SUMMARY - AGGREGATED VIEW")
print("=" * 80)

summary = manager.get_storage_summary()

print("\nTOTAL STORAGE:")
total = summary['total']
print(f"   Size: {total['size_gb']:.3f} GB ({total['size_mb']:.2f} MB)")
print(f"   Series: {total['series_count']}")

print("\nBY SINK:")
for sink, data in summary['by_sink'].items():
    print(f"   {sink:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  [{data['series_count']} series]")

print("\nBY ASSET:")
for asset, data in summary['by_asset'].items():
    print(f"   {asset:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  [{data['series_count']} series]")

print("\nBY INTERVAL:")
for interval, data in summary['by_interval'].items():
    print(f"   {interval:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  [{data['series_count']} series]")

print("\nTOP 10 SYMBOLS BY SIZE:")
for i, symbol_data in enumerate(summary['top_symbols'], 1):
    symbol = symbol_data['symbol']
    size_gb = symbol_data['size_gb']
    pct = symbol_data['percentage']
    print(f"   {i:2d}. {symbol:8s}: {size_gb:8.3f} GB  ({pct:5.1f}%)")

print("\n" + "=" * 80)
