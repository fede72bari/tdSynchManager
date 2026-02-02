#!/usr/bin/env python3
"""
Example: Get storage statistics for local ThetaSync databases.

This script mirrors the same configuration you use for sync jobs (explicit ManagerConfig)
and offers optional controls to include or skip the InfluxDB scan, which can take a while
on large buckets. Adjust the constants below to match your workstation.
"""
from console_log import log_console

import os
import sys
from typing import Iterable, Optional

import pandas as pd

sys.path.insert(0, "src")

try:  # Notebook-friendly display
    from IPython.display import display as _ipython_display
except Exception:  # pragma: no cover
    _ipython_display = None

from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.credentials import get_influx_credentials

# ---------------------------------------------------------------------------
# Configuration (copy of your production settings)
# ---------------------------------------------------------------------------
ROOT_DIR = "examples/data"
INFLUX_DIR = "examples/data/influxdb3"

# Load credentials from .credentials.json
influx = get_influx_credentials()
INFLUX_URL = influx['url']
INFLUX_BUCKET = influx['bucket']
INFLUX_TOKEN = influx['token']

cfg = ManagerConfig(
    root_dir=ROOT_DIR,
    max_concurrency=80,
    max_file_mb=16,
    overlap_seconds=60,
    influx_url=INFLUX_URL,
    influx_bucket=INFLUX_BUCKET,
    influx_token=INFLUX_TOKEN,
    influx_org=None,
    influx_precision="nanosecond",
    influx_measure_prefix="",
    influx_write_batch=5000,
    influx_data_dir=INFLUX_DIR,
)
manager = ThetaSyncManager(cfg, client=None)  # Client not needed for local stats

# Toggle Influx scan (set False to focus on CSV/Parquet only).
SCAN_INFLUX = True
DEBUG_INFLUX_SIZE = True  # Print per-folder size breakdown for Influx sinks
# Optional symbol filters for Influx; set to [] or None to scan all measurements.
INFLUX_SYMBOL_FILTERS: Optional[Iterable[str]] = ["TLRY"]


def _show_df(df):
    """Display a DataFrame via IPython when available."""
    if _ipython_display:
        _ipython_display(df)
    else:
        log_console(df.to_string())


def _print_view(title: str, df, columns=None) -> None:
    """Pretty-print a filtered DataFrame, handling empty frames and missing columns."""
    log_console(f"\n{title}")
    if df.empty:
        log_console("   (no data)")
        return
    view = df
    if columns:
        cols = [col for col in columns if col in df.columns]
        if cols:
            view = df[cols]
    _show_df(view)


def _collect_stats() -> pd.DataFrame:
    """Collect stats per sink, optionally including InfluxDB."""
    frames = []

    log_console("[INFO] Scansione CSV...")
    frames.append(manager.get_storage_stats(sink="csv"))

    log_console("[INFO] Scansione Parquet...")
    frames.append(manager.get_storage_stats(sink="parquet"))

    if SCAN_INFLUX:
        symbol_filters = list(INFLUX_SYMBOL_FILTERS) if INFLUX_SYMBOL_FILTERS else [None]
        for sym in symbol_filters:
            label = sym or "tutti i simboli"
            log_console(f"[INFO] Scansione InfluxDB ({label})... potrebbe richiedere tempo.")
            stats_df = manager.get_storage_stats(sink="influxdb", symbol=sym)
            if DEBUG_INFLUX_SIZE and not stats_df.empty:
                for _, row in stats_df.iterrows():
                    symbol = row["symbol"]
                    asset = row["asset"]
                    interval = row["interval"]
                    measurement = f"{symbol}-{asset}-{interval}"
                    log_console(f"[DEBUG] Misura {measurement}")
                    total_size = 0
                    file_count = 0
                    for file_path in manager._iter_influx_parquet_files(measurement):
                        file_size = os.path.getsize(file_path)
                        total_size += file_size
                        file_count += 1
                        log_console(f"    {file_path}: {file_size} bytes")
                    log_console(f"    -> totale {file_count} file, {total_size} bytes\n")
            frames.append(stats_df)
    else:
        log_console("[INFO] Scansione InfluxDB disattivata (imposta SCAN_INFLUX=True per abilitarla).")

    frames = [df for df in frames if not df.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _build_summary(stats: pd.DataFrame) -> dict:
    """Mirror manager.get_storage_summary without triggering another scan."""
    if stats.empty:
        return {
            "total": {"size_bytes": 0, "size_mb": 0, "size_gb": 0, "series_count": 0},
            "by_sink": {},
            "by_asset": {},
            "by_interval": {},
            "top_symbols": [],
        }

    total_bytes = stats["size_bytes"].sum()
    total_mb = round(total_bytes / (1024 * 1024), 2)
    total_gb = round(total_bytes / (1024 * 1024 * 1024), 3)

    def _group_summary(grouped):
        summary = {}
        for key, group in grouped:
            size_bytes = group["size_bytes"].sum()
            summary[key] = {
                "size_bytes": size_bytes,
                "size_mb": round(size_bytes / (1024 * 1024), 2),
                "size_gb": round(size_bytes / (1024 * 1024 * 1024), 3),
                "series_count": len(group),
                "percentage": round(100 * size_bytes / total_bytes, 1) if total_bytes else 0,
            }
        return summary

    by_sink = _group_summary(stats.groupby("sink"))
    by_asset = _group_summary(stats.groupby("asset"))
    by_interval = _group_summary(stats.groupby("interval"))

    symbol_totals = stats.groupby("symbol")["size_bytes"].sum().sort_values(ascending=False)
    top_symbols = [
        {
            "symbol": symbol,
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 2),
            "size_gb": round(size_bytes / (1024 * 1024 * 1024), 3),
            "percentage": round(100 * size_bytes / total_bytes, 1) if total_bytes else 0,
        }
        for symbol, size_bytes in symbol_totals.head(10).items()
    ]

    return {
        "total": {
            "size_bytes": total_bytes,
            "size_mb": total_mb,
            "size_gb": total_gb,
            "series_count": len(stats),
        },
        "by_sink": by_sink,
        "by_asset": by_asset,
        "by_interval": by_interval,
        "top_symbols": top_symbols,
    }


log_console("=" * 80)
log_console("STORAGE STATISTICS - DETAILED VIEW")
log_console("=" * 80)

stats = _collect_stats()

log_console(f"\nFound {len(stats)} data series")
log_console("\nDetailed statistics (sorted by size):")
_print_view("All series:", stats.sort_values("size_bytes", ascending=False))

log_console("\n" + "=" * 80)
log_console("FILTERED VIEWS")
log_console("=" * 80)

_print_view(
    "1. Options only:",
    stats[stats["asset"] == "option"],
    ["symbol", "interval", "sink", "size_mb", "days_span"],
)
_print_view(
    "2. InfluxDB only:",
    stats[stats["sink"] == "influxdb"],
    ["symbol", "asset", "interval", "size_mb"],
)
_print_view(
    "3. TLRY only:",
    stats[stats["symbol"] == "TLRY"],
    ["interval", "sink", "size_mb", "days_span"],
)
_print_view(
    "4. Tick data only:",
    stats[stats["interval"] == "tick"],
    ["symbol", "asset", "sink", "size_mb"],
)

log_console("\n" + "=" * 80)
log_console("STORAGE SUMMARY - AGGREGATED VIEW")
log_console("=" * 80)

summary = _build_summary(stats)

log_console("\nTOTAL STORAGE:")
total = summary["total"]
log_console(f"   Size: {total['size_gb']:.3f} GB ({total['size_mb']:.2f} MB)")
log_console(f"   Series: {total['series_count']}")

log_console("\nBY SINK:")
for sink, data in summary["by_sink"].items():
    log_console(
        f"   {sink:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  "
        f"[{data['series_count']} series]"
    )

log_console("\nBY ASSET:")
for asset, data in summary["by_asset"].items():
    log_console(
        f"   {asset:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  "
        f"[{data['series_count']} series]"
    )

log_console("\nBY INTERVAL:")
for interval, data in summary["by_interval"].items():
    log_console(
        f"   {interval:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  "
        f"[{data['series_count']} series]"
    )

log_console("\nTOP 10 SYMBOLS BY SIZE:")
for i, symbol_data in enumerate(summary["top_symbols"], 1):
    symbol = symbol_data["symbol"]
    size_gb = symbol_data["size_gb"]
    pct = symbol_data["percentage"]
    log_console(f"   {i:2d}. {symbol:8s}: {size_gb:8.3f} GB  ({pct:5.1f}%)")

log_console("\n" + "=" * 80)
