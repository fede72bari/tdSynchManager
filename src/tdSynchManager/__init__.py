# -*- coding: utf-8 -*-
"""
tdSynchManager package initializer.

Clean public surface:
    from tdSynchManager import (
        ThetaSyncManager, ThetaDataV3Client, ManagerConfig,
        new_manager, __version__
    )

Note: This file is unrelated to class constructors (__init__ methods).
"""

# Ensure every log line (console/text) carries a timestamp
from .logging_utils import install_timestamped_print

install_timestamped_print()

# === >>> VERSION METADATA — BEGIN
def _detect_version() -> str:
    try:
        from importlib.metadata import version, PackageNotFoundError
        try:
            # Prefer distribution-name in lowercase (recommended in pyproject)
            return version("tdsynchmanager")
        except PackageNotFoundError:
            # Fallback if you published with CamelCase (not recommended)
            return version("tdSynchManager")
    except Exception:
        return "0.0.0"

__version__ = _detect_version()
# === >>> VERSION METADATA — END

# === >>> PUBLIC API RE-EXPORTS — BEGIN
from .client import ThetaDataV3Client
from .manager import ThetaSyncManager
from .config import ManagerConfig
from .tick_bucket_analysis import analyze_tick_buckets, DayBucketAnalysisReport, BucketAnalysisResult

__all__ = [
    "ThetaDataV3Client",
    "ThetaSyncManager",
    "ManagerConfig",
    "analyze_tick_buckets",
    "DayBucketAnalysisReport",
    "BucketAnalysisResult",
    "new_manager",
    "__version__",
]
# === >>> PUBLIC API RE-EXPORTS — END

# === >>> OPTIONAL CONVENIENCE FACTORY — BEGIN
def new_manager(cfg: "ManagerConfig | None" = None,
                client: "ThetaDataV3Client | None" = None) -> ThetaSyncManager:
    """Instantiate a ThetaSyncManager from the package root."""
    return ThetaSyncManager(cfg=cfg, client=client)
# === >>> OPTIONAL CONVENIENCE FACTORY — END
