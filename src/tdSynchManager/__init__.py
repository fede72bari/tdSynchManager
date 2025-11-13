# -*- coding: utf-8 -*-
"""
tdSynchManager package initializer.

Clean public surface:
    from tdSynchManager import (
        ThetaSyncManager, ThetaDataV3Client, ManagerConfig,
        build_default_client, new_manager, __version__
    )

Note: This file is unrelated to class constructors (__init__ methods).
"""

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
from .manager import ThetaSyncManager, build_default_client
from .config import ManagerConfig

__all__ = [
    "ThetaDataV3Client",
    "ThetaSyncManager",
    "ManagerConfig",
    "build_default_client",
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
