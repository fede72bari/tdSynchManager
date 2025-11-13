# -*- coding: utf-8 -*-
"""
tdSynchManager package initializer.

- Provides a clean import surface:
    from tdSynchManager import ThetaSyncManager, ThetaDataV3Client, ManagerConfig, build_default_client
- Unrelated to class constructors (__init__ of classes).
"""

# === >>> VERSION METADATA — BEGIN
try:
    from importlib.metadata import version, PackageNotFoundError
    try:
        # Try both the lowercase distribution name and the CamelCase fallback.
        __version__ = version("tdsynchmanager")
    except PackageNotFoundError:
        __version__ = version("tdSynchManager")
except Exception:
    __version__ = "0.0.0"
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
    "__version__",
]
# === >>> PUBLIC API RE-EXPORTS — END

# === >>> OPTIONAL CONVENIENCE FACTORY — BEGIN
def new_manager(cfg: "ManagerConfig | None" = None,
                client: "ThetaDataV3Client | None" = None) -> ThetaSyncManager:
    """Instantiate a ThetaSyncManager from the package root."""
    return ThetaSyncManager(cfg=cfg, client=client)
# === >>> OPTIONAL CONVENIENCE FACTORY — END
