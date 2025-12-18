"""
Compatibility shim: ThetaDataV3Client moved to src/clients.
This module re-exports the same public symbols for backwards compatibility.
"""
from clients.ThetaDataV3Client import Interval, ThetaDataV3Client, ResilientThetaClient, ThetaDataV3HTTPError

__all__ = [
    "Interval",
    "ThetaDataV3Client",
    "ResilientThetaClient",
    "ThetaDataV3HTTPError",
]
