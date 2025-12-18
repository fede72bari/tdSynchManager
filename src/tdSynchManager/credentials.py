"""
Utility to load credentials from .credentials.json file.

This module provides a simple way to load credentials without hardcoding them.
The .credentials.json file is excluded from git via .gitignore.
"""
import json
import os
from pathlib import Path
from typing import Dict, Any


def get_project_root() -> Path:
    """Get the project root directory (where .credentials.json is located)."""
    # Start from this file's directory and go up to project root
    current = Path(__file__).parent
    while current != current.parent:
        if (current / '.credentials.json').exists():
            return current
        if (current / 'src').exists() and (current / 'src' / 'tdSynchManager').exists():
            return current
        current = current.parent

    # Fallback: assume we're in src/tdSynchManager, go up 2 levels
    return Path(__file__).parent.parent.parent


def load_credentials(credentials_file: str = '.credentials.json') -> Dict[str, Any]:
    """
    Load credentials from JSON file.

    Parameters
    ----------
    credentials_file : str, optional
        Name of credentials file (default: '.credentials.json')

    Returns
    -------
    dict
        Dictionary with credentials. Structure:
        {
            'influxdb': {
                'token': str,
                'url': str,
                'bucket': str,
                'org': str or None
            },
            'thetadata': {
                'host': str,
                'port': int,
                'api_key': str or None
            }
        }

    Raises
    ------
    FileNotFoundError
        If credentials file doesn't exist
    ValueError
        If credentials file is invalid JSON

    Example
    -------
    >>> creds = load_credentials()
    >>> influx_token = creds['influxdb']['token']
    >>> influx_url = creds['influxdb']['url']
    """
    project_root = get_project_root()
    creds_path = project_root / credentials_file

    if not creds_path.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {creds_path}\n"
            f"Please create it by copying .credentials.json.example:\n"
            f"  cp .credentials.json.example .credentials.json\n"
            f"Then edit .credentials.json with your actual credentials."
        )

    try:
        with open(creds_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in credentials file: {e}")


def get_influx_credentials() -> Dict[str, Any]:
    """
    Get InfluxDB credentials.

    Returns
    -------
    dict
        InfluxDB credentials with keys: token, url, bucket, org

    Example
    -------
    >>> influx = get_influx_credentials()
    >>> token = influx['token']
    >>> url = influx['url']
    """
    creds = load_credentials()
    return creds.get('influxdb', {})


def get_thetadata_credentials() -> Dict[str, Any]:
    """
    Get ThetaData credentials.

    Returns
    -------
    dict
        ThetaData credentials with keys: host, port, api_key

    Example
    -------
    >>> theta = get_thetadata_credentials()
    >>> host = theta['host']
    >>> port = theta['port']
    """
    creds = load_credentials()
    return creds.get('thetadata', {})
