#!/usr/bin/env python3
"""
Script to systematically update ALL docstrings in manager.py to follow the required structure.
This script will update docstrings for methods that are missing them or have incomplete ones.
"""

import re
from pathlib import Path

# Docstring templates for different types of methods
DOCSTRING_TEMPLATES = {
    "_write_df_to_sink": '''"""Writes a DataFrame to the configured sink (CSV, Parquet, or InfluxDB).

        This method handles the persistence of data to different storage backends, routing the DataFrame
        to the appropriate writer based on the sink type specified in the base_path.

        Parameters
        ----------
        base_path : str
            The full file path (for CSV/Parquet) or measurement name (for InfluxDB) where data should be written.
        df : pandas.DataFrame
            The DataFrame containing market data to persist.
        sink : str
            The sink type ('csv', 'parquet', or 'influxdb').

        Returns
        -------
        None
            Data is written to the specified sink but no value is returned.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _download_and_store_options() after fetching and processing option data
        # - _download_and_store_equity_or_index() after fetching stock/index data
        """''',

    "_append_parquet_df": '''"""Appends a DataFrame to an existing Parquet file with deduplication and file rotation.

        This method reads the existing Parquet file (if it exists), concatenates the new data, removes
        duplicates, and writes the result back. If the file exceeds the configured size cap, it creates
        a new part file to maintain manageable file sizes.

        Parameters
        ----------
        base_path : str
            The full file path to the Parquet file (without _partNN suffix).
        df_new : pandas.DataFrame
            The new DataFrame to append to the existing Parquet file.

        Returns
        -------
        int
            The number of rows written to the Parquet file after deduplication.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _write_df_to_sink() when sink='parquet'
        # - Methods that need to append data to Parquet files with rotation support
        """''',

    "_extract_days_from_df": '''"""Extracts the earliest and latest dates from a DataFrame's timestamp column.

        This helper method scans a DataFrame for timestamp information and returns the date range covered
        by the data, which is useful for logging and determining file naming.

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame containing a timestamp column.
        fallback_day_iso : str
            A fallback date in 'YYYY-MM-DD' format to use if timestamps cannot be extracted.

        Returns
        -------
        tuple of (str, str)
            A tuple containing (first_date, last_date) in 'YYYY-MM-DD' format.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _download_and_store_equity_or_index() to determine the date range of downloaded data
        # - Methods that need to extract date information from DataFrames for logging
        """''',

    "_resolve_first_date": '''"""Resolves the first available date for a symbol using overrides, cache, or discovery.

        This method determines the starting point for data synchronization by checking (in order):
        1) task.first_date_override if specified
        2) cached first date from previous discovery
        3) fresh discovery using the configured discovery policy

        Parameters
        ----------
        task : Task
            The task configuration containing asset type, discover policy, and optional first_date_override.
        symbol : str
            The ticker symbol or root symbol to resolve the first date for.

        Returns
        -------
        str or None
            The first available date in 'YYYY-MM-DD' or 'YYYYMMDD' format, or None if not determinable.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - run() for each symbol before starting synchronization
        # - Methods that need to determine where to start downloading historical data
        """''',

    "_discover_equity_first_date": '''"""Discovers the first available date for stock or index data using API probing or binary search.

        This method attempts to find the earliest date with available data by making strategic API calls.
        The discovery strategy depends on the configured policy: either probing known historical dates
        or performing a binary search within a date range.

        Parameters
        ----------
        symbol : str
            The stock or index ticker symbol.
        req_type : str
            The request type ('trade', 'quote', etc.) to discover data for.
        policy : DiscoverPolicy or None
            The discovery policy configuration specifying the search strategy and date bounds.

        Returns
        -------
        str or None
            The first available date in 'YYYY-MM-DD' format, or None if discovery fails.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _resolve_first_date() when asset type is 'stock' or 'index' and discovery is needed
        """''',

    "_discover_option_first_date": '''"""Discovers the first available date for option data using expiration-based probing or binary search.

        For options, first-date discovery is more complex because it requires finding days when contracts
        actually traded. This method uses the configured discovery policy to efficiently locate the
        earliest available option data.

        Parameters
        ----------
        symbol : str
            The underlying ticker root symbol for options.
        req_type : str
            The request type ('trade', 'quote') to discover option data for.
        policy : DiscoverPolicy or None
            The discovery policy configuration specifying the search strategy and date bounds.

        Returns
        -------
        str or None
            The first available date in 'YYYY-MM-DD' format, or None if discovery fails.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _resolve_first_date() when asset type is 'option' and discovery is needed
        """''',

    "_binary_search_first_date_option": '''"""Performs a binary search to find the first date with available option data within a date range.

        This method efficiently narrows down the first available date by repeatedly bisecting the search
        space, checking if data exists at the midpoint, and adjusting the search bounds accordingly.

        Parameters
        ----------
        symbol : str
            The underlying ticker root symbol for options.
        req_type : str
            The request type ('trade' or 'quote') to search for.
        start_date : date
            The earliest date to consider in the search (inclusive).
        end_date : date
            The latest date to consider in the search (inclusive).

        Returns
        -------
        str or None
            The first date with available data in 'YYYY-MM-DD' format, or None if no data found in range.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _discover_option_first_date() when using binary search discovery mode
        """''',

    "_next_part_path": '''"""Generates the next part file path by incrementing the part number suffix.

        When a file grows beyond the size cap, this method creates a new filename with an incremented
        _partNN suffix to enable file rotation while maintaining chronological order.

        Parameters
        ----------
        path : str
            The current file path, which may or may not have a _partNN suffix.
        ext : str
            The file extension (e.g., 'csv', 'parquet').

        Returns
        -------
        str
            The next part file path with an incremented suffix (e.g., _part02, _part03).

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _ensure_under_cap() when a file exceeds the size limit
        # - _append_parquet_df() for Parquet file rotation
        """''',

    "_cache_key": '''"""Constructs a cache key string for first-date coverage entries.

        This method creates a standardized key format for storing and retrieving first-date information
        in the coverage cache dictionary.

        Parameters
        ----------
        asset : str
            The asset type (e.g., 'stock', 'option', 'index').
        symbol : str
            The ticker symbol or root symbol.
        interval : str
            The bar interval (e.g., '1d', '5m').
        sink : str
            The sink type (e.g., 'csv', 'parquet').

        Returns
        -------
        str
            A formatted cache key string combining all parameters.

        Example Usage
        -------------
        # This is an internal helper method called by:
        # - _load_cache_file() and _save_cache_file() for cache operations
        # - clear_first_date_cache() to delete specific cache entries
        # - _touch_cache() to update cache entries
        """''',
}

def update_manager_docstrings():
    """Update all docstrings in manager.py"""
    manager_path = Path(__file__).parent / "src" / "tdSynchManager" / "manager.py"

    print(f"Reading {manager_path}...")
    with open(manager_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Apply template docstrings
    for method_name, docstring in DOCSTRING_TEMPLATES.items():
        # Find the method definition and its current docstring
        pattern = rf'(    def {re.escape(method_name)}\([^)]*\)[^:]*:)\s*(?:"""[^"]*(?:"(?!"")|[^"])*?""")?'

        # Replace with new docstring
        def replacer(match):
            return f'{match.group(1)}\n        {docstring}'

        content = re.sub(pattern, replacer, content, flags=re.DOTALL)

    # Write back
    print(f"Writing updated content back to {manager_path}...")
    with open(manager_path, 'w', encoding='utf-8') as f:
        f.write(content)

    print("Done! Updated docstrings for key methods.")

if __name__ == "__main__":
    update_manager_docstrings()
