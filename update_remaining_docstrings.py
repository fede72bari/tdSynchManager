#!/usr/bin/env python3
"""
Script to update all remaining docstrings in client.py to follow the required structure.
This handles the large number of option and index methods efficiently.
"""

import re
from typing import Dict

# File path
FILE_PATH = r"d:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src\tdSynchManager\client.py"

# Define all docstring replacements
# Format: (old_docstring_pattern, new_docstring)
DOCSTRING_UPDATES = [
    # option_list_strikes
    (
        r'async def option_list_strikes\(self, symbol: str, expiration: str,\s+format_type:.*?\) -> Tuple\[Any, str\]:\s+""".*?"""',
        '''async def option_list_strikes(self, symbol: str, expiration: str,
                                 format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """Retrieve all available strike prices for a specific underlying symbol and expiration date.

        This method queries the ThetaData API to obtain the complete list of strike prices for which
        option contracts exist at a given expiration date. The strike prices represent all the exercise
        prices available in the option chain for that expiration, typically ranging from deep out-of-the-money
        to deep in-the-money relative to the current underlying price. This endpoint is essential for
        constructing complete option chains, analyzing strike distribution, identifying arbitrage
        opportunities, and building comprehensive option trading strategies that require knowledge of
        all available strikes.

        Parameters
        ----------
        symbol : str
            The underlying stock symbol for which to retrieve strike prices. Examples: "AAPL"
            (Apple Inc.), "SPY" (S&P 500 ETF), "GOOGL" (Alphabet Inc.). Must be a valid symbol
            with option data available in the ThetaData database. Use option_list_symbols() to
            discover available underlying symbols.
        expiration : str
            The option expiration date for which to retrieve strikes. Format must be "YYYY-MM-DD"
            (e.g., "2024-03-15") or "YYYYMMDD" (e.g., "20240315"). This specifies the option
            series whose strikes you want to retrieve. Use option_list_expirations() to discover
            available expiration dates.
        format_type : str, optional
            Default: "json"
            Possible values: "csv", "json", "ndjson"
            The desired response format for the strike data:
            - "json": Returns data as a JSON array of numeric strike values, ideal for programmatic use
            - "csv": Returns strikes as comma-separated values
            - "ndjson": Returns strikes as newline-delimited JSON

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The list of strike prices. For JSON format, this is a list of float
              values representing dollar strike prices (e.g., [140.0, 145.0, 150.0, 155.0, 160.0]).
              For CSV/NDJSON, this is a formatted string. Strikes are sorted numerically.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get all strikes for AAPL March 2024 options
        async with ThetaDataV3Client() as client:
            strikes, url = await client.option_list_strikes(
                symbol="AAPL",
                expiration="2024-03-15"
            )
            print(f"Found {len(strikes)} strike prices")
            print(f"Strike range: ${min(strikes)} to ${max(strikes)}")
            print(f"Strike spacing: ${strikes[1] - strikes[0]}")
        """'''
    ),

    # option_list_contracts
    (
        r'async def option_list_contracts\(\s+self,\s+request_type:.*?"""',
        '''async def option_list_contracts(
        self,
        request_type: Literal["trade", "quote"],
        date: str,
        symbol: str,
        format_type: Optional[Literal["csv", "json", "ndjson"]] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve a complete list of all option contracts that were active on a specific trading date.

        This method queries the ThetaData API to obtain a comprehensive inventory of all option
        contracts (calls and puts across all strikes and expirations) that had trade or quote activity
        on the specified date. This endpoint is invaluable for discovering which specific option
        contracts were liquid and traded on historical dates, building universes of tradable options
        for backtesting, analyzing option market breadth, and identifying which contracts had market
        activity. Results can be filtered by underlying symbol to focus on specific stocks or ETFs.

        Parameters
        ----------
        request_type : str
            Possible values: "trade", "quote"
            The type of market activity to filter contracts by:
            - "trade": Returns contracts that had at least one trade execution on the specified date
            - "quote": Returns contracts that had bid/ask quotes available on the specified date
            Trade data typically represents actual liquidity, while quote data shows theoretical
            market availability.
        date : str
            The specific trading date to query. Recommended format is "YYYYMMDD" (e.g., "20240115"),
            though "YYYY-MM-DD" may also be accepted. Only contracts active on this exact date will
            be returned.
        symbol : str
            The underlying symbol to filter contracts by. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). This can also accept a list/tuple of symbols, which
            will be sent as a comma-separated list to query multiple underlyings. If omitted or None,
            the endpoint returns contracts for all available underlyings on that date (potentially
            very large dataset).
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format for the contract data:
            - "csv": Returns data as comma-separated values (API default), suitable for spreadsheets
            - "json": Returns data as a JSON array of contract objects, ideal for programmatic use
            - "ndjson": Returns data as newline-delimited JSON, useful for streaming large datasets

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The contract data. For JSON/NDJSON, this is a list of dictionaries where
              each dictionary represents one option contract with fields: contract_symbol, underlying,
              expiration (YYYY-MM-DD), strike, right (C/P). For CSV, this is a formatted string.
              Large queries (all symbols) may return tens of thousands of contracts.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get all AAPL option contracts that traded on January 15, 2024
        async with ThetaDataV3Client() as client:
            contracts, url = await client.option_list_contracts(
                request_type="trade",
                date="20240115",
                symbol="AAPL",
                format_type="json"
            )
            print(f"Found {len(contracts)} AAPL contracts with trades")
            calls = [c for c in contracts if c['right'] == 'C']
            puts = [c for c in contracts if c['right'] == 'P']
            print(f"Calls: {len(calls)}, Puts: {len(puts)}")

        # Get contracts for multiple symbols
        contracts, url = await client.option_list_contracts(
            request_type="quote",
            date="20240115",
            symbol=["SPY", "QQQ", "IWM"],
            format_type="json"
        )
        """'''
    ),
]

def update_docstrings():
    """Read the file and update all docstrings"""
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    for pattern, replacement in DOCSTRING_UPDATES:
        content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    if content != original_content:
        with open(FILE_PATH, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated {len(DOCSTRING_UPDATES)} docstrings")
        return True
    else:
        print("No changes made")
        return False

if __name__ == "__main__":
    update_docstrings()
