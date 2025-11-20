from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, Optional, Tuple, Literal
from urllib.parse import urlencode

import aiohttp


Interval = Literal[
    "tick", "10ms", "100ms", "500ms",
    "1s", "5s", "10s", "15s", "30s",
    "1m", "5m", "10m", "15m", "30m", "1h", "1d"
]

class ThetaDataV3HTTPError(Exception):
    """Custom exception for ThetaData v3 HTTP errors."""

    def __init__(self, status: int, url: str, raw_response: str, original_exc=None):
        """
        Initialize a ThetaDataV3HTTPError exception instance.

        This exception is raised when HTTP requests to the ThetaData v3 API fail, either due to
        HTTP error status codes (4xx, 5xx) or network/timeout issues. It captures comprehensive
        error context including the HTTP status code, URL, raw response text, and the original
        exception if available. The exception message includes a truncated version of the response
        (first 1000 characters) for debugging purposes.

        Parameters:
            status (int): HTTP status code returned by the server. Use 0 to indicate network-level
                errors such as connection timeouts or network failures where no HTTP status was received.
                Typical values: 200-299 (success, though not used for exceptions), 400 (Bad Request),
                401 (Unauthorized), 404 (Not Found), 500 (Internal Server Error), 503 (Service Unavailable).
            url (str): The complete URL of the failed request, including query parameters. This helps
                identify which endpoint and with what parameters the error occurred.
            raw_response (str): The raw response text returned by the server. For network errors,
                this contains the string representation of the error. For HTTP errors, this contains
                the response body which may include error details or messages from the API.
            original_exc (Exception, optional): The original exception object that triggered this error,
                if applicable. Defaults to None. This is typically set for network-level errors
                (aiohttp.ClientError, asyncio.TimeoutError) and allows for exception chaining and
                detailed debugging of the root cause.

        Example Usage:
            This constructor is called internally by the _make_request method when HTTP requests fail.
            It should not be called directly by users of the ThetaDataV3Client class.
        """
        self.status = status
        self.url = url
        self.raw = raw_response
        self.original_exc = original_exc
        super().__init__(
            f"HTTP error {status}: {url}\n"
            f"Response (truncated): {raw_response[:1000]}"
        )


class ThetaDataV3Client:


    # =========================================================================
    # (BEGIN)
    # CLIENT INFRASTRUCTURE
    # =========================================================================
    def __init__(self, base_url: str = None, data_dir: str = "./data",
                 max_concurrent_requests: int = 10,
                 timeout_total: float = 300.0,
                 timeout_connect: float = 10.0,
                 timeout_sock_read: float = 120.0):
        """
        Initialize a ThetaData v3 API client for accessing market data.

        This client provides asynchronous access to the ThetaData v3 REST API, which delivers
        comprehensive historical and real-time market data for stocks, options, and indices.
        The client manages HTTP session state, connection pooling, timeout configurations, and
        request concurrency limits. It is designed to work with a locally running Theta Terminal
        instance or a remote ThetaData API server. The client automatically creates the specified
        data directory if it doesn't exist.

        Parameters:
            base_url (str, optional): The base URL for the ThetaData v3 API endpoint. Defaults to None,
                which automatically uses "http://localhost:25503/v3" (the default Theta Terminal local
                server). When connecting to a remote server or custom port, provide the full base URL
                including protocol and version path (e.g., "http://192.168.1.100:25503/v3").
            data_dir (str, optional): Directory path where downloaded data files will be stored.
                Defaults to "./data". The directory is created automatically if it doesn't exist.
                Use absolute paths for production deployments to avoid ambiguity.
            max_concurrent_requests (int, optional): Maximum number of concurrent HTTP requests allowed
                to the API server. Defaults to 10. This limit prevents overwhelming the server and helps
                manage client-side resource usage. Adjust based on server capacity and network bandwidth.
                Lower values (5-10) are safer for shared servers; higher values (20-50) may be used for
                dedicated servers with high throughput.
            timeout_total (float, optional): Total timeout in seconds for the entire request lifecycle,
                including connection establishment, request sending, and response reading. Defaults to
                300.0 (5 minutes). Use higher values for large data requests that may take time to process.
            timeout_connect (float, optional): Connection timeout in seconds for establishing the initial
                TCP connection to the server. Defaults to 10.0 seconds. If connection attempts frequently
                timeout, check network connectivity or increase this value.
            timeout_sock_read (float, optional): Socket read timeout in seconds for reading data from an
                established connection. Defaults to 120.0 (2 minutes). This should be set based on expected
                response times for large datasets. Increase for queries returning millions of records.

        Example Usage:
            Basic usage with default local server:
            ```python
            async with ThetaDataV3Client() as client:
                data, url = await client.stock_snapshot_ohlc("AAPL")
            ```

            Custom configuration for remote server:
            ```python
            async with ThetaDataV3Client(
                base_url="http://api.example.com:8080/v3",
                data_dir="/var/data/market",
                max_concurrent_requests=20,
                timeout_total=600.0
            ) as client:
                data, url = await client.option_list_symbols()
            ```
        """
        self.base_url = base_url or "http://localhost:25503/v3"
        self.data_dir = data_dir
        self.session = None
        self.max_concurrent_requests = max_concurrent_requests
        self._timeout = aiohttp.ClientTimeout(
            total=timeout_total,
            connect=timeout_connect,
            sock_read=timeout_sock_read
        )
        os.makedirs(data_dir, exist_ok=True)

    async def __aenter__(self):
        """Enter the async context manager by initializing the HTTP session.

        This special method is automatically called when entering an async context manager block
        (i.e., when using 'async with ThetaDataV3Client() as client:'). It creates a new aiohttp
        ClientSession with the configured timeout settings, establishing the foundation for making
        HTTP requests to the ThetaData API. The session handles connection pooling, keep-alive
        connections, and automatic resource cleanup, ensuring efficient communication with the API
        server throughout the context's lifetime.

        Parameters
        ----------
        None

        Returns
        -------
        ThetaDataV3Client
            Returns self (the client instance) to allow usage in the 'as' clause of the async
            context manager. This enables direct access to all client methods within the context.

        Example Usage
        -------------
        # This method is called automatically when entering the context:
        async with ThetaDataV3Client() as client:
            # At this point, __aenter__ has been called and the session is ready
            data, url = await client.stock_snapshot_ohlc("AAPL")
        """
        self.session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager by cleaning up the HTTP session.

        This special method is automatically called when exiting an async context manager block,
        whether the exit is due to normal completion or an exception. It ensures proper cleanup
        by closing the aiohttp ClientSession, which releases all connection pool resources,
        terminates any pending connections, and performs graceful shutdown of HTTP communication.
        Proper cleanup prevents resource leaks and ensures all network resources are returned to
        the operating system.

        Parameters
        ----------
        exc_type : type, optional
            Default: None
            The type of exception that caused the context to exit, if any. None if the context
            exited normally without an exception.
        exc_val : Exception, optional
            Default: None
            The exception instance that caused the context to exit, if any. None if the context
            exited normally without an exception.
        exc_tb : traceback, optional
            Default: None
            The traceback object associated with the exception, if any. None if the context
            exited normally without an exception.

        Returns
        -------
        None
            This method does not return a value. Returning None (or not returning explicitly)
            means that any exception is propagated. If this method were to return True, it would
            suppress the exception.

        Example Usage
        -------------
        # This method is called automatically when exiting the context:
        async with ThetaDataV3Client() as client:
            data, url = await client.stock_snapshot_ohlc("AAPL")
        # At this point, __aexit__ has been called and the session is closed
        """
        if self.session:
            await self.session.close()

    async def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Any, str]:
        """
        Make an HTTP GET request to the ThetaData v3 API endpoint.

        This internal helper method handles the low-level HTTP communication with the ThetaData API.
        It constructs the full URL from the base URL and endpoint, sends the GET request with query
        parameters, processes the response based on content type, and handles errors uniformly. The
        method automatically detects JSON responses and parses them, or returns raw text for other
        content types (like CSV). All HTTP errors and network exceptions are converted to
        ThetaDataV3HTTPError for consistent error handling throughout the client.

        Parameters
        ----------
        endpoint : str
            The API endpoint path to request, starting with a forward slash.
            Examples: "/stock/list/symbols", "/option/history/trade", "/index/snapshot/price".
            This is appended to the base_url configured during client initialization.
        params : Dict[str, Any], optional
            Default: None

            Query parameters to include in the request URL. Common parameters include: "symbol",
            "date", "format", "start_date", "end_date", "interval", etc. The dictionary keys are
            parameter names and values are automatically URL-encoded.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The parsed response data. If the response Content-Type is
              "application/json", this is the parsed JSON object (dict, list, etc.). For CSV
              or other content types, this is the raw response text string. If JSON parsing
              fails despite JSON content type, returns the raw text as fallback.
            - Second element: The complete request URL as a string, including all query
              parameters. Useful for debugging, logging, and error reporting.

        Raises
        ------
        ThetaDataV3HTTPError
            Raised for any HTTP error (status codes outside 200-299 range) or network-level
            errors (connection failures, timeouts). The exception includes the status code
            (0 for network errors), full URL, and raw response text.

        Example Usage
        -------------
        This method is called internally by all public API methods such as:
        - stock_list_symbols() -> calls _make_request("/stock/list/symbols", ...)
        - stock_history_ohlc() -> calls _make_request("/stock/history/ohlc", ...)
        - option_snapshot_trade() -> calls _make_request("/option/snapshot/trade", ...)
        - index_history_price() -> calls _make_request("/index/history/price", ...)

        Users should not call this method directly; instead, use the public endpoint methods.
        """
        url = f"{self.base_url}{endpoint}"
        full_url = f"{url}?{urlencode(params or {})}"
        try:
            async with self.session.get(url, params=params, timeout=self._timeout) as response:
                raw_text = await response.text()
                if not (200 <= response.status < 300):
                    raise ThetaDataV3HTTPError(response.status, full_url, raw_text)
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' in content_type:
                    try:
                        data = await response.json()
                        return data, full_url
                    except json.JSONDecodeError:
                        return raw_text, full_url
                return raw_text, full_url

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            # 0 = errore di rete/timeout lato client
            raise ThetaDataV3HTTPError(0, full_url, str(e), e)



    # =========================================================================
    # (END)
    # CLIENT INFRASTRUCTURE
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # STOCK
    # =========================================================================

    # --- List (begin) ---

    async def stock_list_symbols(self, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve a comprehensive list of all available stock symbols in the ThetaData database.

        This method queries the ThetaData API to obtain all stock symbols for which data is available.
        The response includes detailed information about each symbol such as the ticker, company name,
        and exchange. This endpoint is essential for discovering supported tickers before requesting
        specific historical or real-time data. It returns the data in the requested format and provides
        the request URL for reference.

        Parameters
        ----------
        format_type : str, optional
            Default: "json"
            Possible values: ["csv", "json", "ndjson"]

            The desired response format for the data. The format choice affects how the returned
            data is structured and should be selected based on your processing requirements and
            tooling.
            - "json": Returns data as a JSON array of objects, easiest for programmatic parsing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet import
            - "ndjson": Returns newline-delimited JSON, useful for streaming large datasets

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The list of stock symbols. If format_type is "json", this is a list
              of dictionaries with symbol details. If "csv" or "ndjson", this is a string containing
              the formatted data.
            - Second element: The complete request URL as a string, including query parameters.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            symbols, url = await client.stock_list_symbols(format_type="json")
            print(f"Found {len(symbols)} stock symbols")
            for sym in symbols[:5]:
                print(sym)
        ```
        """
        params = {"format": format_type}
        return await self._make_request("/stock/list/symbols", params)
    
    async def stock_list_dates(self, symbol: str, data_type: Literal["trade", "quote"] = "trade",
                              format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve all available dates for which trade or quote data exists for a specific stock symbol.

        This method queries the ThetaData API to obtain a list of dates on which trade or quote data
        is available for the specified stock symbol. This is particularly useful for verifying data
        coverage and availability before making bulk historical data requests. All dates are returned
        in UTC timezone format. The response helps identify gaps in historical data and plan
        comprehensive data downloads efficiently.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples include "AAPL" (Apple Inc.),
            "MSFT" (Microsoft Corporation), "SPY" (S&P 500 ETF), "TSLA" (Tesla Inc.). The symbol
            must be a valid ticker available in the ThetaData database. Use stock_list_symbols()
            to discover available symbols.
        data_type : str
            Default: "trade"
            Possible values: ["trade", "quote"]

            The type of market data to check availability for.
            - "trade": Checks for trade (execution) data availability. Trade data includes
              actual transactions with price, size, timestamp, and exchange information.
            - "quote": Checks for quote (bid/ask) data availability. Quote data includes best
              bid and offer prices with sizes from the National Best Bid and Offer (NBBO).
        format_type : str, optional
            Default: "json"
            Possible values: ["csv", "json", "ndjson"]

            The desired response format.
            - "json": Returns dates as a JSON array, ideal for programmatic processing
            - "csv": Returns dates as comma-separated values
            - "ndjson": Returns dates as newline-delimited JSON

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: List of available dates. For JSON format, this is a list of date
              strings in YYYY-MM-DD format (UTC). For CSV/NDJSON, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            # Check trade data availability for Apple
            dates, url = await client.stock_list_dates("AAPL", data_type="trade")
            print(f"Trade data available for {len(dates)} dates")
            print(f"First date: {dates[0]}, Last date: {dates[-1]}")

            # Check quote data availability
            quote_dates, url = await client.stock_list_dates("SPY", data_type="quote")
        ```
        """
        params = {
            "symbol": symbol,
            "format": format_type
        }
        return await self._make_request(f"/stock/list/dates/{data_type}", params)
    
    

    # --- List (end) ---

    # --- Snapshot (begin) ---

    async def stock_snapshot_ohlc(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve the current OHLC (Open, High, Low, Close) snapshot for a specific stock symbol.

        This method fetches the most recent OHLC data available for the specified stock symbol from
        the ThetaData API. The snapshot represents either the current trading session if markets are
        open, or the most recent completed session if markets are closed. This real-time (or near
        real-time) data is essential for monitoring current market conditions, price levels, and
        intraday trading ranges. The response includes timestamp in UTC, volume, and other market
        metrics that provide a comprehensive view of current price action.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol available in the ThetaData
            database. Use stock_list_symbols() to discover available symbols.
        format_type : str, optional
            Default: "json"
            Possible values: ["csv", "json", "ndjson"]

            The desired response format.
            - "json": Returns data as a JSON object with structured fields, ideal for programmatic use
            - "csv": Returns data as comma-separated values, suitable for import to spreadsheets
            - "ndjson": Returns newline-delimited JSON format

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The OHLC snapshot data. For JSON format, this is a dictionary with
              fields like open, high, low, close, volume, timestamp. For CSV/NDJSON, this is a
              formatted string.
            - Second element: The complete request URL including query parameters.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            # Get current OHLC for Apple
            ohlc_data, url = await client.stock_snapshot_ohlc("AAPL")
            print(f"Current AAPL - Open: {ohlc_data['open']}, Close: {ohlc_data['close']}")

            # Get OHLC in CSV format
            ohlc_csv, url = await client.stock_snapshot_ohlc("SPY", format_type="csv")
        ```
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/stock/snapshot/ohlc", params)
    
    async def stock_snapshot_trade(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve the last executed trade snapshot for a specific stock symbol.

        This method fetches information about the most recent trade executed for the specified stock
        symbol from the ThetaData API. The snapshot provides critical real-time market data including
        the trade price, size (number of shares), timestamp in UTC timezone, exchange where the trade
        occurred, and trade conditions. This data is essential for real-time price discovery, understanding
        current market activity, and monitoring the latest transaction prices. The trade data represents
        actual executions rather than quotes, providing definitive evidence of price levels where
        transactions occurred.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "GOOGL"
            (Alphabet Inc.), "NVDA" (NVIDIA Corporation). Must be a valid symbol in the ThetaData
            database. Use stock_list_symbols() to find available symbols.
        format_type : str, optional
            Default: "json"
            Possible values: ["csv", "json", "ndjson"]

            The desired response format.
            - "json": Returns data as a JSON object with structured trade information
            - "csv": Returns data in comma-separated values format
            - "ndjson": Returns data in newline-delimited JSON format

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The last trade data. For JSON format, this is a dictionary with fields
              such as price, size, timestamp, exchange, conditions. For CSV/NDJSON, this is a string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            # Get last trade for Tesla
            trade_data, url = await client.stock_snapshot_trade("TSLA")
            print(f"Last trade: ${trade_data['price']} for {trade_data['size']} shares")
            print(f"Exchange: {trade_data['exchange']}")
        ```
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/stock/snapshot/trade", params)
    
    async def stock_snapshot_quote(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve the current quote snapshot (best bid and ask) for a specific stock symbol.

        This method fetches the most recent National Best Bid and Offer (NBBO) data for the specified
        stock symbol from the ThetaData API. The quote snapshot includes the current best bid price and
        size, best ask price and size, timestamp in UTC timezone, exchange information, and the current
        bid-ask spread. This data is critical for understanding current market liquidity, assessing the
        order book state, and determining the cost of immediate execution. Quote data differs from trade
        data as it represents available prices rather than completed transactions, providing insight into
        where buyers and sellers are willing to transact.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "AMZN"
            (Amazon.com Inc.), "META" (Meta Platforms Inc.). Must be a valid symbol available in the
            ThetaData database. Use stock_list_symbols() to discover available symbols.
        format_type : str, optional
            Default: "json"
            Possible values: ["csv", "json", "ndjson"]

            The desired response format.
            - "json": Returns data as a JSON object with structured quote fields
            - "csv": Returns data in comma-separated values format
            - "ndjson": Returns data in newline-delimited JSON format

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The quote data. For JSON format, this is a dictionary with fields such
              as bid_price, bid_size, ask_price, ask_size, spread, timestamp, exchange. For CSV/NDJSON,
              this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            # Get current quote for Microsoft
            quote_data, url = await client.stock_snapshot_quote("MSFT")
            spread = quote_data['ask_price'] - quote_data['bid_price']
            print(f"Bid: ${quote_data['bid_price']} x {quote_data['bid_size']}")
            print(f"Ask: ${quote_data['ask_price']} x {quote_data['ask_size']}")
            print(f"Spread: ${spread:.2f}")
        ```
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/stock/snapshot/quote", params)
    
    

    # --- Snapshot (end) ---

    # --- History (begin) ---

    async def stock_history_eod(self, symbol: str, start_date: str, end_date: str,
                               format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve historical end-of-day (EOD) data for a stock symbol over a specified date range.

        This method fetches daily historical market data for the specified stock symbol, providing a
        time series of end-of-day values including open, high, low, close prices, trading volume, and
        adjusted close prices. The data accounts for corporate actions such as stock splits and dividends,
        making it suitable for accurate historical analysis and backtesting. All timestamps are provided
        in UTC timezone. This endpoint is ideal for longer-term analysis, daily charting, and fundamental
        strategy backtesting where intraday granularity is not required.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "JPM" (JPMorgan Chase). Must be a valid symbol in the ThetaData database.
            Use stock_list_symbols() to discover available symbols.
        start_date : str
            The start date for the historical data range, inclusive. Format must be
            "YYYY-MM-DD" (e.g., "2024-01-01"). All dates are interpreted in UTC timezone. This is
            the first date for which data will be returned.
        end_date : str
            The end date for the historical data range, inclusive. Format must be
            "YYYY-MM-DD" (e.g., "2024-12-31"). All dates are interpreted in UTC timezone. This is
            the last date for which data will be returned.
        format_type : str, optional
            Default: "json"
            Possible values: ["csv", "json", "ndjson"]

            The desired response format.
            - "json": Returns data as a JSON array of daily records, ideal for programmatic processing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet import
            - "ndjson": Returns data as newline-delimited JSON, useful for streaming processing

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The historical EOD data. For JSON format, this is a list of dictionaries
              where each dictionary represents one trading day with fields: date, open, high, low,
              close, volume, adjusted_close. For CSV/NDJSON, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            # Get EOD data for Apple for all of 2024
            eod_data, url = await client.stock_history_eod(
                "AAPL",
                "2024-01-01",
                "2024-12-31"
            )
            print(f"Retrieved {len(eod_data)} trading days")
            for day in eod_data[:5]:
                print(f"{day['date']}: Close ${day['close']:.2f}, Volume {day['volume']}")
        ```
        """
        params = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "format": format_type
        }
        return await self._make_request("/stock/history/eod", params)
    
    async def stock_history_ohlc(
        self,
        symbol: str,
        date: str,
        interval: Interval,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        format_type: Optional[Literal["json", "csv", "ndjson"]] = "csv"
    ) -> Tuple[Any, str]:
        """
        Retrieve historical intraday OHLC (Open, High, Low, Close) bars for a stock symbol on a specific trading date.

        This method fetches intraday historical price data aggregated into OHLC bars at the specified
        interval for a single trading day. The data is aggregated from individual trades using Securities
        Information Processor (SIP) rules, ensuring consistency with industry standards. Each bar's
        timestamp represents the bar's open time, and trades are included in a bar if their timestamp
        falls within the half-open interval [open_time, open_time + interval). This data is essential
        for technical analysis, intraday charting, high-frequency strategy backtesting, and understanding
        price movements throughout a trading session.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol in the ThetaData database.
            Use stock_list_symbols() to discover available symbols.
        date : str
            The specific trading date for which to retrieve data. Acceptable formats are
            "YYYY-MM-DD" (e.g., "2024-01-15") or "YYYYMMDD" (e.g., "20240115"). Only data from this
            single trading date will be returned.
        interval : Interval
            The time interval for each OHLC bar. This determines the granularity of the data.
            Possible values include:
            - "tick": Tick-by-tick data (every trade)
            - Time intervals: "10ms", "100ms", "500ms" (millisecond bars)
            - Second intervals: "1s", "5s", "10s", "15s", "30s"
            - Minute intervals: "1m", "5m", "10m", "15m", "30m"
            - Hour intervals: "1h"
            - Daily: "1d"

            Smaller intervals provide more granularity but result in more data points.
        start_time : str, optional
            Default: None (server default 09:30:00)

            The start time within the trading day for data retrieval.
            Format is "HH:MM:SS" (e.g., "09:30:00" for market open, "10:00:00" for 10 AM).
            Times are in America/New_York (Eastern Time) timezone.
        end_time : str, optional
            Default: None (server default 16:00:00)

            The end time within the trading day for data retrieval.
            Format is "HH:MM:SS" (e.g., "16:00:00" for market close, "12:00:00" for noon).
            Times are in America/New_York (Eastern Time) timezone.
        format_type : str, optional
            Default: "csv"
            Possible values: ["json", "csv", "ndjson"]

            The desired response format.
            - "json": Returns data as a JSON array of bar objects
            - "csv": Returns data as comma-separated values
            - "ndjson": Returns data as newline-delimited JSON

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The OHLC bar data. For JSON format, this is a list of dictionaries
              where each dictionary represents one bar with fields: timestamp, open, high, low,
              close, volume, count. For CSV/NDJSON, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            # Get 5-minute bars for Apple on a specific date
            bars, url = await client.stock_history_ohlc(
                symbol="AAPL",
                date="2024-01-15",
                interval="5m",
                format_type="json"
            )
            print(f"Retrieved {len(bars)} 5-minute bars")

            # Get 1-minute bars for morning session only
            morning_bars, url = await client.stock_history_ohlc(
                symbol="SPY",
                date="2024-01-15",
                interval="1m",
                start_time="09:30:00",
                end_time="12:00:00"
            )
        ```
        """
        params = {
            "symbol": symbol,
            "date": date,
            "interval": interval,
            "format": format_type
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
            
        return await self._make_request("/stock/history/ohlc", params)
    
    
    async def stock_history_trade(
        self,
        symbol: str,
        date: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        venue: Optional[Literal["nqb", "utp_cta"]] = None,
        format_type: Optional[Literal["json", "csv", "ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """Retrieve historical tick-level trade data for a stock symbol on a specific trading date.

        This method fetches every individual trade execution that occurred for the specified stock
        symbol during a trading day, providing the most granular level of market data available.
        Each trade record includes the exact execution price, size (number of shares), timestamp
        with nanosecond precision, exchange identifier, trade conditions, and other execution details.
        This tick-level data is essential for microstructure analysis, high-frequency trading research,
        order flow studies, and understanding precise execution patterns throughout the trading day.
        Data can be filtered by venue to focus on specific market data feeds.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "TSLA" (Tesla Inc.),
            "SPY" (S&P 500 ETF). Must be a valid symbol available in the ThetaData database. Use
            stock_list_symbols() to discover available symbols.
        date : str
            The specific trading date for which to retrieve trade data. Acceptable formats are
            "YYYY-MM-DD" (e.g., "2024-01-15") or "YYYYMMDD" (e.g., "20240115"). Only trades from
            this single trading date will be returned.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day for trade data retrieval. Format is "HH:MM:SS"
            or "HH:MM:SS.SSS" with optional millisecond precision (e.g., "09:30:00", "10:00:00.500").
            Times are in America/New_York (Eastern Time) timezone. Only trades at or after this
            time will be included.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day for trade data retrieval. Format is "HH:MM:SS"
            or "HH:MM:SS.SSS" with optional millisecond precision (e.g., "16:00:00", "15:30:00.250").
            Times are in America/New_York (Eastern Time) timezone. Only trades before this time
            will be included.
        venue : str, optional
            Default: "nqb" (server default)
            Possible values: "nqb", "utp_cta"
            The market data venue/feed from which to retrieve trades:
            - "nqb": Nasdaq Quote Basic feed, includes trades from Nasdaq and other venues
            - "utp_cta": UTP/CTA consolidated feed, comprehensive market-wide trade data
            Venue selection affects which exchanges and trade reports are included in the results.
        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"
            The desired response format for the trade data:
            - "json": Returns data as a JSON array of trade objects, ideal for programmatic processing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet analysis
            - "ndjson": Returns newline-delimited JSON, useful for streaming large datasets

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The trade data. For JSON format, this is a list of dictionaries where
              each dictionary represents one trade with fields: timestamp, price, size, exchange,
              conditions, sequence. For CSV/NDJSON, this is a formatted string. Large date ranges
              may return millions of records.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get all trades for Apple on a specific date
        async with ThetaDataV3Client() as client:
            trades, url = await client.stock_history_trade(
                symbol="AAPL",
                date="2024-01-15"
            )
            print(f"Retrieved {len(trades)} trades")
            for trade in trades[:5]:
                print(f"{trade['timestamp']}: ${trade['price']} x {trade['size']}")

        # Get morning session trades only from Nasdaq feed
        trades, url = await client.stock_history_trade(
            symbol="TSLA",
            date="20240115",
            start_time="09:30:00",
            end_time="12:00:00",
            venue="nqb"
        )
        """
        
        params = {"symbol": symbol, "date": date, "format": format_type}
        
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if venue:
            params["venue"] = venue
            
        return await self._make_request("/stock/history/trade", params)


    
            
    async def stock_history_quote(
        self,
        symbol: str,
        date: str,
        interval: Literal["tick","10ms","100ms","500ms","1s","5s","10s","15s","30s","1m","5m","10m","15m","30m","1h"] = "1s",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        venue: Optional[Literal["nqb","utp_cta"]] = None,
        format_type: Optional[Literal["json","csv","ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """Retrieve historical National Best Bid and Offer (NBBO) quote data for a stock symbol on a specific date.

        This method fetches historical quote data showing the best bid and ask prices available
        across all exchanges at each point in time (or aggregated by interval) for the specified
        stock symbol during a trading day. NBBO quotes represent the tightest market spread and
        provide critical insight into market liquidity, order book depth, and execution quality.
        Data can be returned at tick-level granularity (every quote update) or aggregated into
        time intervals where only the last quote per interval is returned. This data is essential
        for market microstructure analysis, spread analysis, liquidity studies, and understanding
        the prevailing market conditions during historical trading periods.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "GOOGL" (Alphabet Inc.),
            "SPY" (S&P 500 ETF). Must be a valid symbol available in the ThetaData database. Use
            stock_list_symbols() to discover available symbols.
        date : str
            The specific trading date for which to retrieve quote data. Acceptable formats are
            "YYYY-MM-DD" (e.g., "2024-01-15") or "YYYYMMDD" (e.g., "20240115"). Only quotes from
            this single trading date will be returned.
        interval : str
            Default: "1s"
            Possible values: "tick", "10ms", "100ms", "500ms", "1s", "5s", "10s", "15s", "30s",
            "1m", "5m", "10m", "15m", "30m", "1h"
            The time interval for quote aggregation. This determines the granularity of the data:
            - "tick": Every single NBBO update (tick-level data, highest granularity)
            - Time intervals (10ms to 1h): Returns the last quote at the end of each interval
            Smaller intervals provide more granularity but result in more data points. Tick-level
            data can produce millions of records for actively traded symbols.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day for quote data retrieval. Format is "HH:MM:SS"
            or "HH:MM:SS.SSS" with optional millisecond precision (e.g., "09:30:00", "10:00:00.500").
            Times are in America/New_York (Eastern Time) timezone. Only quotes at or after this
            time will be included.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day for quote data retrieval. Format is "HH:MM:SS"
            or "HH:MM:SS.SSS" with optional millisecond precision (e.g., "16:00:00", "15:30:00.250").
            Times are in America/New_York (Eastern Time) timezone. Only quotes before this time
            will be included.
        venue : str, optional
            Default: "nqb" (server default)
            Possible values: "nqb", "utp_cta"
            The market data venue/feed from which to retrieve quotes:
            - "nqb": Nasdaq Quote Basic feed, suitable for current-day data and Nasdaq markets
              (requires stocks standard/pro subscription)
            - "utp_cta": UTP/CTA consolidated Security Information Processor (SIP) feeds,
              provides comprehensive market-wide NBBO from all exchanges
            Venue selection affects the source and completeness of quote data.
        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"
            The desired response format for the quote data:
            - "json": Returns data as a JSON array of quote objects, ideal for programmatic processing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet analysis
            - "ndjson": Returns newline-delimited JSON, useful for streaming large datasets

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The quote data. For JSON format, this is a list of dictionaries where
              each dictionary represents one quote with fields: timestamp, bid_price, bid_size,
              ask_price, ask_size, bid_exchange, ask_exchange, conditions. For CSV/NDJSON, this is
              a formatted string. Tick-level data may return millions of records.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get 1-second aggregated quotes for Apple on a specific date
        async with ThetaDataV3Client() as client:
            quotes, url = await client.stock_history_quote(
                symbol="AAPL",
                date="2024-01-15",
                interval="1s"
            )
            print(f"Retrieved {len(quotes)} 1-second quote snapshots")
            for quote in quotes[:5]:
                spread = quote['ask_price'] - quote['bid_price']
                print(f"{quote['timestamp']}: Bid ${quote['bid_price']} Ask ${quote['ask_price']} Spread ${spread:.4f}")

        # Get tick-level quotes for morning session only
        tick_quotes, url = await client.stock_history_quote(
            symbol="TSLA",
            date="20240115",
            interval="tick",
            start_time="09:30:00",
            end_time="10:30:00",
            venue="utp_cta"
        )
        """
        
        params = {
            "symbol": symbol,
            "date": date,
            "interval": interval,
            "format": format_type,
        }
        
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if venue:
            params["venue"] = venue
            
        return await self._make_request("/stock/history/quote", params)
    
    
    async def stock_history_trade_quote(
        self,
        symbol: str,
        date: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        exclusive: Optional[bool] = True,
        venue: Optional[Literal["nqb","utp_cta"]] = None,
        format_type: Optional[Literal["json","csv","ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """Retrieve historical trade data paired with corresponding quote data for a stock symbol on a specific date.

        This method fetches tick-level historical data where each trade execution is paired with the
        prevailing National Best Bid and Offer (NBBO) quote at the time of the trade. This pairing
        provides comprehensive market context for each transaction, enabling analysis of execution
        quality, effective spreads, price improvement, and the relationship between trades and the
        order book state. Each record includes both the trade details (price, size, timestamp, exchange)
        and the corresponding quote details (bid/ask prices and sizes). The exclusive parameter controls
        whether quotes with timestamps exactly equal to trade timestamps are included or excluded,
        affecting the temporal matching logic.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "MSFT" (Microsoft Corp.),
            "SPY" (S&P 500 ETF). Must be a valid symbol available in the ThetaData database. Use
            stock_list_symbols() to discover available symbols.
        date : str
            The specific trading date for which to retrieve trade-quote pairs. Acceptable formats are
            "YYYY-MM-DD" (e.g., "2024-01-15") or "YYYYMMDD" (e.g., "20240115"). Only data from this
            single trading date will be returned.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day for data retrieval. Format is "HH:MM:SS" or
            "HH:MM:SS.SSS" with optional millisecond precision (e.g., "09:30:00", "10:00:00.500").
            Times are in America/New_York (Eastern Time) timezone. Only trade-quote pairs at or
            after this time will be included.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day for data retrieval. Format is "HH:MM:SS" or
            "HH:MM:SS.SSS" with optional millisecond precision (e.g., "16:00:00", "15:30:00.250").
            Times are in America/New_York (Eastern Time) timezone. Only trade-quote pairs before
            this time will be included.
        exclusive : bool, optional
            Default: True (server default)
            Controls the temporal matching behavior for pairing quotes with trades:
            - True: Match quotes with timestamp strictly less than (<) the trade timestamp. This
              ensures the quote existed before the trade and represents the market state immediately
              prior to execution.
            - False: Match quotes with timestamp less than or equal to (<=) the trade timestamp.
              This allows quotes with the exact same timestamp as the trade to be matched.
            The choice affects execution quality analysis and spread calculations.
        venue : str, optional
            Default: "nqb" (server default)
            Possible values: "nqb", "utp_cta"
            The market data venue/feed from which to retrieve data:
            - "nqb": Nasdaq Quote Basic feed, includes data from Nasdaq and other venues
            - "utp_cta": UTP/CTA consolidated Security Information Processor (SIP) feeds,
              provides comprehensive market-wide data from all exchanges
            Venue selection affects the source and completeness of both trade and quote data.
        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"
            The desired response format for the trade-quote data:
            - "json": Returns data as a JSON array of objects, ideal for programmatic processing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet analysis
            - "ndjson": Returns newline-delimited JSON, useful for streaming large datasets

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The trade-quote paired data. For JSON format, this is a list of
              dictionaries where each dictionary contains both trade fields (trade_price, trade_size,
              trade_timestamp, trade_exchange) and quote fields (bid_price, bid_size, ask_price,
              ask_size, quote_timestamp). For CSV/NDJSON, this is a formatted string. Large date
              ranges may return millions of records.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get all trade-quote pairs for Apple on a specific date
        async with ThetaDataV3Client() as client:
            pairs, url = await client.stock_history_trade_quote(
                symbol="AAPL",
                date="2024-01-15"
            )
            print(f"Retrieved {len(pairs)} trade-quote pairs")
            for pair in pairs[:5]:
                mid_price = (pair['bid_price'] + pair['ask_price']) / 2
                trade_price = pair['trade_price']
                print(f"Trade: ${trade_price:.2f}, Mid: ${mid_price:.2f}, Spread: ${pair['ask_price'] - pair['bid_price']:.4f}")

        # Get morning session data with inclusive timestamp matching
        pairs, url = await client.stock_history_trade_quote(
            symbol="TSLA",
            date="20240115",
            start_time="09:30:00",
            end_time="12:00:00",
            exclusive=False,
            venue="utp_cta"
        )
        """
        
        params = {
            "symbol": symbol,
            "date": date,
            "format": format_type,
            "exclusive": str(exclusive).lower() if exclusive is not None else None,
        }
        
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if venue:
            params["venue"] = venue
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        
        return await self._make_request("/stock/history/trade_quote", params)

        

    # async def stock_history_trade_quote(self, symbol: str, start_date: str, end_date: str,
    #                                    format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
    #     """
    #     Get combined historical trade-quote ticks for a stock symbol.
        
    #     Endpoint: GET /stock/history/trade_quote
    #     Reference: https://docs.thetadata.us/docs/rest-api-v3/stock/history/trade_quote
        
    #     Args:
    #         symbol (str): Stock symbol
    #         start_date (str): Start date in YYYY-MM-DD format (UTC)
    #         end_date (str): End date in YYYY-MM-DD format (UTC)
    #         format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
    #     Returns:
    #         Tuple[List[Dict] | str, str]: Combined trade-quote data and request URL
            
    #     Description:
    #         Returns combined records with each trade paired with the temporally closest
    #         bid/ask quote. All timestamps in UTC. Useful for market impact analysis
    #         and understanding the relationship between trades and prevailing quotes.
    #     """
    #     params = {
    #         "symbol": symbol,
    #         "start_date": start_date,
    #         "end_date": end_date,
    #         "format": format_type
    #     }
    #     return await self._make_request("/stock/history/trade_quote", params)

        
    
    

    # --- History (end) ---

    # --- At-Time (begin) ---

    async def stock_at_time_trade(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        venue: Optional[Literal["nqb","utp_cta"]] = None,
        format_type: Optional[Literal["json","csv","ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """Retrieve the last trade execution at a specific time-of-day across multiple trading dates.

        This method fetches the most recent trade that occurred at or before a specified time-of-day
        for each trading date in the specified date range. This time-synchronized query enables
        consistent temporal sampling across multiple days, which is essential for building aligned
        time series datasets, analyzing price behavior at specific intraday moments (e.g., market open,
        close, or lunch hour), conducting event studies with precise timing, and creating time-based
        market replays. The returned dataset contains one trade record per trading date, representing
        the market state at that exact moment each day.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "SPY" (S&P 500 ETF),
            "TSLA" (Tesla Inc.). Must be a valid symbol available in the ThetaData database. Use
            stock_list_symbols() to discover available symbols.
        start_date : str
            The start date of the date range, inclusive. Acceptable formats are "YYYY-MM-DD"
            (e.g., "2024-01-15") or "YYYYMMDD" (e.g., "20240115"). This is the first trading date
            for which the at-time trade will be retrieved.
        end_date : str
            The end date of the date range, inclusive. Acceptable formats are "YYYY-MM-DD"
            (e.g., "2024-12-31") or "YYYYMMDD" (e.g., "20241231"). This is the last trading date
            for which the at-time trade will be retrieved.
        time_of_day : str
            The specific time within each trading day to query. Format is "HH:MM:SS" or "HH:MM:SS.SSS"
            with optional millisecond precision (e.g., "09:30:00", "15:59:59.999", "12:00:00.500").
            Times are in America/New_York (Eastern Time) timezone. The method returns the last trade
            that occurred at or before this time on each date.
        venue : str, optional
            Default: "nqb" (server default)
            Possible values: "nqb", "utp_cta"
            The market data venue/feed from which to retrieve trades:
            - "nqb": Nasdaq Quote Basic feed, includes trades from Nasdaq and other venues
            - "utp_cta": UTP/CTA consolidated Security Information Processor (SIP) feeds,
              provides comprehensive market-wide trade data from all exchanges
            Venue selection affects which trade source is queried.
        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"
            The desired response format for the trade data:
            - "json": Returns data as a JSON array of trade objects, ideal for programmatic processing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet analysis
            - "ndjson": Returns newline-delimited JSON, useful for streaming large datasets

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The at-time trade data. For JSON format, this is a list of dictionaries
              where each dictionary represents the last trade at the specified time for one trading
              date, with fields: date, timestamp, price, size, exchange, conditions. For CSV/NDJSON,
              this is a formatted string. One record is returned per trading date in the range.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get the last trade at 10:00 AM for each day in January 2024
        async with ThetaDataV3Client() as client:
            trades, url = await client.stock_at_time_trade(
                symbol="SPY",
                start_date="2024-01-01",
                end_date="2024-01-31",
                time_of_day="10:00:00"
            )
            print(f"Retrieved {len(trades)} daily 10 AM trades")
            for trade in trades[:5]:
                print(f"{trade['date']} at 10:00 AM: ${trade['price']:.2f}")

        # Get market close prices (last trade before 4 PM) for a week
        close_trades, url = await client.stock_at_time_trade(
            symbol="AAPL",
            start_date="20240115",
            end_date="20240119",
            time_of_day="15:59:59.999",
            venue="utp_cta"
        )
        """
        
        params = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "time_of_day": time_of_day,
            "format": format_type,
        }
        
        if venue:
            params["venue"] = venue
            
        return await self._make_request("/stock/at_time/trade", params)
    
    
    async def stock_at_time_quote(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        venue: Optional[Literal["nqb","utp_cta"]] = None,
        format_type: Optional[Literal["json","csv","ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """Retrieve the last NBBO quote at a specific time-of-day across multiple trading dates.

        This method fetches the most recent National Best Bid and Offer (NBBO) quote that existed
        at or before a specified time-of-day for each trading date in the specified date range.
        This time-synchronized query is invaluable for building consistent time series of market
        liquidity conditions, analyzing bid-ask spreads at specific intraday moments, aligning
        quote data with other time-based datasets (such as news events or economic releases),
        and understanding how market microstructure evolves at the same time each day. The returned
        dataset contains one quote record per trading date, representing the order book state at
        that precise moment each day.

        Parameters
        ----------
        symbol : str
            The stock ticker symbol to query. Examples: "AAPL" (Apple Inc.), "SPY" (S&P 500 ETF),
            "MSFT" (Microsoft Corporation). Must be a valid symbol available in the ThetaData
            database. Use stock_list_symbols() to discover available symbols.
        start_date : str
            The start date of the date range, inclusive. Acceptable formats are "YYYY-MM-DD"
            (e.g., "2024-01-15") or "YYYYMMDD" (e.g., "20240115"). This is the first trading date
            for which the at-time quote will be retrieved.
        end_date : str
            The end date of the date range, inclusive. Acceptable formats are "YYYY-MM-DD"
            (e.g., "2024-12-31") or "YYYYMMDD" (e.g., "20241231"). This is the last trading date
            for which the at-time quote will be retrieved.
        time_of_day : str
            The specific time within each trading day to query. Format is "HH:MM:SS" or "HH:MM:SS.SSS"
            with optional millisecond precision (e.g., "09:30:00", "15:59:59.999", "13:00:00.000").
            Times are in America/New_York (Eastern Time) timezone. The method returns the last NBBO
            quote that existed at or before this time on each date.
        venue : str, optional
            Default: "nqb" (server default)
            Possible values: "nqb", "utp_cta"
            The market data venue/feed from which to retrieve quotes:
            - "nqb": Nasdaq Quote Basic feed, suitable for current-day data and Nasdaq markets
            - "utp_cta": UTP/CTA consolidated Security Information Processor (SIP) feeds,
              provides comprehensive market-wide NBBO from all exchanges
            Venue selection affects which quote source is queried.
        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"
            The desired response format for the quote data:
            - "json": Returns data as a JSON array of quote objects, ideal for programmatic processing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet analysis
            - "ndjson": Returns newline-delimited JSON, useful for streaming large datasets

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The at-time quote data. For JSON format, this is a list of dictionaries
              where each dictionary represents the last NBBO quote at the specified time for one
              trading date, with fields: date, timestamp, bid_price, bid_size, ask_price, ask_size,
              bid_exchange, ask_exchange. For CSV/NDJSON, this is a formatted string. One record is
              returned per trading date in the range.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get the NBBO quote at market open (9:30 AM) for each day in January 2024
        async with ThetaDataV3Client() as client:
            quotes, url = await client.stock_at_time_quote(
                symbol="SPY",
                start_date="2024-01-01",
                end_date="2024-01-31",
                time_of_day="09:30:00"
            )
            print(f"Retrieved {len(quotes)} daily 9:30 AM quotes")
            for quote in quotes[:5]:
                spread = quote['ask_price'] - quote['bid_price']
                print(f"{quote['date']} at 9:30 AM: Bid ${quote['bid_price']:.2f}, Ask ${quote['ask_price']:.2f}, Spread ${spread:.4f}")

        # Get market close NBBO for a week using UTP/CTA feed
        close_quotes, url = await client.stock_at_time_quote(
            symbol="AAPL",
            start_date="20240115",
            end_date="20240119",
            time_of_day="15:59:59.999",
            venue="utp_cta"
        )
        """
        
        params = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "time_of_day": time_of_day,
            "format": format_type,
        }
        
        if venue:
            params["venue"] = venue
            
        return await self._make_request("/stock/at_time/quote", params)

        
    # =====================================
    # =====================================
    
    



    # --- At-Time (end) ---

    # =========================================================================
    # (END)
    # STOCK
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # OPTION
    # =========================================================================

    # --- List (begin) ---

    async def option_list_symbols(self, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve a comprehensive list of all underlying symbols for which option contracts are available.

        This method queries the ThetaData API to obtain all underlying stock symbols that have option
        contracts available in the database. These are the root symbols (e.g., "AAPL", "SPY", "TSLA")
        for which you can query option chains, strikes, expirations, and historical option data. This
        endpoint is essential as a discovery mechanism before requesting specific option data, allowing
        you to understand which underlyings have option market data available. The response helps in
        building option screening tools, constructing watch lists, and planning comprehensive option
        data downloads.

        Parameters
        ----------
        format_type : str, optional
            Default: "json"
            Possible values: ["csv", "json", "ndjson"]

            The desired response format for the data. The format choice should be based on your
            processing requirements and integration tooling.
            - "json": Returns data as a JSON array of underlying symbols, ideal for programmatic parsing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet import
            - "ndjson": Returns data as newline-delimited JSON, useful for streaming processing

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The list of underlying symbols with option data available. For JSON
              format, this is a list of strings or dictionaries with symbol information. For CSV/
              NDJSON formats, this is a formatted string.
            - Second element: The complete request URL as a string, including all query parameters.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            # Get all symbols with options available
            symbols, url = await client.option_list_symbols(format_type="json")
            print(f"Found {len(symbols)} underlyings with option contracts")
            print(f"Sample symbols: {symbols[:10]}")

            # Check if a specific symbol has options
            if "AAPL" in symbols:
                print("Apple options are available")
        ```
        """
        params = {"format": format_type}
        return await self._make_request("/option/list/symbols", params)
        
    
    async def option_list_dates(
        self,
        symbol: str,
        request_type: Literal["trade", "quote"] = "trade",
        expiration: str = None,
        strike: Optional[Union[str, float]] = None,
        right: Literal["call", "put", "both"] = "both",
        format_type: Optional[Literal["csv", "json", "ndjson", "html"]] = "json",
    ) -> Tuple[Any, str]:
        """Retrieve all available dates for which trade or quote data exists for a specific option contract specification.

        This method queries the ThetaData API to obtain a list of trading dates on which trade or quote
        data is available for option contracts matching the specified criteria. The results can be filtered
        by underlying symbol, expiration date, strike price, and option right (call/put), allowing you to
        precisely identify which dates have data coverage for your target option contracts. This endpoint
        is essential for verifying data availability before requesting bulk historical downloads, planning
        research timelines, and understanding gaps in historical option data coverage.

        Parameters
        ----------
        symbol : str
            The underlying stock symbol for the option contracts. Examples: "AAPL" (Apple Inc.),
            "SPY" (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data
            available in the ThetaData database. Use option_list_symbols() to discover available
            underlying symbols.
        request_type : str
            Default: "trade"
            Possible values: "trade", "quote"
            The type of option market data for which to check date availability:
            - "trade": Returns dates on which option trade (execution) data exists for the
              specified contract(s)
            - "quote": Returns dates on which option quote (bid/ask) data exists for the
              specified contract(s)
        expiration : str
            The option expiration date to query. This parameter is **required** by the API.
            Acceptable formats are "YYYY-MM-DD" (e.g., "2024-03-15") or "YYYYMMDD" (e.g.,
            "20240315"). Use option_list_expirations() to discover available expiration dates
            for a symbol.
        strike : str or float, optional
            Default: "*" (all strikes)
            The strike price to filter by, or "*" to include all strikes for the expiration.
            When providing a numeric strike, it can be specified as a float (e.g., 150.00) or
            string (e.g., "150.00"). When set to "*", dates are returned for any strike at the
            specified expiration and right.
        right : str
            Default: "both"
            Possible values: "call", "put", "both"
            The option right to filter by:
            - "call": Only check call option dates
            - "put": Only check put option dates
            - "both": Check dates for both calls and puts
        format_type : str, optional
            Default: "json"
            Possible values: "csv", "json", "ndjson", "html"
            The desired response format for the data:
            - "json": Returns data as a JSON array of date objects, ideal for programmatic processing
            - "csv": Returns data as comma-separated values, suitable for spreadsheet analysis
            - "ndjson": Returns data as newline-delimited JSON, useful for streaming
            - "html": Returns data as HTML table, useful for browser display

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The available dates data. For JSON format, this is a list of dictionaries
              where each dictionary contains fields: symbol, expiration, strike, right, date. For other
              formats, this is a formatted string. Each record represents one date on which data exists.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get all dates with trade data for AAPL March 2024 $150 calls
        async with ThetaDataV3Client() as client:
            dates, url = await client.option_list_dates(
                symbol="AAPL",
                request_type="trade",
                expiration="2024-03-15",
                strike=150.00,
                right="call"
            )
            print(f"Found {len(dates)} trading dates with data")
            for record in dates[:5]:
                print(f"{record['date']}: {record['symbol']} {record['strike']} {record['right']}")

        # Get all dates with quote data for any SPY option expiring in June 2024
        dates, url = await client.option_list_dates(
            symbol="SPY",
            request_type="quote",
            expiration="20240621",
            strike="*",
            right="both"
        )
        """
        if expiration is None:
            raise ValueError("`expiration` is required for /option/list/dates in v3.")
    
        # Build params, defaulting strike to "*"
        if strike is None:
            strike_val = "*"
        elif isinstance(strike, (int, float)):
            strike_val = f"{float(strike):.2f}"
        else:
            strike_val = str(strike)
    
        params = {
            "symbol": symbol,
            "expiration": expiration,   # accepts YYYY-MM-DD or YYYYMMDD
            "strike": strike_val,       # default "*"
            "right": right,             # default "both"
            "format": format_type or "json",
        }
    
        # Note the request_type is part of the path in v3
        return await self._make_request(f"/option/list/dates/{request_type}", params)

    
    async def option_list_expirations(
        self,
        symbol: str,
        format_type: Optional[Literal["csv", "json", "ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """Retrieve all available option expiration dates for a specific underlying symbol.

        This method queries the ThetaData API to obtain a comprehensive list of all option expiration
        dates available for the specified underlying stock symbol. These expiration dates represent
        all the option series for which market data exists in the database, ranging from short-dated
        weekly options to long-dated LEAPS (Long-term Equity AnticiPation Securities) potentially
        spanning years. This endpoint is fundamental for building option chains, constructing
        expiration-specific strategies, and understanding the full term structure of available options
        for a given underlying. The returned dates are in ISO-8601 format (YYYY-MM-DD) in UTC timezone.

        Parameters
        ----------
        symbol : str
            The underlying stock symbol for which to retrieve option expirations. Examples: "AAPL"
            (Apple Inc.), "SPY" (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with
            option data available in the ThetaData database. Use option_list_symbols() to discover
            available underlying symbols.
        format_type : str, optional
            Default: "json"
            Possible values: "csv", "json", "ndjson"
            The desired response format for the expiration dates:
            - "json": Returns data as a JSON array of ISO date strings, ideal for programmatic use
            - "csv": Returns dates as comma-separated values, suitable for spreadsheet import
            - "ndjson": Returns dates as newline-delimited JSON, useful for streaming

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The list of expiration dates. For JSON format, this is a list of strings
              in "YYYY-MM-DD" format (e.g., ["2024-01-19", "2024-02-16", "2024-03-15"]). For
              CSV/NDJSON, this is a formatted string. Dates are sorted chronologically.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get all option expirations for Apple
        async with ThetaDataV3Client() as client:
            expirations, url = await client.option_list_expirations("AAPL")
            print(f"Found {len(expirations)} expiration dates for AAPL")
            print(f"Nearest expiration: {expirations[0]}")
            print(f"Furthest expiration: {expirations[-1]}")

        # Filter for expirations in Q1 2024
        import datetime
        expirations, url = await client.option_list_expirations("SPY")
        q1_2024 = [exp for exp in expirations
                   if "2024-01" <= exp <= "2024-03-31"]
        print(f"Q1 2024 expirations: {q1_2024}")
        """
        if not symbol:
            raise ValueError("`symbol` is required for /option/list/expirations.")
    
        params = {"symbol": symbol, "format": format_type or "json"}
        return await self._make_request("/option/list/expirations", params)


        
    
    async def option_list_strikes(self, symbol: str, expiration: str,
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
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "format": format_type
        }
        return await self._make_request("/option/list/strikes", params)

        
    
    async def option_list_contracts(
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
        """
        # Validate request_type early to fail fast on typos.
        if request_type not in ("trade", "quote"):
            raise ValueError('request_type must be "trade" or "quote"')

        # Build query parameters expected by the v3 REST endpoint.
        params = {"date": date, "format": format_type}

        # Normalize symbol(s) to a comma-separated string if provided.
        if symbol:
            if isinstance(symbol, (list, tuple, set)):
                params["symbol"] = ",".join(map(str, symbol))
            else:
                params["symbol"] = str(symbol)

        # Compose the path with the request_type segment.
        path = f"/option/list/contracts/{request_type}"

        # Delegate HTTP call to the underlying client helper.
        return await self._make_request(path, params)


        
    

    # --- List (end) ---

    # --- Snapshot (begin) ---

    async def option_snapshot_ohlc(self, symbol: str, expiration: str, strike: float, right: str = "C",
                                   format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve the current OHLC (Open, High, Low, Close) snapshot for a specific option contract.

        This method fetches the most recent OHLC data available for the specified option contract from
        the ThetaData API. The snapshot represents the current trading session's price range if markets
        are open, or the most recent session's data if markets are closed. This real-time (or near real-time)
        data is essential for monitoring current option contract prices, understanding intraday price movements,
        and assessing option volatility patterns. The response includes open, high, low, and close prices along
        with volume, timestamp in UTC timezone, and potentially open interest depending on the data feed.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol for the option contract. Examples include "AAPL"
            (Apple Inc.), "SPY" (S&P 500 ETF), "TSLA" (Tesla Inc.). This represents the root symbol
            for the option chain. Must be a valid symbol available in the ThetaData database.

        expiration : str
            The option contract's expiration date. Format: YYYY-MM-DD (e.g., "2024-12-20") or YYYYMMDD
            (e.g., "20241220"). This date represents when the option contract expires and can no longer
            be traded. Use option_list_expirations() to discover available expiration dates for a symbol.

        strike : float
            The strike price of the option contract. This is the price at which the option holder can
            buy (call) or sell (put) the underlying asset. Examples: 150.00, 175.50, 200.00. Strike
            prices are typically spaced at regular intervals (e.g., $5, $10) depending on the underlying.

        right : str, optional
            Default: "C"
            Possible values: "C" (call option), "P" (put option)

            Specifies whether to retrieve data for a call option or put option. Call options give the
            holder the right to buy the underlying at the strike price, while put options give the right
            to sell. This parameter is case-sensitive.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            The desired response format for the data. "json" returns structured data ideal for programmatic
            parsing, "csv" returns comma-separated values suitable for spreadsheet applications, and "ndjson"
            returns newline-delimited JSON useful for streaming large datasets.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The OHLC snapshot data. For JSON format, this is a dictionary with fields
              including open, high, low, close, volume, timestamp, and possibly open_interest. For CSV/NDJSON
              formats, this is a formatted string containing the same information.
            - Second element: The complete request URL as a string, including all query parameters, useful
              for debugging, logging, and understanding the exact API call made.

        Example Usage
        -------------
        # Get current OHLC snapshot for an Apple call option
        async with ThetaDataV3Client() as client:
            ohlc, url = await client.option_snapshot_ohlc(
                symbol="AAPL",
                expiration="2024-12-20",
                strike=175.00,
                right="C"
            )
            print(f"AAPL $175 Call - Open: {ohlc['open']}, High: {ohlc['high']}")
            print(f"Low: {ohlc['low']}, Close: {ohlc['close']}")
            print(f"Volume: {ohlc['volume']}")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type
        }
        return await self._make_request("/option/snapshot/ohlc", params)

            
        
    async def option_snapshot_trade(
        self,
        symbol: str,
        expiration: str,
        strike: float,
        right: Literal["call", "put", "both"] = "both",
        format_type: Optional[Literal["json", "csv", "ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """
        Retrieve the last executed trade snapshot for a specific option contract.

        This method fetches information about the most recent trade executed for the specified option
        contract from the ThetaData API. The snapshot provides critical real-time market data including
        the trade price, size (number of contracts traded), timestamp in UTC timezone, exchange where
        the trade occurred, and trade conditions/flags. This data is essential for real-time price discovery,
        understanding current market activity, and monitoring the latest transaction prices for option contracts.
        The snapshot data represents actual executions rather than quotes, providing definitive evidence of
        price levels where transactions occurred. Snapshot endpoints maintain a real-time cache that resets
        nightly, ensuring you always get the most current market data.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol for the option contract. Examples include "AAPL"
            (Apple Inc.), "SPY" (S&P 500 ETF), "NVDA" (NVIDIA Corporation). This represents the root
            symbol for the option chain. Must be a valid symbol available in the ThetaData database.

        expiration : str
            The option contract's expiration date. Accepts flexible formats: YYYY-MM-DD (e.g., "2024-12-20")
            or YYYYMMDD (e.g., "20241220"). This date represents when the option contract expires. Use
            option_list_expirations() to discover available expiration dates for a symbol.

        strike : float
            The strike price of the option contract. This is the predetermined price at which the option
            holder can buy (for calls) or sell (for puts) the underlying asset. Examples: 150.00, 175.50,
            200.00. Strike prices are set at regular intervals determined by the exchange.

        right : Literal["call", "put", "both"], optional
            Default: "both"
            Possible values: "call", "put", "both"

            Specifies which type of option contract(s) to retrieve trade data for. "call" retrieves only
            call options (right to buy), "put" retrieves only put options (right to sell), and "both"
            retrieves data for both call and put contracts at the specified strike and expiration. Using
            "both" is efficient when you need to compare call and put activity simultaneously.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            The desired response format for the data. "json" returns structured data ideal for programmatic
            parsing, "csv" returns comma-separated values suitable for spreadsheet import, and "ndjson"
            returns newline-delimited JSON useful for streaming and processing large result sets.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The last trade data. For JSON format, this is a dictionary (or list of
              dictionaries if right="both") with fields such as price, size, timestamp, exchange,
              conditions, and trade flags. For CSV/NDJSON formats, this is a formatted string.
            - Second element: The complete request URL as a string, including all query parameters,
              useful for debugging, logging, and audit trails.

        Example Usage
        -------------
        # Get last trade for both call and put options
        async with ThetaDataV3Client() as client:
            trades, url = await client.option_snapshot_trade(
                symbol="TSLA",
                expiration="2024-12-20",
                strike=250.00,
                right="both"
            )
            for trade in trades:
                print(f"{trade['right']}: Last trade at ${trade['price']} for {trade['size']} contracts")
                print(f"Exchange: {trade['exchange']}, Time: {trade['timestamp']}")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type,
        }
        return await self._make_request("/option/snapshot/trade", params)
    
        
    async def option_snapshot_quote(
        self,
        symbol: str,
        expiration: str,
        strike: float,
        right: Literal["call", "put", "both"] = "both",
        format_type: Optional[Literal["json", "csv", "ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """
        Retrieve the current quote snapshot (best bid and ask) for a specific option contract.

        This method fetches the most recent National Best Bid and Offer (NBBO) data for the specified
        option contract from the ThetaData API. The quote snapshot includes the current best bid price
        and size (number of contracts), best ask price and size, timestamp in UTC timezone, exchange
        information, quote conditions, and the current bid-ask spread. This data is critical for understanding
        current market liquidity, assessing the depth of the order book, determining the cost of immediate
        execution, and identifying the best available prices across all exchanges. Quote data differs from
        trade data as it represents available prices where market participants are willing to transact,
        rather than completed transactions. Snapshot endpoints maintain a real-time cache that resets
        nightly, ensuring the most current market data is always available.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol for the option contract. Examples include "AAPL"
            (Apple Inc.), "SPY" (S&P 500 ETF), "AMZN" (Amazon.com Inc.). This represents the root symbol
            for the option chain. Must be a valid symbol available in the ThetaData database.

        expiration : str
            The option contract's expiration date. Accepts flexible formats: YYYY-MM-DD (e.g., "2024-12-20")
            or YYYYMMDD (e.g., "20241220"). Alternatively, use "*" to retrieve quotes for all expiration
            dates at the specified strike and right. The expiration date is when the option contract ceases
            to exist and final settlement occurs.

        strike : float
            The strike price of the option contract. This is the price at which the option can be exercised.
            Examples: 150.00, 175.50, 200.00. Alternatively, use "*" (as a string) to retrieve quotes for
            all strike prices at the specified expiration and right. This is useful for getting an entire
            option chain snapshot.

        right : Literal["call", "put", "both"], optional
            Default: "both"
            Possible values: "call", "put", "both"

            Specifies which type of option contract(s) to retrieve quote data for. "call" returns quotes
            for call options (right to buy the underlying), "put" returns quotes for put options (right
            to sell the underlying), and "both" returns quotes for both calls and puts at the specified
            strike and expiration. Using "both" is efficient for analyzing put-call parity and comparing
            option pricing across contract types.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            The desired response format for the data. "json" returns structured data ideal for programmatic
            parsing, "csv" returns comma-separated values suitable for spreadsheet applications and data
            analysis tools, and "ndjson" returns newline-delimited JSON useful for streaming large datasets.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The quote data. For JSON format, this is a dictionary (or list of dictionaries
              if right="both" or wildcards are used) with fields such as bid_price, bid_size, ask_price,
              ask_size, bid_exchange, ask_exchange, spread, timestamp, and conditions. For CSV/NDJSON
              formats, this is a formatted string containing the same information.
            - Second element: The complete request URL as a string, including all query parameters, useful
              for debugging, logging, and understanding the exact API call made.

        Example Usage
        -------------
        # Get current quote for a specific Microsoft option
        async with ThetaDataV3Client() as client:
            quote, url = await client.option_snapshot_quote(
                symbol="MSFT",
                expiration="2024-12-20",
                strike=370.00,
                right="call"
            )
            spread = quote['ask_price'] - quote['bid_price']
            print(f"Bid: ${quote['bid_price']} x {quote['bid_size']} contracts")
            print(f"Ask: ${quote['ask_price']} x {quote['ask_size']} contracts")
            print(f"Spread: ${spread:.2f}")
            print(f"Mid-price: ${(quote['bid_price'] + quote['ask_price']) / 2:.2f}")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type,
        }
        return await self._make_request("/option/snapshot/quote", params)


    async def option_snapshot_open_interest(self, symbol: str, expiration: str, strike: float, right: str = "C",
                                            format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve the current open interest snapshot for a specific option contract.

        This method fetches the most recent open interest data for the specified option contract from
        the ThetaData API. Open interest represents the total number of outstanding option contracts
        that have been opened but not yet closed, exercised, or expired. This metric is crucial for
        assessing market liquidity, identifying significant support/resistance levels, and understanding
        where large institutional positions may exist. High open interest typically indicates greater
        liquidity and tighter bid-ask spreads, while changes in open interest can signal new positions
        being established or existing positions being closed. Open interest data is typically reported
        once per day by OPRA (Options Price Reporting Authority) and represents the prior trading day's
        end-of-day value.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol for the option contract. Examples include "AAPL"
            (Apple Inc.), "SPY" (S&P 500 ETF), "TSLA" (Tesla Inc.). This represents the root symbol
            for the option chain. Must be a valid symbol available in the ThetaData database.

        expiration : str
            The option contract's expiration date. Format: YYYY-MM-DD (e.g., "2024-12-20") or YYYYMMDD
            (e.g., "20241220"). This is the date when the option expires and all remaining open contracts
            are settled. Use option_list_expirations() to discover available expiration dates.

        strike : float
            The strike price of the option contract. This is the predetermined price at which the option
            can be exercised. Examples: 150.00, 175.50, 200.00. Strike prices are standardized by the
            exchange and vary based on the underlying asset's price and volatility.

        right : str, optional
            Default: "C"
            Possible values: "C" (call option), "P" (put option)

            Specifies whether to retrieve open interest for a call option or put option. "C" represents
            call options (right to buy the underlying), while "P" represents put options (right to sell
            the underlying). This parameter is case-sensitive and uses single-letter codes.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            The desired response format for the data. "json" returns structured data ideal for programmatic
            use, "csv" returns comma-separated values for spreadsheet import, and "ndjson" returns
            newline-delimited JSON for streaming applications.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The open interest data. For JSON format, this is a dictionary with fields
              including the open_interest value (number of contracts), timestamp, symbol, expiration,
              strike, and right. For CSV/NDJSON formats, this is a formatted string.
            - Second element: The complete request URL as a string, including all query parameters,
              useful for debugging and logging.

        Example Usage
        -------------
        # Get current open interest for an Apple call option
        async with ThetaDataV3Client() as client:
            oi_data, url = await client.option_snapshot_open_interest(
                symbol="AAPL",
                expiration="2024-12-20",
                strike=175.00,
                right="C"
            )
            print(f"Open Interest: {oi_data['open_interest']:,} contracts")
            print(f"Last updated: {oi_data['timestamp']}")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type
        }
        return await self._make_request("/option/snapshot/open_interest", params)

    
    async def option_snapshot_implied_volatility(self, symbol: str, expiration: str, strike: float, right: str = "C",
                                            format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve the current implied volatility snapshot for a specific option contract.

        This method fetches the most recent implied volatility (IV) calculation for the specified option
        contract from the ThetaData API. Implied volatility represents the market's expectation of future
        volatility for the underlying asset, derived by working backwards from the option's market price
        using an option pricing model (typically Black-Scholes). IV is expressed as an annualized percentage
        and is one of the most important metrics in options trading. High IV indicates the market expects
        significant price movement (uncertainty), while low IV suggests expectations of stability. IV is
        crucial for comparing option prices across different strikes and expirations, identifying overpriced
        or underpriced options, and understanding market sentiment. The snapshot provides the current IV
        value along with a timestamp in UTC timezone.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol for the option contract. Examples include "AAPL"
            (Apple Inc.), "SPY" (S&P 500 ETF), "NVDA" (NVIDIA Corporation). This represents the root
            symbol for the option chain. Must be a valid symbol available in the ThetaData database.

        expiration : str
            The option contract's expiration date. Format: YYYY-MM-DD (e.g., "2024-12-20") or YYYYMMDD
            (e.g., "20241220"). This is the date when the option expires. Options with different expiration
            dates typically have different implied volatilities, with term structure affecting pricing.

        strike : float
            The strike price of the option contract. Examples: 150.00, 175.50, 200.00. The relationship
            between strike price and implied volatility often exhibits a "volatility smile" or "skew"
            pattern, where out-of-the-money puts may have higher IV than at-the-money options.

        right : str, optional
            Default: "C"
            Possible values: "C" (call option), "P" (put option)

            Specifies whether to retrieve implied volatility for a call or put option. "C" retrieves IV
            for call options, "P" for put options. Put and call IVs at the same strike should theoretically
            be equal due to put-call parity, but may differ slightly in practice due to market dynamics.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            The desired response format for the data. "json" returns structured data ideal for programmatic
            analysis, "csv" returns comma-separated values for spreadsheet use, and "ndjson" returns
            newline-delimited JSON for streaming applications.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The implied volatility data. For JSON format, this is a dictionary with
              fields including the implied_volatility value (as a decimal, e.g., 0.25 for 25%), timestamp,
              symbol, expiration, strike, and right. For CSV/NDJSON formats, this is a formatted string.
            - Second element: The complete request URL as a string, including all query parameters,
              useful for debugging and analysis.

        Example Usage
        -------------
        # Get current implied volatility for a Tesla put option
        async with ThetaDataV3Client() as client:
            iv_data, url = await client.option_snapshot_implied_volatility(
                symbol="TSLA",
                expiration="2024-12-20",
                strike=250.00,
                right="P"
            )
            iv_percent = iv_data['implied_volatility'] * 100
            print(f"Implied Volatility: {iv_percent:.2f}%")
            print(f"This suggests the market expects ~{iv_percent:.1f}% annualized volatility")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type
        }
        return await self._make_request("/option/snapshot/greeks/implied_volatility", params) 
    
    async def option_snapshot_all_greeks(self, symbol: str, expiration: str, strike: float, right: str = "C",
                                            format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve a comprehensive snapshot of all option Greeks for a specific option contract.

        This method fetches a complete set of option Greeks (risk metrics) for the specified option
        contract from the ThetaData API. The Greeks are partial derivatives that measure how an option's
        price changes in response to various factors. This comprehensive snapshot includes first-order
        Greeks (Delta, Gamma, Theta, Vega, Rho), second-order Greeks (Vomma, Vanna, Charm, Veta), and
        third-order Greeks (Speed, Zomma, Color, Ultima). All Greeks are calculated using standard
        option pricing models and represent the current risk profile of the option contract. This data
        is essential for sophisticated option trading strategies, portfolio risk management, hedging
        operations, and understanding how option positions will respond to changes in market conditions.
        The snapshot includes a timestamp in UTC timezone indicating when the calculations were performed.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol for the option contract. Examples include "AAPL"
            (Apple Inc.), "SPY" (S&P 500 ETF), "GOOGL" (Alphabet Inc.). This represents the root symbol
            for the option chain. Must be a valid symbol in the ThetaData database.

        expiration : str
            The option contract's expiration date. Format: YYYY-MM-DD (e.g., "2024-12-20") or YYYYMMDD
            (e.g., "20241220"). Greeks change significantly as expiration approaches, with Gamma and
            Theta particularly sensitive to time decay for near-expiration contracts.

        strike : float
            The strike price of the option contract. Examples: 150.00, 175.50, 200.00. Greeks vary
            considerably across strikes, with Delta ranging from near 0 (deep out-of-the-money) to
            near 1 (deep in-the-money) for calls, and Gamma peaking at-the-money.

        right : str, optional
            Default: "C"
            Possible values: "C" (call option), "P" (put option)

            Specifies whether to retrieve Greeks for a call or put option. Call and put Greeks differ
            in sign conventions: call Delta is positive (0 to 1), put Delta is negative (-1 to 0),
            while Gamma, Vega, and Theta behave similarly for both but with different magnitudes.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            The desired response format for the data. "json" returns structured data ideal for
            programmatic analysis and risk calculations, "csv" returns comma-separated values for
            spreadsheet applications, and "ndjson" returns newline-delimited JSON for streaming.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: All Greeks data. For JSON format, this is a dictionary containing fields
              for all calculated Greeks including delta, gamma, theta, vega, rho, vomma, vanna, charm,
              veta, speed, zomma, color, ultima, plus implied_volatility, timestamp, and contract details.
              For CSV/NDJSON formats, this is a formatted string with the same information.
            - Second element: The complete request URL as a string, including all query parameters.

        Example Usage
        -------------
        # Get all Greeks for comprehensive risk analysis
        async with ThetaDataV3Client() as client:
            greeks, url = await client.option_snapshot_all_greeks(
                symbol="SPY",
                expiration="2024-12-20",
                strike=450.00,
                right="C"
            )
            print(f"Delta: {greeks['delta']:.4f} (price sensitivity)")
            print(f"Gamma: {greeks['gamma']:.4f} (delta sensitivity)")
            print(f"Theta: {greeks['theta']:.4f} (time decay per day)")
            print(f"Vega: {greeks['vega']:.4f} (volatility sensitivity)")
            print(f"Rho: {greeks['rho']:.4f} (interest rate sensitivity)")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type
        }
        return await self._make_request("/option/snapshot/greeks/all", params)
    
    async def option_snapshot_first_order_greeks(self, symbol: str, expiration: str, strike: float, right: str = "C",
                                            format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve first-order Greeks snapshot for a specific option contract.

        This method fetches the fundamental option Greeks (first-order partial derivatives) for the
        specified option contract from the ThetaData API. First-order Greeks are the most commonly used
        risk metrics in options trading and include Delta (price sensitivity), Gamma (Delta sensitivity),
        Theta (time decay), Vega (volatility sensitivity), and Rho (interest rate sensitivity). These
        Greeks measure how an option's price responds to changes in the underlying asset price, time to
        expiration, implied volatility, and interest rates. They are essential for basic option risk
        management, position sizing, hedging strategies, and understanding profit/loss dynamics. The
        snapshot includes a timestamp in UTC timezone and represents current market conditions.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL", "SPY", "TSLA". Must be a
            valid symbol in the ThetaData database.

        expiration : str
            The option contract's expiration date. Format: YYYY-MM-DD or YYYYMMDD. Greeks change
            significantly as expiration approaches, particularly Theta and Gamma.

        strike : float
            The strike price of the option contract. Examples: 150.00, 175.50. Greeks vary across
            strikes, with at-the-money options typically having highest Gamma and Vega.

        right : str, optional
            Default: "C"
            Possible values: "C" (call), "P" (put)

            Call Delta: 0 to 1, Put Delta: -1 to 0. Gamma, Theta, and Vega are similar for both but
            differ in magnitude based on moneyness.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            Response format selection. "json" is ideal for programmatic use.

        Returns
        -------
        Tuple[Any, str]
            - First element: Greeks data dictionary (JSON) or string (CSV/NDJSON) containing delta,
              gamma, theta, vega, rho, timestamp, and contract identifiers.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Get first-order Greeks for risk management
        async with ThetaDataV3Client() as client:
            greeks, url = await client.option_snapshot_first_order_greeks(
                symbol="AAPL", expiration="2024-12-20", strike=175.00, right="C"
            )
            print(f"Delta: {greeks['delta']:.4f}, Theta: {greeks['theta']:.4f}")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type
        }
        return await self._make_request("/option/snapshot/greeks/first_order", params)
    
    async def option_snapshot_second_order_greeks(self, symbol: str, expiration: str, strike: float, right: str = "C",
                                            format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve second-order Greeks snapshot for a specific option contract.

        This method fetches advanced option Greeks (second-order partial derivatives) for the specified
        option contract from the ThetaData API. Second-order Greeks measure how first-order Greeks change
        in response to market conditions and include Vomma (Vega sensitivity to volatility), Vanna (Delta
        sensitivity to volatility), Charm (Delta sensitivity to time), and Veta (Vega sensitivity to time).
        These advanced metrics are crucial for sophisticated options trading strategies, particularly for
        managing large portfolios, understanding convexity effects, and hedging complex positions. Second-order
        Greeks help quantify risks that first-order Greeks cannot capture, such as how a hedged position
        might become unhedged as market conditions change. Essential for volatility trading and dynamic
        hedging strategies.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL", "SPY", "NVDA".

        expiration : str
            The option contract's expiration date. Format: YYYY-MM-DD or YYYYMMDD.

        strike : float
            The strike price of the option contract. Examples: 150.00, 175.50.

        right : str, optional
            Default: "C"
            Possible values: "C" (call), "P" (put)

            Second-order Greeks help understand how first-order Greeks evolve, critical for maintaining
            effective hedges as market conditions shift.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            Response format selection.

        Returns
        -------
        Tuple[Any, str]
            - First element: Second-order Greeks data including vomma, vanna, charm, veta, timestamp,
              and contract details.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Analyze convexity for advanced risk management
        async with ThetaDataV3Client() as client:
            greeks2, url = await client.option_snapshot_second_order_greeks(
                symbol="SPY", expiration="2024-12-20", strike=450.00, right="C"
            )
            print(f"Vomma: {greeks2['vomma']:.6f} (vol convexity)")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type
        }
        return await self._make_request("/option/snapshot/greeks/second_order", params)
    
    async def option_snapshot_third_order_greeks(self, symbol: str, expiration: str, strike: float, right: str = "C",
                                            format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve third-order Greeks snapshot for a specific option contract.

        This method fetches highly advanced option Greeks (third-order partial derivatives) for the
        specified option contract from the ThetaData API. Third-order Greeks measure how second-order
        Greeks change in response to market conditions and include Speed (Gamma sensitivity to underlying
        price), Zomma (Gamma sensitivity to volatility), Color (Gamma sensitivity to time), and Ultima
        (Vomma sensitivity to volatility). These sophisticated metrics are used primarily by professional
        market makers, large institutional traders, and quantitative hedge funds for managing extremely
        large option portfolios and understanding higher-order risks. Third-order Greeks capture complex
        non-linear effects that become significant in volatile markets or for positions with substantial
        Gamma exposure. Essential for institutional-grade risk management systems.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL", "SPY", "TSLA".

        expiration : str
            The option contract's expiration date. Format: YYYY-MM-DD or YYYYMMDD.

        strike : float
            The strike price of the option contract. Examples: 150.00, 175.50.

        right : str, optional
            Default: "C"
            Possible values: "C" (call), "P" (put)

            Third-order Greeks are typically used for managing large, complex option books where
            higher-order sensitivities become material to P&L.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            Response format selection.

        Returns
        -------
        Tuple[Any, str]
            - First element: Third-order Greeks data including speed, zomma, color, ultima, timestamp,
              and contract details.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Advanced institutional risk analysis
        async with ThetaDataV3Client() as client:
            greeks3, url = await client.option_snapshot_third_order_greeks(
                symbol="SPY", expiration="2024-12-20", strike=450.00, right="C"
            )
            print(f"Speed: {greeks3['speed']:.8f} (Gamma convexity)")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type
        }
        return await self._make_request("/option/snapshot/greeks/third_order", params)
        
    



    # --- Snapshot (end) ---

    # --- History (begin) ---

    async def option_history_eod(
        self,
        symbol: str,
        expiration: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        format_type: Literal["csv", "json", "ndjson", "html"] = "csv",
        # backward-compat: allow single 'date' like old wrapper
        date: Optional[str] = None,
    ) -> Tuple[Any, str]:
        """
        Retrieve End-of-Day (EOD) summary data for option contracts across multiple trading days.

        This method fetches daily aggregated option data including open, high, low, close prices,
        volume, and open interest for specified option contracts from the ThetaData API. EOD reports
        are generated at 17:15 ET using official closing prices and settlement data. This endpoint is
        essential for daily option analysis, portfolio valuation, historical backtesting, and tracking
        option price movements over time. The data can span multiple days and supports wildcard filters
        for strikes and expirations, enabling bulk historical data retrieval for entire option chains.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL", "SPY", "TSLA".

        expiration : str
            The option contract's expiration date. Recommended format: YYYYMMDD (e.g., "20241220").
            Also accepts YYYY-MM-DD format. Use "*" to retrieve data for all available expirations
            (note: when using "*", requests should be made day-by-day for optimal performance).

        start_date : str, optional
            Default: None (must be provided unless 'date' parameter is used)
            Start date (inclusive) for the date range in YYYYMMDD or YYYY-MM-DD format.
            Defines the beginning of the historical period to retrieve EOD data.

        end_date : str, optional
            Default: None (must be provided unless 'date' parameter is used)
            End date (inclusive) for the date range in YYYYMMDD or YYYY-MM-DD format.
            Defines the end of the historical period to retrieve EOD data.

        strike : float or str, optional
            Default: "*"
            Possible values: Any valid strike price (e.g., 150.0, 175.5) or "*"
            The strike price of the option contract. Use "*" to retrieve data for all available
            strikes within the specified expiration and date range. This enables downloading
            complete option chain snapshots.

        right : Literal["call", "put", "both"], optional
            Default: "both"
            Possible values: "call", "put", "both"
            Filters results by option type. "call" returns only call options, "put" returns only
            put options, and "both" returns both calls and puts in the same dataset.

        format_type : Literal["csv", "json", "ndjson", "html"], optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson", "html"
            The response format. CSV is recommended for large datasets and easy import into
            analysis tools. JSON/NDJSON are useful for programmatic processing. HTML provides
            human-readable tables for quick review.

        date : str, optional
            Default: None
            Legacy parameter for backward compatibility. When provided, this single date is treated
            as both start_date and end_date (i.e., retrieves EOD data for just one trading day).
            Format: YYYYMMDD or YYYY-MM-DD. Use either this parameter OR start_date/end_date, not both.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing:
            - The EOD data in the requested format (List[Dict] for JSON, string for CSV/HTML)
            - The fully-qualified request URL string used to fetch the data

        Example Usage
        -------------
        # Get EOD data for specific AAPL call option over a week
        async with ThetaDataV3Client() as client:
            data, url = await client.option_history_eod(
                symbol="AAPL",
                expiration="20241220",
                start_date="20241104",
                end_date="20241108",
                strike=180.0,
                right="call",
                format_type="json"
            )

        # Get EOD data for entire option chain on a single day (using legacy 'date' param)
        async with ThetaDataV3Client() as client:
            data, url = await client.option_history_eod(
                symbol="SPY",
                expiration="20241220",
                date="20241105",
                strike="*",
                right="both",
                format_type="csv"
            )

        # Get EOD data for all SPY expirations with 500 strike over multiple days
        async with ThetaDataV3Client() as client:
            data, url = await client.option_history_eod(
                symbol="SPY",
                expiration="*",
                start_date="20241101",
                end_date="20241101",  # Single day when using "*" expiration
                strike=500.0,
                right="both"
            )

        Notes
        -----
        - EOD reports are generated at 17:15 ET using official closing prices
        - When expiration="*", it's recommended to issue requests day-by-day for better performance
        - EOD data typically includes: timestamp, symbol, expiration, strike, right, open, high,
          low, close, volume, open_interest, bid, ask, implied_volatility
        - Join keys with daily OHLC typically include: ["timestamp", "symbol", "expiration", "strike", "right"]

        Endpoint:
            GET /option/history/eod
            Docs: https://docs.thetadata.us/operations/option_history_eod.html
        """
        if (start_date is None or end_date is None) and date is not None:
            start_date = end_date = date
        if start_date is None or end_date is None:
            raise ValueError("start_date and end_date (or 'date') are required.")
    
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "start_date": start_date,
            "end_date": end_date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "format": format_type,
        }
        return await self._make_request("/option/history/eod", params)



    async def option_history_ohlc(
        self,
        symbol: str,
        expiration: str,
        date: str,
        interval: Interval,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve aggregated intraday OHLC (Open-High-Low-Close) bars for option contracts.

        This method fetches aggregated trade data for option contracts on a specific trading date,
        returning OHLC bars at specified time intervals. The bars are constructed using Securities
        Information Processor (SIP) rules where each bar's timestamp represents the bar's open time,
        and trades are included if open_time <= trade_time < open_time + interval. This data is
        critical for analyzing option price movements, identifying support and resistance levels,
        backtesting trading strategies, and understanding intraday volatility patterns for options.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates.
        date : str
            The specific trading date to query. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-11-05")
            or "YYYYMMDD" (e.g., "20241105"). Only data from this single date will be returned.
        interval : Interval
            The aggregation interval for OHLC bars. Possible values: "tick", "10ms", "100ms", "500ms",
            "1s", "5s", "10s", "15s", "30s", "1m", "5m", "10m", "15m", "30m", "1h". Common values
            for option analysis include "1m" (1 minute), "5m" (5 minutes), and "1h" (1 hour).
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
            When set to "*", returns data for all available strikes at the specified expiration.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query. "call" retrieves only call options, "put" retrieves
            only put options, and "both" retrieves both calls and puts in a single request.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day. Format: "HH:MM:SS" (e.g., "09:30:00", "10:00:00").
            Times are in America/New_York (Eastern Time) timezone. Only bars starting at or after
            this time will be included in the results.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day. Format: "HH:MM:SS" (e.g., "16:00:00", "14:30:00").
            Times are in America/New_York (Eastern Time) timezone. Only bars starting before this
            time will be included in the results.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format. "csv" returns comma-separated values, "json" returns
            a JSON array of bar objects, "ndjson" returns newline-delimited JSON for streaming.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The OHLC data. For JSON/NDJSON, this is a list of dictionaries with
              fields: timestamp, open, high, low, close, volume, count, expiration, strike, right.
              For CSV, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get 1-minute OHLC bars for AAPL call options
        async with ThetaDataV3Client() as client:
            bars, url = await client.option_history_ohlc(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241104",
                interval="1m",
                strike=175.0,
                right="call"
            )
            print(f"Retrieved {len(bars)} 1-minute bars")

        # Get 5-minute bars for all strikes and both calls/puts
        bars, url = await client.option_history_ohlc(
            symbol="SPY",
            expiration="2024-11-15",
            date="2024-11-04",
            interval="5m",
            strike="*",
            right="both",
            format_type="json"
        )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "interval": interval,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
            
        return await self._make_request("/option/history/ohlc", params)

    async def option_history_trade(
        self,
        symbol: str,
        expiration: str,
        date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve every trade (tick-level) for option contracts on a specific trading date.

        This method fetches all individual trade executions for option contracts on a given date,
        returning every OPRA (Options Price Reporting Authority) trade message that matches the
        specified filters. Each trade record includes timestamp, price, size, exchange, and option
        identifiers. This tick-level data is essential for analyzing option trading activity,
        understanding volume patterns, studying price discovery mechanisms, identifying large
        block trades, and conducting high-frequency market microstructure analysis.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates.
        date : str
            The specific trading date to query. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-11-04")
            or "YYYYMMDD" (e.g., "20241104"). Only trades from this single date will be returned.
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
            When set to "*", returns data for all available strikes at the specified expiration.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query. "call" retrieves only call options, "put" retrieves
            only put options, and "both" retrieves both calls and puts in a single request.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day. Format: "HH:MM:SS" (e.g., "09:30:00", "10:00:00").
            Times are in America/New_York (Eastern Time) timezone. Only trades at or after this
            time will be included in the results.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day. Format: "HH:MM:SS" (e.g., "16:00:00", "14:30:00").
            Times are in America/New_York (Eastern Time) timezone. Only trades before this time
            will be included in the results.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format. "csv" returns comma-separated values, "json" returns
            a JSON array of trade objects, "ndjson" returns newline-delimited JSON for streaming.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The trade data. For JSON/NDJSON, this is a list of dictionaries with
              fields: timestamp, price, size, exchange, expiration, strike, right, conditions.
              For CSV, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get all trades for AAPL call options at a specific strike
        async with ThetaDataV3Client() as client:
            trades, url = await client.option_history_trade(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241104",
                strike=175.0,
                right="call"
            )
            print(f"Retrieved {len(trades)} trade executions")

        # Get all trades for all strikes and expirations during market hours
        trades, url = await client.option_history_trade(
            symbol="SPY",
            expiration="*",
            date="2024-11-04",
            strike="*",
            right="both",
            start_time="09:30:00",
            end_time="16:00:00",
            format_type="json"
        )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
            
        return await self._make_request("/option/history/trade", params)

    async def option_history_quote(
        self,
        symbol: str,
        expiration: str,
        date: str,
        interval: Interval,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve historical NBBO quote data for option contracts on a specific trading date.

        This method fetches National Best Bid and Offer (NBBO) quote data for option contracts,
        returning either every tick-level quote or the last quote within each specified time interval.
        Quote data includes bid price, ask price, bid size, ask size, exchange identifiers, and
        timestamps. This data is essential for analyzing bid-ask spreads, market liquidity,
        order book dynamics, and understanding the prevailing market conditions for options throughout
        the trading day. Supports filtering by strike, expiration, and option type.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates.
        date : str
            The specific trading date to query. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-11-04")
            or "YYYYMMDD" (e.g., "20241104"). Only data from this single date will be returned.
        interval : Interval
            The aggregation interval for quotes. Possible values: "tick", "10ms", "100ms", "500ms",
            "1s", "5s", "10s", "15s", "30s", "1m", "5m", "10m", "15m", "30m", "1h". Use "tick"
            for every single quote. Other intervals return the last quote within each time window.
            Server default is "1s" if not specified.
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
            When set to "*", returns data for all available strikes at the specified expiration.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query. "call" retrieves only call options, "put" retrieves
            only put options, and "both" retrieves both calls and puts in a single request.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day. Format: "HH:MM:SS" (e.g., "09:30:00", "10:00:00").
            Times are in America/New_York (Eastern Time) timezone. Only quotes at or after this
            time will be included in the results.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day. Format: "HH:MM:SS" (e.g., "16:00:00", "14:30:00").
            Times are in America/New_York (Eastern Time) timezone. Only quotes before this time
            will be included in the results.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format. "csv" returns comma-separated values, "json" returns
            a JSON array of quote objects, "ndjson" returns newline-delimited JSON for streaming.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The quote data. For JSON/NDJSON, this is a list of dictionaries with
              fields: timestamp, bid_price, ask_price, bid_size, ask_size, bid_exchange, ask_exchange,
              expiration, strike, right. For CSV, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get all NBBO quotes for AAPL options at 1-second intervals
        async with ThetaDataV3Client() as client:
            quotes, url = await client.option_history_quote(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241104",
                interval="1s",
                strike=175.0,
                right="call"
            )
            print(f"Retrieved {len(quotes)} quote snapshots")

        # Get tick-level quotes for all strikes and expirations
        quotes, url = await client.option_history_quote(
            symbol="SPY",
            expiration="*",
            date="2024-11-04",
            interval="tick",
            strike="*",
            right="both",
            format_type="json"
        )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "interval": interval,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
            
        return await self._make_request("/option/history/quote", params)

    async def option_history_trade_quote(
        self,
        symbol: str,
        expiration: str,
        date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        exclusive: bool = True,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve every trade paired with the prevailing NBBO quote at the time of each trade.

        This method fetches all option trade executions along with the National Best Bid and Offer
        (NBBO) quote that was in effect at the time of each trade. For each trade, the method returns
        the most recent NBBO whose timestamp is <= (or < if exclusive=True) the trade timestamp. This
        combined data enables critical market quality analysis including execution quality assessment,
        market impact studies, price improvement analysis, and understanding whether trades occurred
        at the bid, ask, or mid-price. Essential for regulatory compliance and best execution analysis.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates.
        date : str
            The specific trading date to query. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-11-04")
            or "YYYYMMDD" (e.g., "20241104"). Only data from this single date will be returned.
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
            When set to "*", returns data for all available strikes at the specified expiration.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query. "call" retrieves only call options, "put" retrieves
            only put options, and "both" retrieves both calls and puts in a single request.
        exclusive : bool, optional
            Default: True
            Controls the quote matching logic. If True, matches quotes with timestamp < trade time
            (strictly before). If False, matches quotes with timestamp <= trade time. The default
            of True is recommended to ensure the quote existed before the trade execution.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day. Format: "HH:MM:SS" (e.g., "09:30:00", "10:00:00").
            Times are in America/New_York (Eastern Time) timezone. Only trades at or after this
            time will be included in the results.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day. Format: "HH:MM:SS" (e.g., "16:00:00", "14:30:00").
            Times are in America/New_York (Eastern Time) timezone. Only trades before this time
            will be included in the results.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format. "csv" returns comma-separated values, "json" returns
            a JSON array of trade-quote objects, "ndjson" returns newline-delimited JSON.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The trade-quote data. For JSON/NDJSON, this is a list of dictionaries
              with fields: trade_timestamp, trade_price, trade_size, trade_exchange, bid_price,
              ask_price, bid_size, ask_size, bid_exchange, ask_exchange, expiration, strike, right.
              For CSV, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get trades with NBBO for AAPL call options for execution quality analysis
        async with ThetaDataV3Client() as client:
            trade_quotes, url = await client.option_history_trade_quote(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241104",
                strike=175.0,
                right="call",
                exclusive=True
            )
            print(f"Retrieved {len(trade_quotes)} trade-quote pairs")

        # Analyze all trades with prevailing quotes for market impact study
        trade_quotes, url = await client.option_history_trade_quote(
            symbol="SPY",
            expiration="2024-11-15",
            date="2024-11-04",
            strike="*",
            right="both",
            format_type="json"
        )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "exclusive": str(exclusive).lower(),
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
            
        return await self._make_request("/option/history/trade_quote", params)



    async def option_history_open_interest(
        self,
        symbol: str,
        expiration: str,
        date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve historical open interest data for option contracts.

        This method fetches open interest (OI) data for option contracts on a specific date.
        Open interest represents the total number of outstanding option contracts that have
        not been settled or closed. OPRA (Options Price Reporting Authority) typically reports
        OI once per day, and the value reflects the prior-day end-of-day open interest. This
        data is essential for understanding option liquidity, identifying high-activity strikes,
        analyzing market maker positioning, and assessing overall market participation in
        specific option contracts.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates.
        date : str
            The trading date for which open interest is reported. Acceptable formats: "YYYY-MM-DD"
            (e.g., "2024-11-04") or "YYYYMMDD" (e.g., "20241104"). Note that the reported OI
            typically represents the prior-day end-of-day open interest.
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
            When set to "*", returns data for all available strikes at the specified expiration.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query. "call" retrieves only call options, "put" retrieves
            only put options, and "both" retrieves both calls and puts in a single request.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format. "csv" returns comma-separated values, "json" returns
            a JSON array of open interest records, "ndjson" returns newline-delimited JSON.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The open interest data. For JSON/NDJSON, this is a list of dictionaries
              with fields: timestamp, open_interest, expiration, strike, right. For CSV, this is
              a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get open interest for AAPL call options at a specific strike
        async with ThetaDataV3Client() as client:
            oi_data, url = await client.option_history_open_interest(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241104",
                strike=175.0,
                right="call"
            )
            print(f"Open interest: {oi_data}")

        # Get open interest for all strikes and both calls/puts
        oi_data, url = await client.option_history_open_interest(
            symbol="SPY",
            expiration="2024-11-15",
            date="2024-11-04",
            strike="*",
            right="both",
            format_type="json"
        )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "format": format_type,
        }
        return await self._make_request("/option/history/open_interest", params)

    async def option_history_greeks_eod(
        self,
        symbol: str,
        expiration: str,
        start_date: str,
        end_date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson", "html"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve end-of-day (EOD) Greeks for option contracts over a date range.

        This method fetches daily end-of-day option Greeks data including Delta, Gamma, Theta, Vega,
        and Rho for the specified date range. The Greeks are calculated using Theta Data's EOD reports
        generated at 17:15 ET using closing prices from the trading session. EOD Greeks provide a
        consistent daily snapshot of option risk metrics, making them ideal for daily risk assessment,
        portfolio monitoring, backtesting strategies over longer periods, and joining with daily OHLC
        data for comprehensive analysis. When expiration="*" (all expirations), requests must be issued
        day-by-day to avoid excessive data volume.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates,
            but note that using "*" requires issuing requests day-by-day to avoid large data volumes.
        start_date : str
            The inclusive start date for the date range. Acceptable formats: "YYYY-MM-DD"
            (e.g., "2024-11-01") or "YYYYMMDD" (e.g., "20241101").
        end_date : str
            The inclusive end date for the date range. Acceptable formats: "YYYY-MM-DD"
            (e.g., "2024-11-30") or "YYYYMMDD" (e.g., "20241130").
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
            When set to "*", returns data for all available strikes at the specified expiration.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query. "call" retrieves only call options, "put" retrieves
            only put options, and "both" retrieves both calls and puts in a single request.
        annual_dividend : float, optional
            Default: None (server uses default dividend data)
            The annualized dividend amount to override default values in Greeks calculations.
            Useful for custom modeling scenarios or sensitivity analysis.
        rate_type : str, optional
            Default: "sofr"
            Possible values: "sofr", "treasury_m1", "treasury_m3", "treasury_m6", "treasury_y1",
            "treasury_y2", "treasury_y3", "treasury_y5", "treasury_y7", "treasury_y10", "treasury_y20",
            "treasury_y30"
            The interest rate curve to use for Greeks calculations. SOFR (Secured Overnight Financing
            Rate) is recommended. Treasury rates of various maturities can be selected for specific needs.
        rate_value : float, optional
            Default: None (server uses current rate for rate_type)
            A custom risk-free interest rate percentage (e.g., 5.25 for 5.25%) to override the
            default rate. When specified, this takes precedence over the rate_type curve value.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson", "html"
            The desired response format. "csv" returns comma-separated values, "json" returns a
            JSON array, "ndjson" returns newline-delimited JSON, "html" returns formatted HTML table.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The EOD Greeks data. For JSON/NDJSON, this is a list of dictionaries
              with fields: timestamp, delta, gamma, theta, vega, rho, expiration, strike, right.
              Common join keys with daily OHLC: ["timestamp", "symbol", "expiration", "strike", "right"].
              For CSV/HTML, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get EOD Greeks for AAPL call options over a month
        async with ThetaDataV3Client() as client:
            greeks_eod, url = await client.option_history_greeks_eod(
                symbol="AAPL",
                expiration="2024-12-20",
                start_date="20241101",
                end_date="20241130",
                strike=175.0,
                right="call"
            )
            print(f"Retrieved EOD Greeks for {len(greeks_eod)} days")

        # Get EOD Greeks with custom rate for all strikes
        greeks_eod, url = await client.option_history_greeks_eod(
            symbol="SPY",
            expiration="2024-11-15",
            start_date="2024-11-01",
            end_date="2024-11-30",
            strike="*",
            right="both",
            rate_type="treasury_y1",
            rate_value=4.5,
            format_type="json"
        )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "start_date": start_date,
            "end_date": end_date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
    
        return await self._make_request("/option/history/greeks/eod", params)

        

    async def option_history_all_greeks(
        self,
        symbol: str,
        expiration: str,
        date: str,
        interval: Interval,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve all-order Greeks (first, second, and third order) at fixed intervals.

        This method fetches comprehensive option Greeks including all first-order (Delta, Gamma,
        Theta, Vega, Rho), second-order (Vomma, Veta, etc.), and third-order (Speed, Color, etc.)
        Greeks calculated from option and underlying mid prices at specified intervals. This provides
        the complete risk profile in a single request, ideal for comprehensive portfolio analysis
        and complete risk management systems. Requires Terminal v3 for computation.

        Parameters
        ----------
        symbol : str
            Underlying ticker symbol.
        expiration, date, interval, strike, right, start_time, end_time, annual_dividend, rate_type, rate_value, format_type
            Standard parameters (see other Greeks methods).

        Returns
        -------
        Tuple[Any, str]
            Tuple of (comprehensive Greeks data with all orders, request URL).

        Example Usage
        -------------
        async with ThetaDataV3Client() as client:
            all_greeks, url = await client.option_history_all_greeks(
                symbol="AAPL", expiration="2024-12-20", date="20241105",
                interval="1m", strike=175.0, right="call"
            )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "interval": interval,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/greeks/all", params)

    async def option_history_all_trade_greeks(
       self,
       symbol: str,
       expiration: str,
       date: str,
       strike: Optional[Union[float, str]] = "*",
       right: Literal["call", "put", "both"] = "both",
       start_time: Optional[str] = None,
       end_time: Optional[str] = None,
       annual_dividend: Optional[float] = None,
       rate_type: Literal[
           "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
           "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
           "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
       ] = "sofr",
       rate_value: Optional[float] = None,
       format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
       """Retrieve all-order Greeks (first, second, third) calculated at each trade (tick-based).

       This method fetches comprehensive option Greeks including all orders computed at every trade
       execution, using the most recent underlying price at each trade timestamp. This provides the
       complete risk profile at tick-level granularity, ideal for high-frequency risk analysis and
       understanding complete risk dynamics at each trade. No interval parameter needed as data is
       inherently trade-based.

       Parameters
       ----------
       symbol : str
           Underlying ticker symbol.
       expiration, date, strike, right, start_time, end_time, annual_dividend, rate_type, rate_value, format_type
           Standard parameters (see other trade Greeks methods).

       Returns
       -------
       Tuple[Any, str]
           Tuple of (comprehensive trade-level Greeks data with all orders, request URL).

       Example Usage
       -------------
       async with ThetaDataV3Client() as client:
           all_trade_greeks, url = await client.option_history_all_trade_greeks(
               symbol="AAPL", expiration="2024-12-20", date="20241105",
               strike=175.0, right="call"
           )
       """
       params = {
           "symbol": symbol,
           "expiration": expiration,
           "date": date,
           "strike": str(strike) if strike != "*" else "*",
           "right": right,
           "rate_type": rate_type,
           "format": format_type,
       }
       if start_time is not None:
           params["start_time"] = start_time
       if end_time is not None:
           params["end_time"] = end_time
       if annual_dividend is not None:
           params["annual_dividend"] = annual_dividend
       if rate_value is not None:
           params["rate_value"] = rate_value
           
       return await self._make_request("/option/history/trade_greeks/all", params)


        
    async def option_history_greeks(
        self,
        symbol: str,
        expiration: str,
        date: str,
        interval: Interval,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve first-order Greeks at fixed time intervals for option contracts.

        This method fetches first-order option Greeks (Delta, Gamma, Theta, Vega, and Rho) calculated
        from option mid prices at specified time intervals throughout a trading day. These are the
        fundamental risk metrics for option positions, essential for risk management, implementing
        delta-neutral strategies, monitoring portfolio Greeks exposure, and understanding option
        sensitivities to market changes at regular intervals.

        Parameters
        ----------
        symbol : str
            The underlying ticker symbol. Examples: "AAPL", "SPY", "TSLA".
        expiration : str
            Contract expiration date. Formats: "YYYY-MM-DD" or "YYYYMMDD", or "*" for all.
        date : str
            Trading date. Formats: "YYYY-MM-DD" or "YYYYMMDD".
        interval : Interval
            Aggregation interval. Examples: "1m", "5m", "1h". Common values for Greeks monitoring.
        strike : float or str, optional
            Default: "*"
            Strike price or "*" for all strikes.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
        start_time : str, optional
            Default: "09:30:00"
            Format: "HH:MM:SS". Eastern Time.
        end_time : str, optional
            Default: "16:00:00"
            Format: "HH:MM:SS". Eastern Time.
        annual_dividend : float, optional
            Default: None
            Annualized dividend for calculations.
        rate_type : str, optional
            Default: "sofr"
            Interest rate curve.
        rate_value : float, optional
            Default: None
            Custom rate percentage.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"

        Returns
        -------
        Tuple[Any, str]
            Tuple of (first-order Greeks data, request URL). Data includes: timestamp, delta, gamma,
            theta, vega, rho, expiration, strike, right.

        Example Usage
        -------------
        # Get 1-minute first-order Greeks for AAPL options
        async with ThetaDataV3Client() as client:
            greeks, url = await client.option_history_greeks(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241105",
                interval="1m",
                strike=175.0,
                right="call"
            )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "interval": interval,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/greeks/first_order", params)

    async def option_history_trade_greeks(
        self,
        symbol: str,
        expiration: str,
        date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve first-order Greeks calculated at each option trade (tick-based).

        This method fetches first-order option Greeks (Delta, Gamma, Theta, Vega, and Rho) computed
        at every trade execution. Each trade record includes the Greeks calculated using the most
        recent underlying price at the trade timestamp. This tick-level Greeks data provides the
        highest granularity for risk analysis, enabling precise trade-by-trade risk assessment,
        understanding how Greeks change with each transaction, and conducting high-frequency Greeks
        analysis. No interval parameter is required as the data is inherently trade-based.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates.
        date : str
            The specific trading date to query. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-11-05")
            or "YYYYMMDD" (e.g., "20241105"). Only data from this single date will be returned.
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
            When set to "*", returns data for all available strikes at the specified expiration.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query. "call" retrieves only call options, "put" retrieves
            only put options, and "both" retrieves both calls and puts in a single request.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day. Format: "HH:MM:SS" (e.g., "09:30:00", "10:00:00").
            Times are in America/New_York (Eastern Time) timezone. Only trades at or after this
            time will be included in the results.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day. Format: "HH:MM:SS" (e.g., "16:00:00", "14:30:00").
            Times are in America/New_York (Eastern Time) timezone. Only trades before this time
            will be included in the results.
        annual_dividend : float, optional
            Default: None (server uses default dividend data)
            The annualized dividend amount to use in Greeks calculations. Overrides default values
            when specified. Useful for sensitivity analysis or custom modeling assumptions.
        rate_type : str, optional
            Default: "sofr"
            Possible values: "sofr", "treasury_m1", "treasury_m3", "treasury_m6", "treasury_y1",
            "treasury_y2", "treasury_y3", "treasury_y5", "treasury_y7", "treasury_y10", "treasury_y20",
            "treasury_y30"
            The risk-free interest rate curve to use in Greeks calculations. SOFR is recommended.
        rate_value : float, optional
            Default: None (server uses current rate for rate_type)
            A custom risk-free interest rate percentage (e.g., 5.25 for 5.25%) to override the
            default rate. When specified, this takes precedence over the rate_type curve value.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format. "csv" returns comma-separated values, "json" returns
            a JSON array of trade Greeks objects, "ndjson" returns newline-delimited JSON.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The trade Greeks data. For JSON/NDJSON, this is a list of dictionaries
              with fields: timestamp, trade_price, delta, gamma, theta, vega, rho, underlying_price,
              expiration, strike, right. For CSV, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get tick-level first-order Greeks for AAPL call options
        async with ThetaDataV3Client() as client:
            trade_greeks, url = await client.option_history_trade_greeks(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241105",
                strike=175.0,
                right="call"
            )
            print(f"Retrieved {len(trade_greeks)} trade-level Greek calculations")

        # Analyze Greeks changes for all strikes during market hours
        trade_greeks, url = await client.option_history_trade_greeks(
            symbol="SPY",
            expiration="2024-11-15",
            date="2024-11-05",
            strike="*",
            right="both",
            start_time="09:30:00",
            end_time="16:00:00",
            format_type="json"
        )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/trade_greeks/first_order", params)

    async def option_history_greeks_second_order(
        self,
        symbol: str,
        expiration: str,
        date: str,
        interval: Interval,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve second-order Greeks at fixed time intervals for option contracts.

        This method fetches second-order option Greeks (Vomma, Veta, and other second derivatives)
        calculated from option mid prices at specified intervals. Second-order Greeks measure how
        first-order Greeks change, essential for advanced risk management and understanding option
        convexity dynamics at regular time intervals.

        Parameters
        ----------
        symbol : str
            Underlying ticker symbol.
        expiration : str
            Contract expiration. Formats: "YYYY-MM-DD" or "YYYYMMDD", or "*".
        date : str
            Trading date. Formats: "YYYY-MM-DD" or "YYYYMMDD".
        interval : Interval
            Aggregation interval. Examples: "1m", "5m", "1h".
        strike : float or str, optional
            Default: "*"
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
        start_time : str, optional
            Default: "09:30:00"
        end_time : str, optional
            Default: "16:00:00"
        annual_dividend : float, optional
            Default: None
        rate_type : str, optional
            Default: "sofr"
        rate_value : float, optional
            Default: None
        format_type : str, optional
            Default: "csv"

        Returns
        -------
        Tuple[Any, str]
            Tuple of (second-order Greeks data, request URL).

        Example Usage
        -------------
        async with ThetaDataV3Client() as client:
            greeks_2nd, url = await client.option_history_greeks_second_order(
                symbol="AAPL", expiration="2024-12-20", date="20241105",
                interval="1m", strike=175.0, right="call"
            )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "interval": interval,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/greeks/second_order", params)

    async def option_history_trade_greeks_second_order(
        self,
        symbol: str,
        expiration: str,
        date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve second-order Greeks calculated at each option trade (tick-based).

        This method fetches second-order option Greeks (including Vomma, Veta, and other second
        derivatives) computed at every trade execution. These advanced Greeks measure how first-order
        Greeks change with respect to volatility and time, providing deeper insights into option
        convexity and risk dynamics. Each trade record includes second-order Greeks calculated using
        the most recent underlying price at the trade timestamp. Essential for sophisticated risk
        management, volatility trading strategies, and understanding higher-order sensitivities.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates.
        date : str
            The specific trading date to query. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-11-05")
            or "YYYYMMDD" (e.g., "20241105"). Only data from this single date will be returned.
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day. Format: "HH:MM:SS". Times are in Eastern Time.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day. Format: "HH:MM:SS". Times are in Eastern Time.
        annual_dividend : float, optional
            Default: None (server uses default dividend data)
            The annualized dividend amount to use in Greeks calculations.
        rate_type : str, optional
            Default: "sofr"
            Possible values: "sofr", "treasury_m1", "treasury_m3", "treasury_m6", "treasury_y1",
            "treasury_y2", "treasury_y3", "treasury_y5", "treasury_y7", "treasury_y10", "treasury_y20",
            "treasury_y30"
            The risk-free interest rate curve to use in calculations.
        rate_value : float, optional
            Default: None (server uses current rate for rate_type)
            A custom risk-free interest rate percentage to override the default rate.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The trade second-order Greeks data with fields: timestamp, trade_price,
              vomma, veta, and other second derivatives, underlying_price, expiration, strike, right.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get tick-level second-order Greeks for AAPL call options
        async with ThetaDataV3Client() as client:
            greeks_2nd, url = await client.option_history_trade_greeks_second_order(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241105",
                strike=175.0,
                right="call"
            )
            print(f"Retrieved {len(greeks_2nd)} second-order Greek calculations")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/trade_greeks/second_order", params)

    async def option_history_greeks_third_order(
        self,
        symbol: str,
        expiration: str,
        date: str,
        interval: Interval,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve third-order Greeks at fixed time intervals for option contracts.

        This method fetches third-order option Greeks (Speed, Color, and other third derivatives)
        calculated from option mid prices at specified intervals. Third-order Greeks measure the
        rate of change of second-order Greeks, used for sophisticated portfolio management and
        higher-order risk analysis at regular time intervals.

        Parameters
        ----------
        symbol : str
            Underlying ticker symbol.
        expiration, date, interval, strike, right, start_time, end_time, annual_dividend, rate_type, rate_value, format_type
            Standard option parameters (see other Greeks methods for details).

        Returns
        -------
        Tuple[Any, str]
            Tuple of (third-order Greeks data, request URL).

        Example Usage
        -------------
        async with ThetaDataV3Client() as client:
            greeks_3rd, url = await client.option_history_greeks_third_order(
                symbol="AAPL", expiration="2024-12-20", date="20241105",
                interval="1m", strike=175.0, right="call"
            )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "interval": interval,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/greeks/third_order", params)

    async def option_history_trade_greeks_third_order(
        self,
        symbol: str,
        expiration: str,
        date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve third-order Greeks calculated at each option trade (tick-based).

        This method fetches third-order option Greeks (including Speed, Color, and other third
        derivatives) computed at every trade execution. These highly advanced Greeks measure the
        rate of change of second-order Greeks, providing the deepest level of sensitivity analysis
        for sophisticated option portfolios. Each trade record includes third-order Greeks calculated
        using the most recent underlying price. Used for advanced portfolio risk management and
        understanding extreme market sensitivities.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL", "SPY", "TSLA".
        expiration : str
            The option contract expiration date. Formats: "YYYY-MM-DD" or "YYYYMMDD", or "*" for all.
        date : str
            The specific trading date to query. Formats: "YYYY-MM-DD" or "YYYYMMDD".
        strike : float or str, optional
            Default: "*"
            The strike price (e.g., 150.0, 175.5) or "*" for all strikes.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
        start_time : str, optional
            Default: "09:30:00" (server default)
            Format: "HH:MM:SS". Eastern Time.
        end_time : str, optional
            Default: "16:00:00" (server default)
            Format: "HH:MM:SS". Eastern Time.
        annual_dividend : float, optional
            Default: None
            The annualized dividend amount for calculations.
        rate_type : str, optional
            Default: "sofr"
            The risk-free interest rate curve.
        rate_value : float, optional
            Default: None
            A custom risk-free rate percentage.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"

        Returns
        -------
        Tuple[Any, str]
            Tuple of (third-order Greeks data, request URL). Data includes: timestamp, trade_price,
            speed, color, and other third derivatives, underlying_price, expiration, strike, right.

        Example Usage
        -------------
        # Get tick-level third-order Greeks for AAPL options
        async with ThetaDataV3Client() as client:
            greeks_3rd, url = await client.option_history_trade_greeks_third_order(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241105",
                strike=175.0,
                right="call"
            )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/trade_greeks/third_order", params)

    async def option_history_implied_volatility(
        self,
        symbol: str,
        expiration: str,
        date: str,
        interval: Interval,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve implied volatility for option contracts at fixed time intervals.

        This method fetches implied volatility (IV) data calculated from option bid, mid, and ask
        prices at specified time intervals throughout a trading day. Implied volatility represents
        the market's expectation of future price volatility and is derived by solving the Black-Scholes
        model backwards using observed option prices. The method returns IV values at regular intervals
        (e.g., 1 minute, 5 minutes), making it essential for volatility trading strategies, monitoring
        volatility term structure, analyzing volatility skew, and implementing risk management systems.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL" (Apple Inc.), "SPY"
            (S&P 500 ETF), "TSLA" (Tesla Inc.). Must be a valid symbol with option data available.
        expiration : str
            The option contract expiration date. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-12-20")
            or "YYYYMMDD" (e.g., "20241220"). Use "*" to retrieve data for all expiration dates.
        date : str
            The specific trading date to query. Acceptable formats: "YYYY-MM-DD" (e.g., "2024-11-05")
            or "YYYYMMDD" (e.g., "20241105"). Only data from this single date will be returned.
        interval : Interval
            The aggregation interval for IV calculations. Possible values: "10ms", "100ms", "500ms",
            "1s", "5s", "10s", "15s", "30s", "1m", "5m", "10m", "15m", "30m", "1h". Common values
            for IV analysis include "1m" (1 minute), "5m" (5 minutes), and "1h" (1 hour).
        strike : float or str, optional
            Default: "*"
            The strike price of the option contract (e.g., 150.0, 175.5) or "*" for all strikes.
            When set to "*", returns data for all available strikes at the specified expiration.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
            The option right/type to query. "call" retrieves only call options, "put" retrieves
            only put options, and "both" retrieves both calls and puts in a single request.
        start_time : str, optional
            Default: "09:30:00" (server default)
            The start time within the trading day. Format: "HH:MM:SS" (e.g., "09:30:00", "10:00:00").
            Times are in America/New_York (Eastern Time) timezone. Only IV bars starting at or after
            this time will be included in the results.
        end_time : str, optional
            Default: "16:00:00" (server default)
            The end time within the trading day. Format: "HH:MM:SS" (e.g., "16:00:00", "14:30:00").
            Times are in America/New_York (Eastern Time) timezone. Only IV bars starting before this
            time will be included in the results.
        annual_dividend : float, optional
            Default: None (server uses default dividend data)
            The annualized dividend amount to use in IV calculations. Overrides the default dividend
            data when specified. Useful for sensitivity analysis or custom modeling assumptions.
        rate_type : str, optional
            Default: "sofr"
            Possible values: "sofr", "treasury_m1", "treasury_m3", "treasury_m6", "treasury_y1",
            "treasury_y2", "treasury_y3", "treasury_y5", "treasury_y7", "treasury_y10", "treasury_y20",
            "treasury_y30"
            The risk-free interest rate curve to use in IV calculations. SOFR (Secured Overnight
            Financing Rate) is the recommended default. Treasury rates of various maturities can
            be selected for specific modeling requirements.
        rate_value : float, optional
            Default: None (server uses current rate for rate_type)
            A custom risk-free interest rate percentage (e.g., 5.25 for 5.25%) to override the
            default rate. When specified, this takes precedence over the rate_type curve value.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"
            The desired response format. "csv" returns comma-separated values, "json" returns
            a JSON array of IV bar objects, "ndjson" returns newline-delimited JSON for streaming.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The implied volatility data. For JSON/NDJSON, this is a list of
              dictionaries with fields: timestamp, iv_bid, iv_mid, iv_ask, expiration, strike, right.
              For CSV, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Get 1-minute IV bars for AAPL call options
        async with ThetaDataV3Client() as client:
            iv_data, url = await client.option_history_implied_volatility(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241105",
                interval="1m",
                strike=175.0,
                right="call"
            )
            print(f"Retrieved {len(iv_data)} IV snapshots")

        # Get IV data with custom rate for sensitivity analysis
        iv_data, url = await client.option_history_implied_volatility(
            symbol="SPY",
            expiration="2024-11-15",
            date="2024-11-05",
            interval="5m",
            strike="*",
            right="both",
            rate_type="treasury_y1",
            rate_value=4.5,
            format_type="json"
        )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "interval": interval,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/greeks/implied_volatility", params)


    async def option_history_trade_implied_volatility(
        self,
        symbol: str,
        expiration: str,
        date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        annual_dividend: Optional[float] = None,
        rate_type: Literal[
            "sofr", "treasury_m1", "treasury_m3", "treasury_m6",
            "treasury_y1", "treasury_y2", "treasury_y3", "treasury_y5",
            "treasury_y7", "treasury_y10", "treasury_y20", "treasury_y30"
        ] = "sofr",
        rate_value: Optional[float] = None,
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """Retrieve implied volatility calculated at each option trade (tick-based).

        This method fetches implied volatility computed at every trade execution, using the trade
        price and the most recent underlying price at each trade timestamp. This tick-level IV data
        provides the highest granularity for volatility analysis, enabling trade-by-trade volatility
        assessment, identifying volatility shifts at transaction level, and understanding how implied
        volatility evolves with each trade throughout the trading session.

        Parameters
        ----------
        symbol : str
            The underlying ticker symbol. Examples: "AAPL", "SPY", "TSLA".
        expiration : str
            Contract expiration date. Formats: "YYYY-MM-DD" or "YYYYMMDD", or "*" for all.
        date : str
            Trading date. Formats: "YYYY-MM-DD" or "YYYYMMDD".
        strike : float or str, optional
            Default: "*"
            Strike price or "*" for all strikes.
        right : str, optional
            Default: "both"
            Possible values: "call", "put", "both"
        start_time : str, optional
            Default: "09:30:00"
            Format: "HH:MM:SS". Eastern Time.
        end_time : str, optional
            Default: "16:00:00"
            Format: "HH:MM:SS". Eastern Time.
        annual_dividend : float, optional
            Default: None
            Annualized dividend for IV calculations.
        rate_type : str, optional
            Default: "sofr"
            Interest rate curve for calculations.
        rate_value : float, optional
            Default: None
            Custom risk-free rate percentage.
        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"

        Returns
        -------
        Tuple[Any, str]
            Tuple of (trade IV data, request URL). Data includes: timestamp, trade_price, implied_vol,
            underlying_price, expiration, strike, right.

        Example Usage
        -------------
        # Get tick-level IV for AAPL call options
        async with ThetaDataV3Client() as client:
            trade_iv, url = await client.option_history_trade_implied_volatility(
                symbol="AAPL",
                expiration="2024-12-20",
                date="20241105",
                strike=175.0,
                right="call"
            )
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "date": date,
            "strike": str(strike) if strike != "*" else "*",
            "right": right,
            "rate_type": rate_type,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if annual_dividend is not None:
            params["annual_dividend"] = annual_dividend
        if rate_value is not None:
            params["rate_value"] = rate_value
            
        return await self._make_request("/option/history/trade_greeks/implied_volatility", params)


    # --- History (end) ---

    # --- At-Time (begin) ---

    async def option_at_time_trade(
        self,
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        format_type: Optional[Literal["csv", "json", "ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """
        Retrieve the last option trade at a specific time-of-day across a date range.

        This method fetches the most recent trade executed at or before a specified time-of-day for an
        option contract across multiple trading days from the ThetaData API. This is invaluable for
        analyzing option prices at consistent intraday moments (e.g., market open, midday, close) over
        time, conducting backtests that enter/exit at specific times, studying time-of-day pricing patterns,
        and comparing option premiums at standardized timestamps. The endpoint returns one trade record
        per day in the specified range, showing the last trade price, size, exchange, and conditions at
        the requested time. If no exact trade exists at the specified time, the last available trade prior
        to that moment is returned, ensuring you always get the most recent market information.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL", "SPY", "TSLA".

        expiration : str
            The option contract's expiration date in YYYYMMDD format (e.g., "20241220").

        strike : float
            The strike price of the option contract. Examples: 150.00, 175.50, 200.00.

        right : str
            Possible values: "C" (call), "P" (put)

            Specifies whether to retrieve trade data for a call or put option.

        start_date : str
            Start date (inclusive) for the date range in YYYYMMDD format (e.g., "20241101").

        end_date : str
            End date (inclusive) for the date range in YYYYMMDD format (e.g., "20241130").

        time_of_day : str
            The specific time within each trading day in America/New_York timezone. Format: "HH:MM:SS"
            or "HH:MM:SS.SSS" (e.g., "09:30:00" for market open, "16:00:00" for market close, "12:00:00.500"
            for midday with millisecond precision). The endpoint returns the last trade at or before this time.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            Response format selection.

        Returns
        -------
        Tuple[Any, str]
            - First element: Trade data for each date. For JSON, this is a list of dictionaries with fields
              like timestamp, price, size, exchange, conditions (one per date). For CSV/NDJSON, a formatted string.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Get market close prices for an option over a month
        async with ThetaDataV3Client() as client:
            trades, url = await client.option_at_time_trade(
                symbol="AAPL", expiration="20241220", strike=175.00, right="C",
                start_date="20241101", end_date="20241130", time_of_day="16:00:00"
            )
            for trade in trades:
                print(f"{trade['date']}: Close price ${trade['price']}")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "time_of_day": time_of_day,
            "format": format_type,
        }
        return await self._make_request("/option/at_time/trade", params)
    
    
    async def option_at_time_quote(
        self,
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        format_type: Optional[Literal["csv", "json", "ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """
        Retrieve the last option NBBO quote at a specific time-of-day across a date range.

        This method fetches the most recent National Best Bid and Offer (NBBO) quote at or before a
        specified time-of-day for an option contract across multiple trading days from the ThetaData API.
        This is essential for analyzing bid-ask spreads at consistent intraday times, studying liquidity
        patterns throughout the trading session, conducting backtests with realistic entry/exit pricing,
        and understanding how market maker quotes evolve at specific moments each day. The endpoint returns
        one quote record per day showing the best bid price, bid size, ask price, ask size, and spread at
        the requested time. If no exact quote exists at the specified time, the last available NBBO prior
        to that moment is returned, ensuring accurate representation of market conditions.

        Parameters
        ----------
        symbol : str
            The underlying stock or ETF ticker symbol. Examples: "AAPL", "SPY", "TSLA".

        expiration : str
            The option contract's expiration date in YYYYMMDD format (e.g., "20241220").

        strike : float
            The strike price of the option contract. Examples: 150.00, 175.50, 200.00.

        right : str
            Possible values: "C" (call), "P" (put)

            Specifies whether to retrieve quote data for a call or put option.

        start_date : str
            Start date (inclusive) for the date range in YYYYMMDD format (e.g., "20241101").

        end_date : str
            End date (inclusive) for the date range in YYYYMMDD format (e.g., "20241130").

        time_of_day : str
            The specific time within each trading day in America/New_York timezone. Format: "HH:MM:SS"
            or "HH:MM:SS.SSS" (e.g., "09:30:00", "15:59:00", "12:00:00.500"). The endpoint returns
            the last NBBO quote at or before this time each day.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            Response format selection.

        Returns
        -------
        Tuple[Any, str]
            - First element: Quote data for each date. For JSON, this is a list of dictionaries with
              fields like timestamp, bid_price, bid_size, ask_price, ask_size, spread, bid_exchange,
              ask_exchange (one per date). For CSV/NDJSON, a formatted string.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Analyze mid-day bid-ask spreads over a week
        async with ThetaDataV3Client() as client:
            quotes, url = await client.option_at_time_quote(
                symbol="SPY", expiration="20241220", strike=450.00, right="C",
                start_date="20241104", end_date="20241108", time_of_day="12:00:00"
            )
            for quote in quotes:
                mid = (quote['bid_price'] + quote['ask_price']) / 2
                print(f"{quote['date']}: Bid ${quote['bid_price']}, Ask ${quote['ask_price']}, Mid ${mid:.2f}")
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "time_of_day": time_of_day,
            "format": format_type,
        }
        return await self._make_request("/option/at_time/quote", params)
    
        
    # =====================================
    # =====================================
    
    



    # --- At-Time (end) ---

    # =========================================================================
    # (END)
    # OPTION
    # =========================================================================

    # =========================================================================
    # (BEGIN)
    # INDEX
    # =========================================================================

    # --- List (begin) ---

    async def index_list_symbols(self, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve a comprehensive list of all available market index symbols in the ThetaData database.

        This method queries the ThetaData API to obtain all index symbols for which data is available.
        The response includes major market indices such as SPX (S&P 500), VIX (Volatility Index),
        NDX (NASDAQ-100), DJX (Dow Jones), RUT (Russell 2000), and many others. This endpoint is
        essential for discovering supported indices before requesting specific historical or real-time
        index data. Index data is crucial for market analysis, benchmarking portfolio performance,
        tracking market volatility, and understanding broad market movements. The indices available
        typically include equity indices, volatility indices, sector indices, and other market benchmarks.

        Parameters
        ----------
        format_type : str, optional
            Default: "json"
            Possible values: ["csv", "json", "ndjson"]

            The desired response format for the data. Choose the format based on your downstream
            processing requirements and tooling ecosystem.
            - "json": Returns data as a JSON array of index symbols with metadata, ideal for
              programmatic parsing and integration
            - "csv": Returns data as comma-separated values, suitable for spreadsheet applications
              and data analysis tools
            - "ndjson": Returns data as newline-delimited JSON, useful for streaming and large-scale
              data processing

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: The list of index symbols. For JSON format, this is typically a list
              of strings or dictionaries containing symbol names and possibly additional metadata
              like full names or descriptions. For CSV/NDJSON formats, this is a formatted string.
            - Second element: The complete request URL as a string, including all query parameters,
              useful for logging, debugging, and audit trails.

        Example Usage
        -------------
        ```python
        async with ThetaDataV3Client() as client:
            # Get all available index symbols
            indices, url = await client.index_list_symbols(format_type="json")
            print(f"Found {len(indices)} available indices")

            # Display some major indices
            major_indices = ["SPX", "VIX", "NDX", "RUT", "DJX"]
            for idx in indices:
                if idx in major_indices:
                    print(f"Available: {idx}")

            # Get indices in CSV format for export
            indices_csv, url = await client.index_list_symbols(format_type="csv")
        ```
        """
        params = {"format": format_type}
        return await self._make_request("/index/list/symbols", params)
    
    async def index_list_dates(self, symbol: str, data_type: Literal["price", "ohlc"] = "price",
                              format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve all available dates for which index data exists for a specific symbol.

        This method queries the ThetaData API to obtain a comprehensive list of dates on which price or
        OHLC data is available for the specified index symbol. This is essential for verifying data coverage
        and availability before making bulk historical data requests, planning data downloads, identifying
        gaps in historical records, and understanding the temporal extent of available index data. All dates
        are returned in UTC timezone format. The endpoint helps ensure your analysis or backtests only request
        data for dates where information actually exists, avoiding unnecessary API calls and errors.

        Parameters
        ----------
        symbol : str
            The index ticker symbol to query. Examples: "SPX" (S&P 500 Index), "VIX" (CBOE Volatility Index),
            "NDX" (NASDAQ-100 Index), "DJX" (Dow Jones Industrial Average), "RUT" (Russell 2000 Index).
            Must be a valid index symbol available in the ThetaData database.

        data_type : Literal["price", "ohlc"], optional
            Default: "price"
            Possible values: "price", "ohlc"

            Specifies which type of index data to check availability for. "price" checks for tick-level
            price data availability (individual price updates throughout the day), while "ohlc" checks
            for aggregated OHLC bar data availability. Different data types may have different coverage
            periods depending on when ThetaData began collecting each type.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            The desired response format. "json" returns dates as a JSON array ideal for programmatic
            processing, "csv" returns comma-separated values, and "ndjson" returns newline-delimited JSON.

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: List of available dates. For JSON format, this is a list of date strings
              in YYYY-MM-DD format (UTC timezone). For CSV/NDJSON, this is a formatted string.
            - Second element: The complete request URL including all query parameters.

        Example Usage
        -------------
        # Check OHLC data availability for VIX index
        async with ThetaDataV3Client() as client:
            dates, url = await client.index_list_dates("VIX", data_type="ohlc")
            print(f"OHLC data available for {len(dates)} dates")
            print(f"First date: {dates[0]}, Last date: {dates[-1]}")
        """
        params = {
            "symbol": symbol,
            "type": data_type,
            "format": format_type
        }
        return await self._make_request("/index/list/dates", params)
    
    



    # --- List (end) ---

    # --- Snapshot (begin) ---

    async def index_snapshot_ohlc(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve the current OHLC snapshot for a specific market index.

        This method fetches the most recent Open, High, Low, Close data for the specified index from
        the ThetaData API. The snapshot represents either the current trading session if markets are
        open, or the most recent session if markets are closed. This real-time or near real-time data
        is essential for monitoring current market levels, understanding intraday market movements,
        assessing market volatility, and tracking major index performance. The response includes the
        opening price, highest price reached, lowest price reached, and current/closing price, along
        with a timestamp in UTC timezone indicating when the snapshot was captured.

        Parameters
        ----------
        symbol : str
            The index ticker symbol. Examples: "SPX" (S&P 500), "VIX" (Volatility Index), "NDX" (NASDAQ-100),
            "RUT" (Russell 2000). Must be a valid index symbol in the ThetaData database.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            Response format selection. "json" is ideal for programmatic use.

        Returns
        -------
        Tuple[Any, str]
            - First element: OHLC snapshot data. For JSON, a dictionary with open, high, low, close,
              timestamp fields. For CSV/NDJSON, a formatted string.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Monitor current S&P 500 levels
        async with ThetaDataV3Client() as client:
            ohlc, url = await client.index_snapshot_ohlc("SPX")
            print(f"SPX - Open: {ohlc['open']:.2f}, High: {ohlc['high']:.2f}")
            print(f"Low: {ohlc['low']:.2f}, Current: {ohlc['close']:.2f}")
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/index/snapshot/ohlc", params)
    
    async def index_snapshot_price(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve the current price snapshot for a specific market index.

        This method fetches the most recent price level for the specified index from the ThetaData API.
        The snapshot provides the current index value with a timestamp in UTC timezone, representing
        either real-time pricing if markets are open or the most recent value if markets are closed.
        This is critical for monitoring index levels in real-time, tracking market movements, comparing
        index performance, and building market overview dashboards. Unlike the OHLC snapshot which provides
        the session's range, this endpoint focuses specifically on the current index value, making it ideal
        for applications that need just the latest price without additional session statistics.

        Parameters
        ----------
        symbol : str
            The index ticker symbol. Examples: "SPX" (S&P 500), "VIX" (Volatility Index), "NDX" (NASDAQ-100),
            "DJX" (Dow Jones), "RUT" (Russell 2000). Must be a valid index in the ThetaData database.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            Response format selection.

        Returns
        -------
        Tuple[Any, str]
            - First element: Price snapshot. For JSON, a dictionary with price and timestamp fields.
              For CSV/NDJSON, a formatted string.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Get current VIX level for volatility monitoring
        async with ThetaDataV3Client() as client:
            price_data, url = await client.index_snapshot_price("VIX")
            print(f"Current VIX: {price_data['price']:.2f}")
            print(f"Timestamp: {price_data['timestamp']}")
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/index/snapshot/price", params)
    
    

    # --- Snapshot (end) ---

    # --- History (begin) ---

    async def index_history_eod(self, symbol: str, start_date: str, end_date: str,
                               format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Retrieve historical end-of-day data for a market index over a specified date range.

        This method fetches daily historical data for the specified index, providing a time series of
        end-of-day values including open, high, low, and close prices for each trading day in the range.
        All timestamps are in UTC timezone. This data is essential for long-term market analysis, index
        performance backtesting, correlation studies, historical volatility calculations, and understanding
        market trends over time. EOD data provides a consolidated view of each trading session, making it
        ideal for daily charting, identifying support/resistance levels, and analyzing market cycles without
        the granularity (and data volume) of intraday tick data.

        Parameters
        ----------
        symbol : str
            The index ticker symbol. Examples: "SPX" (S&P 500), "VIX" (Volatility Index), "NDX" (NASDAQ-100).

        start_date : str
            Start date (inclusive) for the historical range. Format: YYYY-MM-DD (e.g., "2024-01-01") in UTC.

        end_date : str
            End date (inclusive) for the historical range. Format: YYYY-MM-DD (e.g., "2024-12-31") in UTC.

        format_type : str, optional
            Default: "json"
            Possible values: "json", "csv", "ndjson"

            Response format selection.

        Returns
        -------
        Tuple[Any, str]
            - First element: Historical EOD data. For JSON, a list of dictionaries with date, open, high,
              low, close fields for each day. For CSV/NDJSON, a formatted string.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Analyze S&P 500 performance over a year
        async with ThetaDataV3Client() as client:
            eod_data, url = await client.index_history_eod(
                symbol="SPX", start_date="2024-01-01", end_date="2024-12-31"
            )
            for day in eod_data[:5]:
                print(f"{day['date']}: Close {day['close']:.2f}")
        """
        params = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "format": format_type
        }
        return await self._make_request("/index/history/eod", params)
    
    async def index_history_ohlc(
            self,
            symbol: str,
            start_date: str,
            end_date: str,
            interval: Interval = "1s",
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            format_type: Optional[Literal["csv", "json", "ndjson", "html"]] = "csv",
        ) -> Tuple[Any, str]:
            """Retrieve historical OHLC (Open-High-Low-Close) bars for a market index.

            This method fetches aggregated OHLC data for market indices over a specified date range
            with customizable time intervals. Indices include major benchmarks like SPX (S&P 500),
            VIX (volatility index), NDX (NASDAQ-100), and RUT (Russell 2000). The data provides
            comprehensive price information including timestamp, open, high, low, close, volume,
            count, and VWAP (Volume-Weighted Average Price). Essential for analyzing index movements,
            backtesting strategies against benchmarks, and understanding broader market dynamics.

            Parameters
            ----------
            symbol : str
                The index ticker symbol. Examples: "SPX" (S&P 500), "VIX" (CBOE Volatility Index),
                "NDX" (NASDAQ-100), "RUT" (Russell 2000).
            start_date : str
                Inclusive start date for the date range. Recommended format: "YYYYMMDD" (e.g., "20241101").
                Also accepts "YYYY-MM-DD" format.
            end_date : str
                Inclusive end date for the date range. Recommended format: "YYYYMMDD" (e.g., "20241130").
                Also accepts "YYYY-MM-DD" format.
            interval : Interval, optional
                Default: "1s"
                Possible values: "tick", "1s", "5s", "10s", "15s", "30s", "1m", "5m", "10m", "15m",
                "30m", "1h", "1d"
                The aggregation interval for OHLC bars.
            start_time : str, optional
                Default: "09:30:00" (server default)
                Intraday start time in HH:MM:SS format (e.g., "09:30:00"). Eastern Time timezone.
            end_time : str, optional
                Default: "16:00:00" (server default)
                Intraday end time in HH:MM:SS format (e.g., "16:00:00"). Eastern Time timezone.
            format_type : str, optional
                Default: "csv"
                Possible values: "csv", "json", "ndjson", "html"
                The desired response format.

            Returns
            -------
            Tuple[Any, str]
                Tuple of (OHLC data, request URL). Each record includes: timestamp (YYYY-MM-DDTHH:mm:ss.SSS),
                open, high, low, close, volume, count, vwap.

            Example Usage
            -------------
            # Get daily OHLC bars for SPX over a month
            async with ThetaDataV3Client() as client:
                spx_bars, url = await client.index_history_ohlc(
                    symbol="SPX",
                    start_date="20241101",
                    end_date="20241130",
                    interval="1d"
                )

            # Get 1-minute bars for VIX during market hours
            vix_bars, url = await client.index_history_ohlc(
                symbol="VIX",
                start_date="2024-11-01",
                end_date="2024-11-30",
                interval="1m",
                start_time="09:30:00",
                end_time="16:00:00",
                format_type="json"
            )
            """
            params = {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "interval": interval,
                "format": format_type,
            }
            if start_time is not None:
                params["start_time"] = start_time
            if end_time is not None:
                params["end_time"] = end_time
        
            return await self._make_request("/index/history/ohlc", params)

    
    async def index_history_price(
        self,
        symbol: str,
        date: str,
        interval: Interval = "1s",  # usa l'alias globale
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        format_type: Optional[Literal["csv", "json", "ndjson"]] = "csv",
    ) -> Tuple[Any, str]:
        """
        Retrieve historical price ticks for a market index on a specific trading date.

        This method fetches intraday price tick data or aggregated price bars for the specified index
        on a single trading date from the ThetaData API. The data can be retrieved at various intervals
        ranging from individual ticks to aggregated bars (1 second, 5 seconds, 1 minute, etc.). This is
        essential for intraday analysis, studying index movements throughout a trading session, analyzing
        market microstructure, and backtesting intraday strategies. The response includes timestamp, price,
        and potentially other metrics depending on the interval selected. All timestamps are in UTC timezone.

        Parameters
        ----------
        symbol : str
            The index ticker symbol. Examples: "SPX", "VIX", "NDX", "RUT".

        date : str
            The trading date for which to retrieve price data. Recommended format: YYYYMMDD (e.g., "20241105").
            Also accepts YYYY-MM-DD format.

        interval : Interval, optional
            Default: "1s"
            Possible values: "tick", "10ms", "100ms", "500ms", "1s", "5s", "10s", "15s", "30s",
            "1m", "5m", "10m", "15m", "30m", "1h", "1d"

            The aggregation interval for price data. "tick" returns every price update, while other
            values aggregate data into bars of the specified duration.

        start_time : str, optional
            Default: None (server default, typically 09:30:00)

            Intraday start time in HH:MM:SS format (e.g., "09:30:00"). Limits the data to times on or
            after this time.

        end_time : str, optional
            Default: None (server default, typically 16:00:00)

            Intraday end time in HH:MM:SS format (e.g., "16:00:00"). Limits the data to times before
            or at this time.

        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"

            Response format selection.

        Returns
        -------
        Tuple[Any, str]
            - First element: Price tick/bar data. For JSON, a list of records with timestamp and price.
              For CSV/NDJSON, a formatted string.
            - Second element: Complete request URL string.

        Example Usage
        -------------
        # Get 5-minute bars for SPX during market hours
        async with ThetaDataV3Client() as client:
            prices, url = await client.index_history_price(
                symbol="SPX", date="20241105", interval="5m",
                start_time="09:30:00", end_time="16:00:00"
            )
        """
        params = {
            "symbol": symbol,
            "date": date,          # consigliato YYYYMMDD in v3
            "interval": interval,
            "format": format_type,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
    
        return await self._make_request("/index/history/price", params)



    # --- History (end) ---

    # --- At-Time (begin) ---

    async def index_at_time_price(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        format_type: Optional[Literal["json", "csv", "ndjson"]] = "csv",
    ) -> Tuple[Any, str]:
        """
        Retrieve the index price at a specific time-of-day across a date range.

        This method fetches the index price at or before a specified time-of-day for each trading day
        in the given date range from the ThetaData API. This is invaluable for analyzing index levels at
        consistent intraday moments (e.g., market open, lunch time, market close) over multiple days,
        conducting backtests that reference index values at specific times, studying time-of-day patterns
        in market indices, and comparing index performance at standardized timestamps across different
        periods. The endpoint returns one price record per day showing the index value at the requested
        time. If no exact tick exists at the specified time, the last available price prior to that moment
        is returned, ensuring accurate representation of market conditions at that time.

        Parameters
        ----------
        symbol : str
            The index ticker symbol. Examples: "SPX" (S&P 500), "VIX" (Volatility Index), "NDX" (NASDAQ-100).
            Single symbol per request.

        start_date : str
            Start date (inclusive) for the date range. Recommended format: YYYYMMDD (e.g., "20241104").
            Also accepts YYYY-MM-DD format.

        end_date : str
            End date (inclusive) for the date range. Recommended format: YYYYMMDD (e.g., "20241108").
            Also accepts YYYY-MM-DD format.

        time_of_day : str
            The specific time within each trading day in America/New_York timezone. Format: "HH:MM:SS"
            or "HH:MM:SS.SSS" with millisecond precision (e.g., "09:30:00" for market open, "16:00:00"
            for market close, "12:00:00.500" for midday). The endpoint returns the index price at or
            immediately before this time each day.

        format_type : str, optional
            Default: "csv"
            Possible values: "csv", "json", "ndjson"

            Response format selection. The v3 API default is "csv".

        Returns
        -------
        Tuple[Any, str]
            A tuple containing two elements:
            - First element: Price data for each date. For JSON, this is a list of dictionaries with
              fields including timestamp (ISO-8601 format in UTC), symbol, and price (one record per date).
              For CSV/NDJSON, this is a formatted string.
            - Second element: The complete request URL as a string, useful for logging and debugging.

        Example Usage
        -------------
        # Get market close prices for S&P 500 over a week
        async with ThetaDataV3Client() as client:
            prices, url = await client.index_at_time_price(
                symbol="SPX",
                start_date="20241104",
                end_date="20241108",
                time_of_day="16:00:00"
            )
            for record in prices:
                print(f"{record['timestamp'][:10]}: SPX closed at {record['price']:.2f}")
        """
        params = {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "time_of_day": time_of_day,
            "format": format_type,
        }
        return await self._make_request("/index/at_time/price", params)
    

def extract_error_text(error_text: str) -> str:
    """
    Extract and clean meaningful error text from HTML-formatted HTTP error responses.

    This utility function processes raw HTTP error response text, which is often formatted as HTML,
    and extracts the relevant error message in a clean, readable format. It attempts multiple extraction
    strategies including parsing pre tags, h1 headers, and general HTML tag removal to ensure that
    users receive clear, actionable error messages rather than raw HTML markup. This is particularly
    useful when debugging API errors or handling exceptions in production environments where clear
    error messages are essential for troubleshooting.

    Parameters:
        error_text (str): The raw error response text from an HTTP response. This is typically HTML-
            formatted content returned by web servers when errors occur. Examples include 404 Not Found
            pages, 500 Internal Server Error pages, or custom error responses from the ThetaData API.
            Can be an empty string, in which case a placeholder message is returned.

    Returns:
        str: The cleaned, readable error text with HTML markup removed. Returns one of the following:
            - Text extracted from HTML pre tags if present (highest priority)
            - Text content following h1 headers if pre tags not found
            - All text with HTML tags stripped if structured extraction fails
            - "(Empty response body)" if the input is empty or whitespace-only
            - The original trimmed text if all extraction attempts fail

    Example Usage:
        This function is typically used internally by error handling code or can be called when
        processing ThetaDataV3HTTPError exceptions:

        ```python
        try:
            data, url = await client.stock_list_symbols()
        except ThetaDataV3HTTPError as e:
            clean_error = extract_error_text(e.raw)
            print(f"API Error: {clean_error}")
        ```
    """
    if not error_text:
        return "(Empty response body)"
    
    # Look for text in <pre> tags
    match = re.search(r"<pre[^>]*>(.*?)</pre>", error_text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Look for text after <h1>
    match = re.search(r"<h1[^>]*>.*?</h1>(.*)", error_text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Remove HTML tags and return text only
    text_only = re.sub(r'<[^>]+>', '', error_text)
    if text_only.strip():
        return text_only.strip()
    
    return error_text.strip()


async def test_all_endpoints():
    """
    Test all implemented endpoints (stock, option, index) and generate detailed report.
    
    Returns:
        pd.DataFrame: DataFrame with test results for every endpoint
    """
    # Test configuration
    client = ThetaDataV3Client()
    
    # Test symbols and parameters

    # Test parameters
    STOCK_SYMBOL = "AAPL"
    OPTION_SYMBOL_ROOT = "AAPL"
    OPTION_EXPIRATION = "20260116" 
    OPTION_DATE = "20220930"
    OPTION_STRIKE = 275.0
    OPTION_RIGHT = "call"
    INDEX_SYMBOL = "SPX"
    START_DATE = "2024-01-01"
    END_DATE = "2024-01-02"
    TEST_DATE = "2024-01-01"
    TEST_TIMESTAMP = "2024-01-01T15:30:00"
    INTERVAL_MS = 60000  # 1 minute
    OPTION_EXPIRATION_2  = "2024-11-15"  # o "20241115"
    OPTION_DATE_2        = "20241105"
    OPTION_STRIKE_2      = 220.00
    OPTION_EXPIRATION_3  = "20241108"
    OPTION_DATE_3        = "20241104"
    OPTION_STRIKE_3 = 220

# expiration=20241108&strike=220.000&right=call&start_date=20241104&end_date=20241104
#     OPTION_EXPIRATION_3
#     start = OPTION_DATE_3

    
    # Intervals validi dalla documentazione
    INTERVAL_TICK = "tick"
    INTERVAL_1M = "1m"
    INTERVAL_5M = "5m"
    INTERVAL_15M = "15m"
    INTERVAL_1H = "1h"
    
    # Complete test case list with corrected parameters
    test_cases = [
        # =====================================
        # =====================================

        # -------- STOCK EOD --------
        ("stock_history_eod",
            client.stock_history_eod,
            (STOCK_SYMBOL, START_DATE, END_DATE)),
        ("stock_history_eod_csv",
            client.stock_history_eod,
            (STOCK_SYMBOL, START_DATE, END_DATE, "csv")),
    
        # -------- INDEX EOD --------
        ("index_history_eod",
            client.index_history_eod,
            (INDEX_SYMBOL, START_DATE, END_DATE)),
        ("index_history_eod_csv",
            client.index_history_eod,
            (INDEX_SYMBOL, START_DATE, END_DATE, "csv")),
    
        # -------- OPTION EOD (OHLC 1d wrapper) --------
        # chain intera (stessa expiration) in un giorno
        # 4) Single contract, single day
    # 4) Single contract, single day
        ("option_history_eod_day_contract",
         client.option_history_eod,
         (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, OPTION_STRIKE_2, OPTION_RIGHT, "json")),
    
        # 5) All expirations, single day (chain-wide)
        ("option_history_eod_day_all_exp",
         client.option_history_eod,
         (OPTION_SYMBOL_ROOT, "*", OPTION_DATE_3, OPTION_DATE_3, "*", "both", "csv")),
    
        # 6) Single expiration (exp_2), chain for one day
        ("option_history_eod_day_chain_exp2",
         client.option_history_eod,
         (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, "*", "both", "csv")),

        # -------- OPTION EOD GREEKS --------
        # [8] EOD Greeks — chain for a single day (same expiration)
        ("option_history_greeks_eod_day_chain",
         client.option_history_greeks_eod,
         (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, "*", "both", None, "sofr", None, "json")),
        
        # [9] EOD Greeks — single contract for a single day
        ("option_history_greeks_eod_day_contract",
         client.option_history_greeks_eod,
         (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, OPTION_STRIKE_2, OPTION_RIGHT, None, "sofr", None, "json")),

        # chain intera (stessa expiration) in un giorno
        ("option_history_greeks_eod_day_chain",
            client.option_history_greeks_eod,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, "*", "both", None, "sofr", None, "json")),
        # singolo contratto in un giorno
        ("option_history_greeks_eod_day_contract",
            client.option_history_greeks_eod,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, OPTION_STRIKE_2, OPTION_RIGHT, None, "sofr", None, "json")),
        # range di giorni per stessa expiration
        ("option_history_greeks_eod_range_chain",
            client.option_history_greeks_eod,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, "*", "both", None, "sofr", None, "csv")),
        # tutte le expiration in un giorno
        ("option_history_greeks_eod_all_exp_day",
            client.option_history_greeks_eod,
            (OPTION_SYMBOL_ROOT, "*", OPTION_DATE_3, OPTION_DATE_3, "*", "both", None, "sofr", None, "csv")),
        
        ("stock_list_symbols", client.stock_list_symbols, ()),
        ("stock_list_symbols_csv", client.stock_list_symbols, ("csv",)),
        ("stock_list_dates_trade", client.stock_list_dates, (STOCK_SYMBOL, "trade")),
        ("stock_list_dates_quote", client.stock_list_dates, (STOCK_SYMBOL, "quote")),
        
        ("stock_snapshot_ohlc", client.stock_snapshot_ohlc, (STOCK_SYMBOL,)),
        ("stock_snapshot_ohlc_csv", client.stock_snapshot_ohlc, (STOCK_SYMBOL, "csv")),
        ("stock_snapshot_trade", client.stock_snapshot_trade, (STOCK_SYMBOL,)),
        ("stock_snapshot_trade_csv", client.stock_snapshot_trade, (STOCK_SYMBOL, "csv")),
        ("stock_snapshot_quote", client.stock_snapshot_quote, (STOCK_SYMBOL,)),
        ("stock_snapshot_quote_csv", client.stock_snapshot_quote, (STOCK_SYMBOL, "csv")),
        
        ("stock_history_eod", client.stock_history_eod, (STOCK_SYMBOL, START_DATE, END_DATE)),
        ("stock_history_eod_csv", client.stock_history_eod, (STOCK_SYMBOL, START_DATE, END_DATE, "csv")),
    
        # Basic intervals using existing parameters
        ("stock_history_ohlc_1m", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1M)),
        ("stock_history_ohlc_5m", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_5M)),
        ("stock_history_ohlc_15m", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_15M)),
        ("stock_history_ohlc_1h", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1H)),
        ("stock_history_ohlc_tick", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_TICK)),
        
        # With time ranges
        ("stock_history_ohlc_1m_morning", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1M, "09:30:00", "12:00:00")),
        ("stock_history_ohlc_5m_afternoon", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_5M, "13:00:00", "16:00:00")),
        ("stock_history_ohlc_1h_full_day", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1H, "09:30:00", "16:00:00")),
        
        # Different formats
        ("stock_history_ohlc_1m_json", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1M, None, None, "json")),
        ("stock_history_ohlc_5m_ndjson", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_5M, "09:30:00", "16:00:00", "ndjson")),
        ("stock_history_ohlc_15m_csv", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_15M, None, None, "csv")),
        
        # Using END_DATE as alternative
        ("stock_history_ohlc_end_date", client.stock_history_ohlc, 
         (STOCK_SYMBOL, END_DATE, INTERVAL_5M)),
         
        # Different symbol using INDEX_SYMBOL
        ("stock_history_ohlc_index_symbol", client.stock_history_ohlc, 
         (INDEX_SYMBOL, TEST_DATE, INTERVAL_1H, "10:00:00", "15:00:00", "json")),

        ("stock_history_trade", client.stock_history_trade, (STOCK_SYMBOL, TEST_DATE)),
        ("stock_history_trade_csv", client.stock_history_trade, (STOCK_SYMBOL, TEST_DATE, "csv")),
        ("stock_history_quote", client.stock_history_quote, (STOCK_SYMBOL, TEST_DATE.replace("-", ""), INTERVAL_1M, "09:30:00", "16:00:00", None, "json")),
        ("stock_history_quote_csv", client.stock_history_quote, (STOCK_SYMBOL, TEST_DATE.replace("-", ""), INTERVAL_1M, "09:30:00", "16:00:00", None, "csv")),
        ("stock_history_trade_quote", client.stock_history_trade_quote, (STOCK_SYMBOL, TEST_DATE.replace("-", ""), "09:30:00", "16:00:00", True, None, "json")),
        ("stock_history_trade_quote_csv", client.stock_history_trade_quote, (STOCK_SYMBOL, TEST_DATE.replace("-", ""), "09:30:00", "16:00:00", True, None, "csv")),

        ("stock_at_time_trade", client.stock_at_time_trade, (STOCK_SYMBOL, START_DATE.replace("-", ""), END_DATE.replace("-", ""), TEST_TIMESTAMP.split("T")[1], None, "json")),
        ("stock_at_time_quote", client.stock_at_time_quote, (STOCK_SYMBOL, START_DATE.replace("-", ""), END_DATE.replace("-", ""), TEST_TIMESTAMP.split("T")[1], None, "json")),
        
                
        
        # =====================================
        # =====================================
        # ("option_list_symbols", client.option_list_symbols, ()),
        # ("option_list_symbols_csv", client.option_list_symbols, ("csv",)),


        ("option_list_dates_trade", client.option_list_dates, (OPTION_SYMBOL_ROOT, "trade", OPTION_DATE)),
        ("option_list_dates_quote", client.option_list_dates, (OPTION_SYMBOL_ROOT, "quote", OPTION_DATE)),
        ("option_list_expirations", client.option_list_expirations, (OPTION_SYMBOL_ROOT,)),
        ("option_list_expirations_csv", client.option_list_expirations, (OPTION_SYMBOL_ROOT, "csv")),
        ("option_list_strikes", client.option_list_strikes, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION)),
        ("option_list_strikes_csv", client.option_list_strikes, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, "csv")),
        ("option_list_contracts_trade",       client.option_list_contracts, ("trade", OPTION_DATE, OPTION_SYMBOL_ROOT, "json")),
        ("option_list_contracts_trade_csv",   client.option_list_contracts, ("trade", OPTION_DATE, OPTION_SYMBOL_ROOT, "csv")),
        ("option_list_contracts_quote",       client.option_list_contracts, ("quote", OPTION_DATE, OPTION_SYMBOL_ROOT, "json")),
        ("option_list_contracts_quote_csv",   client.option_list_contracts, ("quote", OPTION_DATE, OPTION_SYMBOL_ROOT, "csv")),
            
        ("option_snapshot_ohlc", client.option_snapshot_ohlc, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_ohlc_csv", client.option_snapshot_ohlc, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT, "csv")),

        ("option_snapshot_trade",      client.option_snapshot_trade, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, "call")),
        ("option_snapshot_trade_put",  client.option_snapshot_trade, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, "put")),
        ("option_snapshot_quote",      client.option_snapshot_quote, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, "call")),
        ("option_snapshot_quote_put",  client.option_snapshot_quote, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, "put")),

        ("option_snapshot_open_interest", client.option_snapshot_open_interest, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_implied_volatility", client.option_snapshot_implied_volatility, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_all_greeks", client.option_snapshot_all_greeks, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_first_order_greeks", client.option_snapshot_first_order_greeks, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_second_order_greeks", client.option_snapshot_second_order_greeks, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_third_order_greeks", client.option_snapshot_third_order_greeks, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        
   

        # option_history_quote tests
        ("option_history_quote_tick",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2,
             "tick",          # puoi valutare "1s" o "1m" per ridurre ancora
             "*", "both",     # strike, right (riduci se vuoi meno dati)
             "10:00:00", "10:05:00",
             "json")),

            
        ("option_history_quote_1m_specific",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M, OPTION_STRIKE, "C")),  # "C" o "P", non "call"/"put"
            
        ("option_history_quote_all_strikes",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, "*", "C")),
            
        ("option_history_quote_with_times",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M, OPTION_STRIKE_2, "P", "09:30:00", "16:00:00", "csv")),
            
        # option_history_ohlc tests  
        ("option_history_ohlc_1m",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M)),

            
        ("option_history_ohlc_5m_json",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, OPTION_STRIKE_2, "P", "09:30:00", "16:00:00", "json")),
            
        ("option_history_ohlc_all_strikes",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_15M, "*", "C")),
            
        # option_history_open_interest tests
        ("option_history_open_interest_specific",
            client.option_history_open_interest,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE, "C")),
            
        ("option_history_open_interest_all",
            client.option_history_open_interest,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "C", "json")),
            
        # option_history_trade tests
        ("option_history_trade_all",
            client.option_history_trade,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2)),
            
        ("option_history_trade_specific",
            client.option_history_trade,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "C", "10:00:00", "14:00:00", "json")),
            
        # option_history_trade_quote tests
        ("option_history_trade_quote_default",
            client.option_history_trade_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_STRIKE_3, "call")),
            
        ("option_history_trade_quote_non_exclusive",
            client.option_history_trade_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, "*", "put", True)), # False
            
        # option_history_implied_volatility tests
        ("option_history_implied_volatility_1m",
            client.option_history_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M, OPTION_STRIKE, "C")),
            
        ("option_history_implied_volatility_custom",
            client.option_history_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, "*", "C", None, None, 0.5, "treasury_y1", None, "json")),
            
        # option_history_trade_greeks tests (no interval needed for trade-based)
        ("option_history_trade_greeks_first",
            client.option_history_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE, "C")),
            
        ("option_history_trade_greeks_with_dividend",
            client.option_history_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "P", None, None, 2.5, "sofr")),
            
        ("option_history_trade_greeks_second_order",
            client.option_history_trade_greeks_second_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "P")),
            
        ("option_history_trade_greeks_third_order",
            client.option_history_trade_greeks_third_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "", "both", "09:30:00", "12:00:00")),
            
        ("option_history_trade_implied_volatility",
            client.option_history_trade_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "C")),
            
        # option_history_greeks tests (interval required)
        ("option_history_greeks_first_1m",
            client.option_history_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M, OPTION_STRIKE, "C")),
            
        ("option_history_greeks_first_5m",
            client.option_history_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, "*", "P", None, None, 2.0, "treasury_y2")),
            
        ("option_history_greeks_second_order",
            client.option_history_greeks_second_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_15M, OPTION_STRIKE, "P")),
            
        ("option_history_greeks_third_order",
            client.option_history_greeks_third_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1H, "*", "C")),
            
        ("option_history_all_greeks",
            client.option_history_all_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, OPTION_STRIKE, "C")),
            
        ("option_history_all_trade_greeks",
            client.option_history_all_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "P", None, None, 1.5, "sofr", None, "json")),

        # OHLC (intraday bars)
        ("option_history_ohlc_1m",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1m", OPTION_STRIKE_2, OPTION_RIGHT)),
        ("option_history_ohlc_5m",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "5m", OPTION_STRIKE_2, OPTION_RIGHT, "09:30:00", "16:00:00", "json")),
    
        # Quote (ticks or last-per-interval)
        ("option_history_quote_1m",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1m", OPTION_STRIKE_2, "call")),
        ("option_history_quote_1s_json",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1s", OPTION_STRIKE_2, "both", "09:30:00", "10:00:00", "json")),
    
     
        # Open Interest (per date)
        ("option_history_open_interest",
            client.option_history_open_interest,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "call")),
        ("option_history_open_interest_chain",
            client.option_history_open_interest,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "both", "json")),
    
        # All Greeks (interval)
        ("option_history_all_greeks_5m",
            client.option_history_all_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "5m", OPTION_STRIKE_2, "call")),
        ("option_history_all_greeks_params",
            client.option_history_all_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1m", "*", "both", "09:30:00", "16:00:00", 0.0, "sofr", None, "json")),
    
        # All Trade Greeks (per trade)
        ("option_history_all_trade_greeks",
            client.option_history_all_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "call")),
        ("option_history_all_trade_greeks_chain",
            client.option_history_all_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "both", None, None, None, "sofr", None, "json")),
    
        # First/Second/Third Order Greeks (interval)
        ("option_history_first_order_greeks_1m",
            client.option_history_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1m",  OPTION_STRIKE_2, "call")),
        ("option_history_second_order_greeks_1h",
            client.option_history_greeks_second_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1h", "*", "both", "09:30:00", "16:00:00", "", "sofr", "", "json")),
        ("option_history_third_order_greeks_10m",
            client.option_history_greeks_third_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "10m", OPTION_STRIKE_2, "call")),
        
        ("option_history_first_order_trade_greeks",
            client.option_history_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2,
             "tick",                 # valuta se passare a "1s" o "1m" per ridurre ulteriormente
             OPTION_STRIKE_2, "call",
             "10:00:00", "10:05:00", # start_time, end_time (ET)
             None, "sofr", None, "json")),
        
        ("option_history_second_order_trade_greeks",
            client.option_history_greeks_second_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2,
             "tick",
             "*", "both",
             "10:00:00", "10:05:00",
             None, "sofr", None, "ndjson")),
        
        ("option_history_third_order_trade_greeks",
            client.option_history_greeks_third_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2,
             "tick",
             OPTION_STRIKE_2, "call",
             "10:00:00", "10:05:00",
             None, "sofr", None, "json")),


    
        # Implied Volatility (interval) and Trade Implied Volatility (per trade)
        ("option_history_implied_volatility_5m",
            client.option_history_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "5m", OPTION_STRIKE_2, "call")),
        ("option_history_trade_implied_volatility",
            client.option_history_trade_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "call", None, None, None, "sofr", None, "json")),

        
        ("option_at_time_trade", client.option_at_time_trade, (
            OPTION_SYMBOL_ROOT,
            OPTION_EXPIRATION,
            OPTION_STRIKE,
            OPTION_RIGHT,
            START_DATE.replace("-", ""),
            END_DATE.replace("-", ""),
            TEST_TIMESTAMP.split("T")[1],
        )),
        
        ("option_at_time_quote", client.option_at_time_quote, (
            OPTION_SYMBOL_ROOT,
            OPTION_EXPIRATION,
            OPTION_STRIKE,
            OPTION_RIGHT,
            START_DATE.replace("-", ""),
            END_DATE.replace("-", ""),
            TEST_TIMESTAMP.split("T")[1],
        )),

        # =====================================
        # =====================================
        
        ("index_list_symbols", client.index_list_symbols, ()),
        ("index_list_symbols_csv", client.index_list_symbols, ("csv",)),
        ("index_list_dates_price", client.index_list_dates, (INDEX_SYMBOL, "price")),
        ("index_list_dates_ohlc", client.index_list_dates, (INDEX_SYMBOL, "ohlc")),
        
        # ("index_snapshot_ohlc", client.index_snapshot_ohlc, (INDEX_SYMBOL,)),
        # ("index_snapshot_ohlc_csv", client.index_snapshot_ohlc, (INDEX_SYMBOL, "csv")),
        # ("index_snapshot_price", client.index_snapshot_price, (INDEX_SYMBOL,)),
        # ("index_snapshot_price_csv", client.index_snapshot_price, (INDEX_SYMBOL, "csv")),
        
        ("index_history_eod", client.index_history_eod, (INDEX_SYMBOL, START_DATE, END_DATE)),
        ("index_history_eod_csv", client.index_history_eod, (INDEX_SYMBOL, START_DATE, END_DATE, "csv")),

        ("index_history_ohlc_1min",
            client.index_history_ohlc,
            (INDEX_SYMBOL, START_DATE.replace("-", ""), END_DATE.replace("-", ""),
             INTERVAL_1M, "09:30:00", "09:45:00", "csv")),
        
        ("index_history_ohlc_5min",
            client.index_history_ohlc,
            (INDEX_SYMBOL, START_DATE.replace("-", ""), END_DATE.replace("-", ""),
             INTERVAL_5M, "13:00:00", "13:30:00", "csv")),
        
        # Price (v3: interval obbligatorio; aggiungi anche una finestra)
        ("index_history_price",
            client.index_history_price,
            (INDEX_SYMBOL, TEST_DATE.replace("-", ""), "1s", "09:30:00", "09:45:00", "csv")),
        
        ("index_history_price_csv",
            client.index_history_price,
            (INDEX_SYMBOL, TEST_DATE.replace("-", ""), "1s", "15:00:00", "15:15:00", "csv")),

        
        ("index_at_time_price", client.index_at_time_price, (
            INDEX_SYMBOL,
            START_DATE.replace("-", ""),
            END_DATE.replace("-", ""),
            TEST_TIMESTAMP.split("T")[1],
            "json",
        )),
        ("index_at_time_price_csv", client.index_at_time_price, (
            INDEX_SYMBOL,
            START_DATE.replace("-", ""),
            END_DATE.replace("-", ""),
            TEST_TIMESTAMP.split("T")[1],
            "csv",
        )),

    ]
    
    results = []
    
    async with client:
        print(f"Testing {len(test_cases)} endpoints...")
        
        for i, (test_name, func, args) in enumerate(test_cases, 1):
            print(f"[{i:2d}/{len(test_cases)}] Testing {test_name}...")
            
            try:
                data, url = await func(*args)

                # print(f"URL: {url}")
                # print(data)

                # Analyze the result (detect CSV vs JSON/NDJSON; report bytes and row count)
                import json, csv, io

                def _infer_rows_from_dict(d: dict) -> int:
                    # Common wrappers like {"data":[...]} or {"results":[...]}
                    for k in ("data", "results", "items", "rows", "records"):
                        v = d.get(k)
                        if isinstance(v, list):
                            return len(v)
                    # Dict-of-lists (columnar)
                    lens = [len(v) for v in d.values() if isinstance(v, (list, tuple))]
                    if lens:
                        return lens[0] if all(n == lens[0] for n in lens) else max(lens)
                    # Dict-of-dicts (e.g., pandas orient="columns" or "index")
                    lens = [len(v) for v in d.values() if isinstance(v, dict)]
                    if lens:
                        return lens[0] if all(n == lens[0] for n in lens) else max(lens)
                    # Fallback: single record
                    return 1

                if isinstance(data, (list, dict)):
                    # Already-parsed JSON
                    data_type = "JSON"
                    bytes_len = len(json.dumps(data, ensure_ascii=False, default=str).encode("utf-8", errors="ignore"))
                    rows = len(data) if isinstance(data, list) else _infer_rows_from_dict(data)
                    sample = json.dumps(
                        (data[:1] if isinstance(data, list) else {k: data[k] for k in list(data)[:5]}),
                        ensure_ascii=False, default=str
                    )[:200]
                elif isinstance(data, str):
                    bytes_len = len(data.encode("utf-8", errors="ignore"))
                    parsed = None
                    # Try JSON
                    try:
                        parsed = json.loads(data.strip())
                    except Exception:
                        parsed = None

                    if isinstance(parsed, list):
                        data_type = "JSON"
                        rows = len(parsed)
                        sample = json.dumps(parsed[:1], ensure_ascii=False, default=str)[:200]
                    elif isinstance(parsed, dict):
                        data_type = "JSON"
                        rows = _infer_rows_from_dict(parsed)
                        sample = json.dumps({k: parsed[k] for k in list(parsed)[:5]}, ensure_ascii=False, default=str)[:200]
                    else:
                        # Try NDJSON
                        lines = [ln for ln in data.splitlines() if ln.strip()]
                        ndjson_ok = 0
                        for ln in lines:
                            try:
                                json.loads(ln)
                                ndjson_ok += 1
                            except Exception:
                                pass
                        if ndjson_ok > 0 and ndjson_ok >= max(1, int(0.9 * len(lines))):
                            data_type = "NDJSON"
                            rows = ndjson_ok
                            sample = "\n".join(lines[:3])[:200]
                        else:
                            # Try CSV
                            try:
                                reader = csv.reader(io.StringIO(data))
                                rows_list = [r for r in reader if r and any(cell.strip() for cell in r)]
                                if rows_list:
                                    data_type = "CSV"
                                    rows = len(rows_list)
                                    sample = "\n".join(",".join(r) for r in rows_list[:3])[:200]
                                else:
                                    data_type = "Unknown"
                                    rows = None
                                    sample = data.strip()[:200]
                            except Exception:
                                data_type = "Unknown"
                                rows = None
                                sample = data.strip()[:200]
                else:
                    # Unknown / non-text payload
                    data_type = "Unknown"
                    text_repr = str(data)
                    bytes_len = len(text_repr.encode("utf-8", errors="ignore"))
                    rows = None
                    sample = text_repr[:200]

                data_size = f"{bytes_len} bytes" + (f", rows={rows}" if rows is not None else ", rows=?")


                
                # Determine endpoint category
                if test_name.startswith("stock_"):
                    category = "Stock"
                elif test_name.startswith("option_"):
                    category = "Option"
                elif test_name.startswith("index_"):
                    category = "Index"
                else:
                    category = "Other"
                
                results.append({
                    "Category": category,
                    "Test Name": test_name,
                    "Status": "✅ SUCCESS",
                    "Data Type": data_type,
                    "Data Rows": rows,
                    "Data Size (bytes)": data_size,
                    # "Sample": sample,
                    "URL": url,
                    "Error": ""
                })
                
            except ThetaDataV3HTTPError as e:
                error_msg = extract_error_text(e.raw)
                category = test_name.split("_")[0].title()
                
                results.append({
                    "Category": category,
                    "Test Name": test_name,
                    "Status": f"❌ HTTP {e.status}",
                    "Data Type": "",
                    "Data Size": 0,
                    # "Sample": "",
                    "URL": e.url,
                    "Error": error_msg[:500]
                })
                
            except Exception as e:
                category = test_name.split("_")[0].title()
                
                results.append({
                    "Category": category,
                    "Test Name": test_name,
                    "Status": "❌ EXCEPTION",
                    "Data Type": "",
                    "Data Size": 0,
                    # "Sample": "",
                    "URL": "",
                    "Error": str(e)[:500]
                })
            
            # Pause to avoid rate limiting
            await asyncio.sleep(0.5)
    
    # Create DataFrame with results
    df_results = pd.DataFrame(results)
    
    # Generate comprehensive statistics
    total_tests = len(results)
    successful_tests = len([r for r in results if r["Status"] == "✅ SUCCESS"])
    
    # Statistics by category
    category_stats = df_results.groupby('Category').agg({
        'Status': lambda x: (x == '✅ SUCCESS').sum(),
        'Test Name': 'count'
    }).rename(columns={'Status': 'Successful', 'Test Name': 'Total'})
    category_stats['Success Rate %'] = (category_stats['Successful'] / category_stats['Total'] * 100).round(1)
    
    # Convert to integers to avoid formatting issues
    category_stats['Successful'] = category_stats['Successful'].astype(int)
    category_stats['Total'] = category_stats['Total'].astype(int)
    
    # Print comprehensive report
    print(f"\n{'='*100}")
    print(f"COMPREHENSIVE TEST REPORT - ThetaData v3 API Client")
    print(f"{'='*100}")
    print(f"Overall Statistics:")
    print(f"  Total Tests: {total_tests}")
    print(f"  Successful: {successful_tests}")
    print(f"  Failed: {total_tests - successful_tests}")
    print(f"  Overall Success Rate: {(successful_tests / total_tests * 100):.1f}%")
    print(f"\nBy Category:")
    for category, stats in category_stats.iterrows():
        successful = int(stats['Successful'])
        total = int(stats['Total'])
        success_rate = stats['Success Rate %']
        print(f"  {category:<8}: {successful:2d}/{total:2d} ({success_rate:5.1f}%)")
    print(f"{'='*100}\n")
    
    return df_results
    
    # async def _expirations_that_traded(self, symbol: str, day_iso: str, req_type: str = "trade") -> list[str]:
    #     """
    #     Ritorna le expiration (YYYYMMDD) che hanno contratti quotati/scambiati in 'day_iso' per il root dato.
    #     Interroga /option/list/contracts/{req_type}?date=YYYYMMDD&symbol=ROOT e fa cache per la durata del processo.
    #     """
    #     from datetime import datetime
        
    #     day_ymd = self._td_ymd(day_iso)
    #     key = (symbol, day_ymd, req_type)
    #     cached = self._exp_by_day_cache.get(key)
    #     if cached is not None:
    #         return list(cached)
    
    #     try:
    #         payload, _ = await self.client.option_list_contracts(
    #             request_type=req_type, date=day_ymd, symbol=symbol, format_type="json"
    #         )
    #     except Exception:
    #         self._exp_by_day_cache[key] = []
    #         return []
    
    #     items = self._ensure_list(payload, keys=("data", "results", "items", "contracts"))
    #     exps: set[str] = set()
    #     for it in items:
    #         if isinstance(it, dict):
    #             exp = it.get("expiration") or it.get("expirationDate") or it.get("exp")
    #             if exp:
    #                 s = str(exp).replace("-", "")
    #                 if len(s) == 8 and s.isdigit():
    #                     exps.add(s)
    
    #     out = sorted(exps)
    #     self._exp_by_day_cache[key] = out
    #     return list(out)



# Helper function for Jupyter Notebook execution
async def run_comprehensive_tests():
    """Helper function to run all tests in Jupyter."""
    print("Helper function to run all tests in Jupyter.")
    results_df = await test_all_endpoints()
    display(results_df)
    return results_df


# Usage examples for Jupyter Notebook:
# 
# # Run comprehensive test suite
# results = await run_comprehensive_tests()
#
# # Filter results by category
# stock_results = results[results['Category'] == 'Stock']
# option_results = results[results['Category'] == 'Option']  
# index_results = results[results['Category'] == 'Index']
#
# # Show only failed tests
# failed_tests = results[results['Status'] != '✅ SUCCESS']
#
# # Basic client usage example
# async with ThetaDataV3Client(api_key="your_key") as client:
#     # Get stock data
#     symbols, url = await client.stock_list_symbols()
#     
#     # Get option data  
#     option_chains, url = await client.option_list_contracts("AAPL", "2024-03-15")
#     greeks, url = await client.option_snapshot_all_greeks("AAPL240315C00150000")
#     
#     # Get index data
#     spx_data, url = await client.index_history_eod("SPX", "2024-01-01", "2024-01-31")

    # --- At-Time (end) ---

    # =========================================================================
    # (END)
    # INDEX
    # =========================================================================

def extract_error_text(error_text: str) -> str:
    """
    Extract and clean meaningful error text from HTML-formatted HTTP error responses.

    This utility function processes raw HTTP error response text, which is often formatted as HTML,
    and extracts the relevant error message in a clean, readable format. It attempts multiple extraction
    strategies including parsing pre tags, h1 headers, and general HTML tag removal to ensure that
    users receive clear, actionable error messages rather than raw HTML markup. This is particularly
    useful when debugging API errors or handling exceptions in production environments where clear
    error messages are essential for troubleshooting.

    Parameters:
        error_text (str): The raw error response text from an HTTP response. This is typically HTML-
            formatted content returned by web servers when errors occur. Examples include 404 Not Found
            pages, 500 Internal Server Error pages, or custom error responses from the ThetaData API.
            Can be an empty string, in which case a placeholder message is returned.

    Returns:
        str: The cleaned, readable error text with HTML markup removed. Returns one of the following:
            - Text extracted from HTML pre tags if present (highest priority)
            - Text content following h1 headers if pre tags not found
            - All text with HTML tags stripped if structured extraction fails
            - "(Empty response body)" if the input is empty or whitespace-only
            - The original trimmed text if all extraction attempts fail

    Example Usage:
        This function is typically used internally by error handling code or can be called when
        processing ThetaDataV3HTTPError exceptions:

        ```python
        try:
            data, url = await client.stock_list_symbols()
        except ThetaDataV3HTTPError as e:
            clean_error = extract_error_text(e.raw)
            print(f"API Error: {clean_error}")
        ```
    """
    if not error_text:
        return "(Empty response body)"
    
    # Look for text in <pre> tags
    match = re.search(r"<pre[^>]*>(.*?)</pre>", error_text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Look for text after <h1>
    match = re.search(r"<h1[^>]*>.*?</h1>(.*)", error_text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Remove HTML tags and return text only
    text_only = re.sub(r'<[^>]+>', '', error_text)
    if text_only.strip():
        return text_only.strip()
    
    return error_text.strip()


async def test_all_endpoints():
    """
    Test all implemented endpoints (stock, option, index) and generate detailed report.
    
    Returns:
        pd.DataFrame: DataFrame with test results for every endpoint
    """
    # Test configuration
    client = ThetaDataV3Client()
    
    # Test symbols and parameters

    # Test parameters
    STOCK_SYMBOL = "AAPL"
    OPTION_SYMBOL_ROOT = "AAPL"
    OPTION_EXPIRATION = "20260116" 
    OPTION_DATE = "20220930"
    OPTION_STRIKE = 275.0
    OPTION_RIGHT = "call"
    INDEX_SYMBOL = "SPX"
    START_DATE = "2024-01-01"
    END_DATE = "2024-01-02"
    TEST_DATE = "2024-01-01"
    TEST_TIMESTAMP = "2024-01-01T15:30:00"
    INTERVAL_MS = 60000  # 1 minute
    OPTION_EXPIRATION_2  = "2024-11-15"  # o "20241115"
    OPTION_DATE_2        = "20241105"
    OPTION_STRIKE_2      = 220.00
    OPTION_EXPIRATION_3  = "20241108"
    OPTION_DATE_3        = "20241104"
    OPTION_STRIKE_3 = 220

# expiration=20241108&strike=220.000&right=call&start_date=20241104&end_date=20241104
#     OPTION_EXPIRATION_3
#     start = OPTION_DATE_3

    
    # Intervals validi dalla documentazione
    INTERVAL_TICK = "tick"
    INTERVAL_1M = "1m"
    INTERVAL_5M = "5m"
    INTERVAL_15M = "15m"
    INTERVAL_1H = "1h"
    
    # Complete test case list with corrected parameters
    test_cases = [
        # =====================================
        # STOCK ENDPOINTS (13 functions)
        # =====================================

        # -------- STOCK EOD --------
        ("stock_history_eod",
            client.stock_history_eod,
            (STOCK_SYMBOL, START_DATE, END_DATE)),
        ("stock_history_eod_csv",
            client.stock_history_eod,
            (STOCK_SYMBOL, START_DATE, END_DATE, "csv")),
    
        # -------- INDEX EOD --------
        ("index_history_eod",
            client.index_history_eod,
            (INDEX_SYMBOL, START_DATE, END_DATE)),
        ("index_history_eod_csv",
            client.index_history_eod,
            (INDEX_SYMBOL, START_DATE, END_DATE, "csv")),
    
        # -------- OPTION EOD (OHLC 1d wrapper) --------
        # chain intera (stessa expiration) in un giorno
        # 4) Single contract, single day
    # 4) Single contract, single day
        ("option_history_eod_day_contract",
         client.option_history_eod,
         (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, OPTION_STRIKE_2, OPTION_RIGHT, "json")),
    
        # 5) All expirations, single day (chain-wide)
        ("option_history_eod_day_all_exp",
         client.option_history_eod,
         (OPTION_SYMBOL_ROOT, "*", OPTION_DATE_3, OPTION_DATE_3, "*", "both", "csv")),
    
        # 6) Single expiration (exp_2), chain for one day
        ("option_history_eod_day_chain_exp2",
         client.option_history_eod,
         (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, "*", "both", "csv")),

        # -------- OPTION EOD GREEKS --------
        # [8] EOD Greeks — chain for a single day (same expiration)
        ("option_history_greeks_eod_day_chain",
         client.option_history_greeks_eod,
         (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, "*", "both", None, "sofr", None, "json")),
        
        # [9] EOD Greeks — single contract for a single day
        ("option_history_greeks_eod_day_contract",
         client.option_history_greeks_eod,
         (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, OPTION_STRIKE_2, OPTION_RIGHT, None, "sofr", None, "json")),

        # chain intera (stessa expiration) in un giorno
        ("option_history_greeks_eod_day_chain",
            client.option_history_greeks_eod,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, "*", "both", None, "sofr", None, "json")),
        # singolo contratto in un giorno
        ("option_history_greeks_eod_day_contract",
            client.option_history_greeks_eod,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, OPTION_STRIKE_2, OPTION_RIGHT, None, "sofr", None, "json")),
        # range di giorni per stessa expiration
        ("option_history_greeks_eod_range_chain",
            client.option_history_greeks_eod,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_DATE_3, "*", "both", None, "sofr", None, "csv")),
        # tutte le expiration in un giorno
        ("option_history_greeks_eod_all_exp_day",
            client.option_history_greeks_eod,
            (OPTION_SYMBOL_ROOT, "*", OPTION_DATE_3, OPTION_DATE_3, "*", "both", None, "sofr", None, "csv")),
        
        # --- LIST ENDPOINTS (2 functions) ---
        ("stock_list_symbols", client.stock_list_symbols, ()),
        ("stock_list_symbols_csv", client.stock_list_symbols, ("csv",)),
        ("stock_list_dates_trade", client.stock_list_dates, (STOCK_SYMBOL, "trade")),
        ("stock_list_dates_quote", client.stock_list_dates, (STOCK_SYMBOL, "quote")),
        
        # --- SNAPSHOT ENDPOINTS (3 functions) ---
        ("stock_snapshot_ohlc", client.stock_snapshot_ohlc, (STOCK_SYMBOL,)),
        ("stock_snapshot_ohlc_csv", client.stock_snapshot_ohlc, (STOCK_SYMBOL, "csv")),
        ("stock_snapshot_trade", client.stock_snapshot_trade, (STOCK_SYMBOL,)),
        ("stock_snapshot_trade_csv", client.stock_snapshot_trade, (STOCK_SYMBOL, "csv")),
        ("stock_snapshot_quote", client.stock_snapshot_quote, (STOCK_SYMBOL,)),
        ("stock_snapshot_quote_csv", client.stock_snapshot_quote, (STOCK_SYMBOL, "csv")),
        
        # --- HISTORY ENDPOINTS (5 functions) ---
        ("stock_history_eod", client.stock_history_eod, (STOCK_SYMBOL, START_DATE, END_DATE)),
        ("stock_history_eod_csv", client.stock_history_eod, (STOCK_SYMBOL, START_DATE, END_DATE, "csv")),
    
        # Basic intervals using existing parameters
        ("stock_history_ohlc_1m", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1M)),
        ("stock_history_ohlc_5m", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_5M)),
        ("stock_history_ohlc_15m", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_15M)),
        ("stock_history_ohlc_1h", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1H)),
        ("stock_history_ohlc_tick", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_TICK)),
        
        # With time ranges
        ("stock_history_ohlc_1m_morning", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1M, "09:30:00", "12:00:00")),
        ("stock_history_ohlc_5m_afternoon", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_5M, "13:00:00", "16:00:00")),
        ("stock_history_ohlc_1h_full_day", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1H, "09:30:00", "16:00:00")),
        
        # Different formats
        ("stock_history_ohlc_1m_json", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_1M, None, None, "json")),
        ("stock_history_ohlc_5m_ndjson", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_5M, "09:30:00", "16:00:00", "ndjson")),
        ("stock_history_ohlc_15m_csv", client.stock_history_ohlc, 
         (STOCK_SYMBOL, TEST_DATE, INTERVAL_15M, None, None, "csv")),
        
        # Using END_DATE as alternative
        ("stock_history_ohlc_end_date", client.stock_history_ohlc, 
         (STOCK_SYMBOL, END_DATE, INTERVAL_5M)),
         
        # Different symbol using INDEX_SYMBOL
        ("stock_history_ohlc_index_symbol", client.stock_history_ohlc, 
         (INDEX_SYMBOL, TEST_DATE, INTERVAL_1H, "10:00:00", "15:00:00", "json")),

        ("stock_history_trade", client.stock_history_trade, (STOCK_SYMBOL, TEST_DATE)),
        ("stock_history_trade_csv", client.stock_history_trade, (STOCK_SYMBOL, TEST_DATE, "csv")),
        ("stock_history_quote", client.stock_history_quote, (STOCK_SYMBOL, TEST_DATE.replace("-", ""), INTERVAL_1M, "09:30:00", "16:00:00", None, "json")),
        ("stock_history_quote_csv", client.stock_history_quote, (STOCK_SYMBOL, TEST_DATE.replace("-", ""), INTERVAL_1M, "09:30:00", "16:00:00", None, "csv")),
        ("stock_history_trade_quote", client.stock_history_trade_quote, (STOCK_SYMBOL, TEST_DATE.replace("-", ""), "09:30:00", "16:00:00", True, None, "json")),
        ("stock_history_trade_quote_csv", client.stock_history_trade_quote, (STOCK_SYMBOL, TEST_DATE.replace("-", ""), "09:30:00", "16:00:00", True, None, "csv")),

        # --- AT-TIME ENDPOINTS (2 functions) ---
        ("stock_at_time_trade", client.stock_at_time_trade, (STOCK_SYMBOL, START_DATE.replace("-", ""), END_DATE.replace("-", ""), TEST_TIMESTAMP.split("T")[1], None, "json")),
        ("stock_at_time_quote", client.stock_at_time_quote, (STOCK_SYMBOL, START_DATE.replace("-", ""), END_DATE.replace("-", ""), TEST_TIMESTAMP.split("T")[1], None, "json")),
        
                
        
        # =====================================
        # OPTION ENDPOINTS (44 functions)
        # =====================================
        # OPTION_DATE
        # --- LIST ENDPOINTS (5 functions) ---
        # ("option_list_symbols", client.option_list_symbols, ()),
        # ("option_list_symbols_csv", client.option_list_symbols, ("csv",)),


        ("option_list_dates_trade", client.option_list_dates, (OPTION_SYMBOL_ROOT, "trade", OPTION_DATE)),
        ("option_list_dates_quote", client.option_list_dates, (OPTION_SYMBOL_ROOT, "quote", OPTION_DATE)),
        ("option_list_expirations", client.option_list_expirations, (OPTION_SYMBOL_ROOT,)),
        ("option_list_expirations_csv", client.option_list_expirations, (OPTION_SYMBOL_ROOT, "csv")),
        ("option_list_strikes", client.option_list_strikes, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION)),
        ("option_list_strikes_csv", client.option_list_strikes, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, "csv")),
        ("option_list_contracts_trade",       client.option_list_contracts, ("trade", OPTION_DATE, OPTION_SYMBOL_ROOT, "json")),
        ("option_list_contracts_trade_csv",   client.option_list_contracts, ("trade", OPTION_DATE, OPTION_SYMBOL_ROOT, "csv")),
        ("option_list_contracts_quote",       client.option_list_contracts, ("quote", OPTION_DATE, OPTION_SYMBOL_ROOT, "json")),
        ("option_list_contracts_quote_csv",   client.option_list_contracts, ("quote", OPTION_DATE, OPTION_SYMBOL_ROOT, "csv")),
            
        # --- SNAPSHOT ENDPOINTS (9 functions) ---
        ("option_snapshot_ohlc", client.option_snapshot_ohlc, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_ohlc_csv", client.option_snapshot_ohlc, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT, "csv")),

        ("option_snapshot_trade",      client.option_snapshot_trade, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, "call")),
        ("option_snapshot_trade_put",  client.option_snapshot_trade, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, "put")),
        ("option_snapshot_quote",      client.option_snapshot_quote, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, "call")),
        ("option_snapshot_quote_put",  client.option_snapshot_quote, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, "put")),

        ("option_snapshot_open_interest", client.option_snapshot_open_interest, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_implied_volatility", client.option_snapshot_implied_volatility, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_all_greeks", client.option_snapshot_all_greeks, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_first_order_greeks", client.option_snapshot_first_order_greeks, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_second_order_greeks", client.option_snapshot_second_order_greeks, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        ("option_snapshot_third_order_greeks", client.option_snapshot_third_order_greeks, (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION, OPTION_STRIKE, OPTION_RIGHT)),
        
        # --- HISTORY ENDPOINTS (28 functions) ---
   

        # option_history_quote tests
        ("option_history_quote_tick",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2,
             "tick",          # puoi valutare "1s" o "1m" per ridurre ancora
             "*", "both",     # strike, right (riduci se vuoi meno dati)
             "10:00:00", "10:05:00",
             "json")),

            
        ("option_history_quote_1m_specific",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M, OPTION_STRIKE, "C")),  # "C" o "P", non "call"/"put"
            
        ("option_history_quote_all_strikes",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, "*", "C")),
            
        ("option_history_quote_with_times",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M, OPTION_STRIKE_2, "P", "09:30:00", "16:00:00", "csv")),
            
        # option_history_ohlc tests  
        ("option_history_ohlc_1m",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M)),

            
        ("option_history_ohlc_5m_json",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, OPTION_STRIKE_2, "P", "09:30:00", "16:00:00", "json")),
            
        ("option_history_ohlc_all_strikes",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_15M, "*", "C")),
            
        # option_history_open_interest tests
        ("option_history_open_interest_specific",
            client.option_history_open_interest,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE, "C")),
            
        ("option_history_open_interest_all",
            client.option_history_open_interest,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "C", "json")),
            
        # option_history_trade tests
        ("option_history_trade_all",
            client.option_history_trade,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2)),
            
        ("option_history_trade_specific",
            client.option_history_trade,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "C", "10:00:00", "14:00:00", "json")),
            
        # option_history_trade_quote tests
        ("option_history_trade_quote_default",
            client.option_history_trade_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, OPTION_STRIKE_3, "call")),
            
        ("option_history_trade_quote_non_exclusive",
            client.option_history_trade_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_3, OPTION_DATE_3, "*", "put", True)), # False
            
        # option_history_implied_volatility tests
        ("option_history_implied_volatility_1m",
            client.option_history_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M, OPTION_STRIKE, "C")),
            
        ("option_history_implied_volatility_custom",
            client.option_history_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, "*", "C", None, None, 0.5, "treasury_y1", None, "json")),
            
        # option_history_trade_greeks tests (no interval needed for trade-based)
        ("option_history_trade_greeks_first",
            client.option_history_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE, "C")),
            
        ("option_history_trade_greeks_with_dividend",
            client.option_history_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "P", None, None, 2.5, "sofr")),
            
        ("option_history_trade_greeks_second_order",
            client.option_history_trade_greeks_second_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "P")),
            
        ("option_history_trade_greeks_third_order",
            client.option_history_trade_greeks_third_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "", "both", "09:30:00", "12:00:00")),
            
        ("option_history_trade_implied_volatility",
            client.option_history_trade_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "C")),
            
        # option_history_greeks tests (interval required)
        ("option_history_greeks_first_1m",
            client.option_history_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1M, OPTION_STRIKE, "C")),
            
        ("option_history_greeks_first_5m",
            client.option_history_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, "*", "P", None, None, 2.0, "treasury_y2")),
            
        ("option_history_greeks_second_order",
            client.option_history_greeks_second_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_15M, OPTION_STRIKE, "P")),
            
        ("option_history_greeks_third_order",
            client.option_history_greeks_third_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_1H, "*", "C")),
            
        ("option_history_all_greeks",
            client.option_history_all_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, INTERVAL_5M, OPTION_STRIKE, "C")),
            
        ("option_history_all_trade_greeks",
            client.option_history_all_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "P", None, None, 1.5, "sofr", None, "json")),

        # OHLC (intraday bars)
        ("option_history_ohlc_1m",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1m", OPTION_STRIKE_2, OPTION_RIGHT)),
        ("option_history_ohlc_5m",
            client.option_history_ohlc,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "5m", OPTION_STRIKE_2, OPTION_RIGHT, "09:30:00", "16:00:00", "json")),
    
        # Quote (ticks or last-per-interval)
        ("option_history_quote_1m",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1m", OPTION_STRIKE_2, "call")),
        ("option_history_quote_1s_json",
            client.option_history_quote,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1s", OPTION_STRIKE_2, "both", "09:30:00", "10:00:00", "json")),
    
     
        # Open Interest (per date)
        ("option_history_open_interest",
            client.option_history_open_interest,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "call")),
        ("option_history_open_interest_chain",
            client.option_history_open_interest,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "both", "json")),
    
        # All Greeks (interval)
        ("option_history_all_greeks_5m",
            client.option_history_all_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "5m", OPTION_STRIKE_2, "call")),
        ("option_history_all_greeks_params",
            client.option_history_all_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1m", "*", "both", "09:30:00", "16:00:00", 0.0, "sofr", None, "json")),
    
        # All Trade Greeks (per trade)
        ("option_history_all_trade_greeks",
            client.option_history_all_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "call")),
        ("option_history_all_trade_greeks_chain",
            client.option_history_all_trade_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "*", "both", None, None, None, "sofr", None, "json")),
    
        # First/Second/Third Order Greeks (interval)
        ("option_history_first_order_greeks_1m",
            client.option_history_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1m",  OPTION_STRIKE_2, "call")),
        ("option_history_second_order_greeks_1h",
            client.option_history_greeks_second_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "1h", "*", "both", "09:30:00", "16:00:00", "", "sofr", "", "json")),
        ("option_history_third_order_greeks_10m",
            client.option_history_greeks_third_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "10m", OPTION_STRIKE_2, "call")),
        
        ("option_history_first_order_trade_greeks",
            client.option_history_greeks,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2,
             "tick",                 # valuta se passare a "1s" o "1m" per ridurre ulteriormente
             OPTION_STRIKE_2, "call",
             "10:00:00", "10:05:00", # start_time, end_time (ET)
             None, "sofr", None, "json")),
        
        ("option_history_second_order_trade_greeks",
            client.option_history_greeks_second_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2,
             "tick",
             "*", "both",
             "10:00:00", "10:05:00",
             None, "sofr", None, "ndjson")),
        
        ("option_history_third_order_trade_greeks",
            client.option_history_greeks_third_order,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2,
             "tick",
             OPTION_STRIKE_2, "call",
             "10:00:00", "10:05:00",
             None, "sofr", None, "json")),


    
        # Implied Volatility (interval) and Trade Implied Volatility (per trade)
        ("option_history_implied_volatility_5m",
            client.option_history_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, "5m", OPTION_STRIKE_2, "call")),
        ("option_history_trade_implied_volatility",
            client.option_history_trade_implied_volatility,
            (OPTION_SYMBOL_ROOT, OPTION_EXPIRATION_2, OPTION_DATE_2, OPTION_STRIKE_2, "call", None, None, None, "sofr", None, "json")),

        
        # --- AT-TIME ENDPOINTS (2 functions) ---
        ("option_at_time_trade", client.option_at_time_trade, (
            OPTION_SYMBOL_ROOT,
            OPTION_EXPIRATION,
            OPTION_STRIKE,
            OPTION_RIGHT,
            START_DATE.replace("-", ""),
            END_DATE.replace("-", ""),
            TEST_TIMESTAMP.split("T")[1],
        )),
        
        ("option_at_time_quote", client.option_at_time_quote, (
            OPTION_SYMBOL_ROOT,
            OPTION_EXPIRATION,
            OPTION_STRIKE,
            OPTION_RIGHT,
            START_DATE.replace("-", ""),
            END_DATE.replace("-", ""),
            TEST_TIMESTAMP.split("T")[1],
        )),

        # =====================================
        # INDEX ENDPOINTS (10 functions)
        # =====================================
        
        # --- LIST ENDPOINTS (2 functions) ---
        ("index_list_symbols", client.index_list_symbols, ()),
        ("index_list_symbols_csv", client.index_list_symbols, ("csv",)),
        ("index_list_dates_price", client.index_list_dates, (INDEX_SYMBOL, "price")),
        ("index_list_dates_ohlc", client.index_list_dates, (INDEX_SYMBOL, "ohlc")),
        
        # --- SNAPSHOT ENDPOINTS (2 functions) REAL TIME STREAMING ---
        # ("index_snapshot_ohlc", client.index_snapshot_ohlc, (INDEX_SYMBOL,)),
        # ("index_snapshot_ohlc_csv", client.index_snapshot_ohlc, (INDEX_SYMBOL, "csv")),
        # ("index_snapshot_price", client.index_snapshot_price, (INDEX_SYMBOL,)),
        # ("index_snapshot_price_csv", client.index_snapshot_price, (INDEX_SYMBOL, "csv")),
        
        # --- HISTORY ENDPOINTS (3 functions) ---
        ("index_history_eod", client.index_history_eod, (INDEX_SYMBOL, START_DATE, END_DATE)),
        ("index_history_eod_csv", client.index_history_eod, (INDEX_SYMBOL, START_DATE, END_DATE, "csv")),

        ("index_history_ohlc_1min",
            client.index_history_ohlc,
            (INDEX_SYMBOL, START_DATE.replace("-", ""), END_DATE.replace("-", ""),
             INTERVAL_1M, "09:30:00", "09:45:00", "csv")),
        
        ("index_history_ohlc_5min",
            client.index_history_ohlc,
            (INDEX_SYMBOL, START_DATE.replace("-", ""), END_DATE.replace("-", ""),
             INTERVAL_5M, "13:00:00", "13:30:00", "csv")),
        
        # Price (v3: interval obbligatorio; aggiungi anche una finestra)
        ("index_history_price",
            client.index_history_price,
            (INDEX_SYMBOL, TEST_DATE.replace("-", ""), "1s", "09:30:00", "09:45:00", "csv")),
        
        ("index_history_price_csv",
            client.index_history_price,
            (INDEX_SYMBOL, TEST_DATE.replace("-", ""), "1s", "15:00:00", "15:15:00", "csv")),

        
        # --- AT-TIME ENDPOINTS (1 function) ---
        ("index_at_time_price", client.index_at_time_price, (
            INDEX_SYMBOL,
            START_DATE.replace("-", ""),
            END_DATE.replace("-", ""),
            TEST_TIMESTAMP.split("T")[1],
            "json",
        )),
        ("index_at_time_price_csv", client.index_at_time_price, (
            INDEX_SYMBOL,
            START_DATE.replace("-", ""),
            END_DATE.replace("-", ""),
            TEST_TIMESTAMP.split("T")[1],
            "csv",
        )),

    ]
    
    results = []
    
    async with client:
        print(f"Testing {len(test_cases)} endpoints...")
        
        for i, (test_name, func, args) in enumerate(test_cases, 1):
            print(f"[{i:2d}/{len(test_cases)}] Testing {test_name}...")
            
            try:
                data, url = await func(*args)

                # print(f"URL: {url}")
                # print(data)

                # Analyze the result (detect CSV vs JSON/NDJSON; report bytes and row count)
                import json, csv, io

                def _infer_rows_from_dict(d: dict) -> int:
                    # Common wrappers like {"data":[...]} or {"results":[...]}
                    for k in ("data", "results", "items", "rows", "records"):
                        v = d.get(k)
                        if isinstance(v, list):
                            return len(v)
                    # Dict-of-lists (columnar)
                    lens = [len(v) for v in d.values() if isinstance(v, (list, tuple))]
                    if lens:
                        return lens[0] if all(n == lens[0] for n in lens) else max(lens)
                    # Dict-of-dicts (e.g., pandas orient="columns" or "index")
                    lens = [len(v) for v in d.values() if isinstance(v, dict)]
                    if lens:
                        return lens[0] if all(n == lens[0] for n in lens) else max(lens)
                    # Fallback: single record
                    return 1

                if isinstance(data, (list, dict)):
                    # Already-parsed JSON
                    data_type = "JSON"
                    bytes_len = len(json.dumps(data, ensure_ascii=False, default=str).encode("utf-8", errors="ignore"))
                    rows = len(data) if isinstance(data, list) else _infer_rows_from_dict(data)
                    sample = json.dumps(
                        (data[:1] if isinstance(data, list) else {k: data[k] for k in list(data)[:5]}),
                        ensure_ascii=False, default=str
                    )[:200]
                elif isinstance(data, str):
                    bytes_len = len(data.encode("utf-8", errors="ignore"))
                    parsed = None
                    # Try JSON
                    try:
                        parsed = json.loads(data.strip())
                    except Exception:
                        parsed = None

                    if isinstance(parsed, list):
                        data_type = "JSON"
                        rows = len(parsed)
                        sample = json.dumps(parsed[:1], ensure_ascii=False, default=str)[:200]
                    elif isinstance(parsed, dict):
                        data_type = "JSON"
                        rows = _infer_rows_from_dict(parsed)
                        sample = json.dumps({k: parsed[k] for k in list(parsed)[:5]}, ensure_ascii=False, default=str)[:200]
                    else:
                        # Try NDJSON
                        lines = [ln for ln in data.splitlines() if ln.strip()]
                        ndjson_ok = 0
                        for ln in lines:
                            try:
                                json.loads(ln)
                                ndjson_ok += 1
                            except Exception:
                                pass
                        if ndjson_ok > 0 and ndjson_ok >= max(1, int(0.9 * len(lines))):
                            data_type = "NDJSON"
                            rows = ndjson_ok
                            sample = "\n".join(lines[:3])[:200]
                        else:
                            # Try CSV
                            try:
                                reader = csv.reader(io.StringIO(data))
                                rows_list = [r for r in reader if r and any(cell.strip() for cell in r)]
                                if rows_list:
                                    data_type = "CSV"
                                    rows = len(rows_list)
                                    sample = "\n".join(",".join(r) for r in rows_list[:3])[:200]
                                else:
                                    data_type = "Unknown"
                                    rows = None
                                    sample = data.strip()[:200]
                            except Exception:
                                data_type = "Unknown"
                                rows = None
                                sample = data.strip()[:200]
                else:
                    # Unknown / non-text payload
                    data_type = "Unknown"
                    text_repr = str(data)
                    bytes_len = len(text_repr.encode("utf-8", errors="ignore"))
                    rows = None
                    sample = text_repr[:200]

                data_size = f"{bytes_len} bytes" + (f", rows={rows}" if rows is not None else ", rows=?")


                
                # Determine endpoint category
                if test_name.startswith("stock_"):
                    category = "Stock"
                elif test_name.startswith("option_"):
                    category = "Option"
                elif test_name.startswith("index_"):
                    category = "Index"
                else:
                    category = "Other"
                
                results.append({
                    "Category": category,
                    "Test Name": test_name,
                    "Status": "✅ SUCCESS",
                    "Data Type": data_type,
                    "Data Rows": rows,
                    "Data Size (bytes)": data_size,
                    # "Sample": sample,
                    "URL": url,
                    "Error": ""
                })
                
            except ThetaDataV3HTTPError as e:
                error_msg = extract_error_text(e.raw)
                category = test_name.split("_")[0].title()
                
                results.append({
                    "Category": category,
                    "Test Name": test_name,
                    "Status": f"❌ HTTP {e.status}",
                    "Data Type": "",
                    "Data Size": 0,
                    # "Sample": "",
                    "URL": e.url,
                    "Error": error_msg[:500]
                })
                
            except Exception as e:
                category = test_name.split("_")[0].title()
                
                results.append({
                    "Category": category,
                    "Test Name": test_name,
                    "Status": "❌ EXCEPTION",
                    "Data Type": "",
                    "Data Size": 0,
                    # "Sample": "",
                    "URL": "",
                    "Error": str(e)[:500]
                })
            
            # Pause to avoid rate limiting
            await asyncio.sleep(0.5)
    
    # Create DataFrame with results
    df_results = pd.DataFrame(results)
    
    # Generate comprehensive statistics
    total_tests = len(results)
    successful_tests = len([r for r in results if r["Status"] == "✅ SUCCESS"])
    
    # Statistics by category
    category_stats = df_results.groupby('Category').agg({
        'Status': lambda x: (x == '✅ SUCCESS').sum(),
        'Test Name': 'count'
    }).rename(columns={'Status': 'Successful', 'Test Name': 'Total'})
    category_stats['Success Rate %'] = (category_stats['Successful'] / category_stats['Total'] * 100).round(1)
    
    # Convert to integers to avoid formatting issues
    category_stats['Successful'] = category_stats['Successful'].astype(int)
    category_stats['Total'] = category_stats['Total'].astype(int)
    
    # Print comprehensive report
    print(f"\n{'='*100}")
    print(f"COMPREHENSIVE TEST REPORT - ThetaData v3 API Client")
    print(f"{'='*100}")
    print(f"Overall Statistics:")
    print(f"  Total Tests: {total_tests}")
    print(f"  Successful: {successful_tests}")
    print(f"  Failed: {total_tests - successful_tests}")
    print(f"  Overall Success Rate: {(successful_tests / total_tests * 100):.1f}%")
    print(f"\nBy Category:")
    for category, stats in category_stats.iterrows():
        successful = int(stats['Successful'])
        total = int(stats['Total'])
        success_rate = stats['Success Rate %']
        print(f"  {category:<8}: {successful:2d}/{total:2d} ({success_rate:5.1f}%)")
    print(f"{'='*100}\n")
    
    return df_results
    
    # async def _expirations_that_traded(self, symbol: str, day_iso: str, req_type: str = "trade") -> list[str]:
    #     """
    #     Ritorna le expiration (YYYYMMDD) che hanno contratti quotati/scambiati in 'day_iso' per il root dato.
    #     Interroga /option/list/contracts/{req_type}?date=YYYYMMDD&symbol=ROOT e fa cache per la durata del processo.
    #     """
    #     from datetime import datetime
        
    #     day_ymd = self._td_ymd(day_iso)
    #     key = (symbol, day_ymd, req_type)
    #     cached = self._exp_by_day_cache.get(key)
    #     if cached is not None:
    #         return list(cached)
    
    #     try:
    #         payload, _ = await self.client.option_list_contracts(
    #             request_type=req_type, date=day_ymd, symbol=symbol, format_type="json"
    #         )
    #     except Exception:
    #         self._exp_by_day_cache[key] = []
    #         return []
    
    #     items = self._ensure_list(payload, keys=("data", "results", "items", "contracts"))
    #     exps: set[str] = set()
    #     for it in items:
    #         if isinstance(it, dict):
    #             exp = it.get("expiration") or it.get("expirationDate") or it.get("exp")
    #             if exp:
    #                 s = str(exp).replace("-", "")
    #                 if len(s) == 8 and s.isdigit():
    #                     exps.add(s)
    
    #     out = sorted(exps)
    #     self._exp_by_day_cache[key] = out
    #     return list(out)



# Helper function for Jupyter Notebook execution
async def run_comprehensive_tests():
    """Helper function to run all tests in Jupyter."""
    print("Helper function to run all tests in Jupyter.")
    results_df = await test_all_endpoints()
    display(results_df)
    return results_df


# Usage examples for Jupyter Notebook:
# 
# # Run comprehensive test suite
# results = await run_comprehensive_tests()
#
# # Filter results by category
# stock_results = results[results['Category'] == 'Stock']
# option_results = results[results['Category'] == 'Option']  
# index_results = results[results['Category'] == 'Index']
#
# # Show only failed tests
# failed_tests = results[results['Status'] != '✅ SUCCESS']
#
# # Basic client usage example
# async with ThetaDataV3Client(api_key="your_key") as client:
#     # Get stock data
#     symbols, url = await client.stock_list_symbols()
#     
#     # Get option data  
#     option_chains, url = await client.option_list_contracts("AAPL", "2024-03-15")
#     greeks, url = await client.option_snapshot_all_greeks("AAPL240315C00150000")
#     
#     # Get index data
#     spx_data, url = await client.index_history_eod("SPX", "2024-01-01", "2024-01-31")
    # =========================================================================
    # (END)
    # INDEX ENDPOINTS
    # =========================================================================


# =========================================================================
# RESILIENT CLIENT WRAPPER (Session Recovery)
# =========================================================================


class ResilientThetaClient:
    """Wrapper around ThetaDataV3Client with automatic session recovery.

    This class wraps the ThetaDataV3Client and adds automatic reconnection
    logic when the session is closed. It intercepts "session closed" errors
    and attempts to reconnect a configurable number of times before failing.

    Parameters
    ----------
    base_client : ThetaDataV3Client
        The underlying ThetaDataV3Client instance to wrap.
    logger : DataConsistencyLogger
        Logger instance for recording session closed events.
    max_reconnect_attempts : int, optional
        Maximum number of reconnection attempts (default 1).

    Examples
    --------
    >>> from .logger import DataConsistencyLogger
    >>> logger = DataConsistencyLogger("/path/to/root")
    >>> base_client = ThetaDataV3Client(api_key="your_key")
    >>> resilient_client = ResilientThetaClient(base_client, logger, max_reconnect_attempts=1)
    >>> data, url = await resilient_client.stock_history_eod("AAPL", "2024-01-01", "2024-01-31")
    """

    def __init__(
        self,
        base_client: ThetaDataV3Client,
        logger: 'DataConsistencyLogger',
        max_reconnect_attempts: int = 1
    ):
        """Initialize resilient client wrapper.

        Parameters
        ----------
        base_client : ThetaDataV3Client
            The underlying client to wrap.
        logger : DataConsistencyLogger
            Logger for session events.
        max_reconnect_attempts : int
            Maximum reconnection attempts.
        """
        self._client = base_client
        self._logger = logger
        self._max_reconnect = max_reconnect_attempts

    async def _execute_with_reconnect(self, method_name: str, *args, **kwargs):
        """Execute client method with session closed recovery.

        Parameters
        ----------
        method_name : str
            Name of the method to call on the base client.
        *args
            Positional arguments to pass to the method.
        **kwargs
            Keyword arguments to pass to the method.

        Returns
        -------
        Any
            Result from the method call.

        Raises
        ------
        SystemExit
            If session closed and reconnection fails after max attempts.
        Exception
            Any other exception from the underlying method.
        """
        method = getattr(self._client, method_name)

        for attempt in range(self._max_reconnect + 1):
            try:
                return await method(*args, **kwargs)

            except Exception as e:
                error_str = str(e).lower()

                # Check if this is a session closed error
                if "session closed" in error_str or "session is closed" in error_str:
                    if attempt < self._max_reconnect:
                        # Log and attempt reconnection
                        self._logger.log_session_closed(
                            symbol="SYSTEM",
                            asset="SYSTEM",
                            interval="SYSTEM",
                            attempt=attempt + 1,
                            fatal=False,
                            details={'method': method_name}
                        )

                        # Attempt reconnection (close and re-initialize)
                        await self._reconnect()
                    else:
                        # Final failure
                        self._logger.log_session_closed(
                            symbol="SYSTEM",
                            asset="SYSTEM",
                            interval="SYSTEM",
                            attempt=attempt + 1,
                            fatal=True,
                            details={'method': method_name}
                        )
                        raise SystemExit(
                            f"Session closed - failed reconnection after {self._max_reconnect + 1} attempts"
                        )
                else:
                    # Not a session error, re-raise immediately
                    raise

    async def _reconnect(self):
        """Reconnect to ThetaData server.

        This method closes the current session and attempts to re-establish
        the connection. The implementation depends on the client's architecture.
        """
        # Close existing session
        try:
            await self._client.close()
        except Exception:
            pass  # Ignore errors during close

        # Wait a moment before reconnecting
        await asyncio.sleep(1.0)

        # The client should automatically reconnect on next request
        # If the client has explicit connect method, call it here

    # Delegate all client methods to _execute_with_reconnect
    # Stock methods
    async def stock_list_symbols(self, *args, **kwargs):
        return await self._execute_with_reconnect('stock_list_symbols', *args, **kwargs)

    async def stock_list_dates(self, *args, **kwargs):
        return await self._execute_with_reconnect('stock_list_dates', *args, **kwargs)

    async def stock_history_eod(self, *args, **kwargs):
        return await self._execute_with_reconnect('stock_history_eod', *args, **kwargs)

    async def stock_history_ohlc(self, *args, **kwargs):
        return await self._execute_with_reconnect('stock_history_ohlc', *args, **kwargs)

    async def stock_history_trade_quote(self, *args, **kwargs):
        return await self._execute_with_reconnect('stock_history_trade_quote', *args, **kwargs)

    async def stock_snapshot_trade_quote(self, *args, **kwargs):
        return await self._execute_with_reconnect('stock_snapshot_trade_quote', *args, **kwargs)

    async def stock_snapshot_ohlc(self, *args, **kwargs):
        return await self._execute_with_reconnect('stock_snapshot_ohlc', *args, **kwargs)

    async def stock_snapshot_quote(self, *args, **kwargs):
        return await self._execute_with_reconnect('stock_snapshot_quote', *args, **kwargs)

    # Option methods
    async def option_list_symbols(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_list_symbols', *args, **kwargs)

    async def option_list_dates(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_list_dates', *args, **kwargs)

    async def option_list_expirations(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_list_expirations', *args, **kwargs)

    async def option_list_strikes(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_list_strikes', *args, **kwargs)

    async def option_list_contracts(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_list_contracts', *args, **kwargs)

    async def option_history_eod(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_history_eod', *args, **kwargs)

    async def option_history_ohlc(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_history_ohlc', *args, **kwargs)

    async def option_history_greeks_eod(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_history_greeks_eod', *args, **kwargs)

    async def option_history_greeks_all(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_history_greeks_all', *args, **kwargs)

    async def option_history_greeks_implied_volatility(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_history_greeks_implied_volatility', *args, **kwargs)

    async def option_history_open_interest(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_history_open_interest', *args, **kwargs)

    async def option_snapshot_greeks_all(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_snapshot_greeks_all', *args, **kwargs)

    async def option_snapshot_trade_quote(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_snapshot_trade_quote', *args, **kwargs)

    async def option_snapshot_ohlc(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_snapshot_ohlc', *args, **kwargs)

    async def option_snapshot_quote(self, *args, **kwargs):
        return await self._execute_with_reconnect('option_snapshot_quote', *args, **kwargs)

    # Index methods
    async def index_list_dates(self, *args, **kwargs):
        return await self._execute_with_reconnect('index_list_dates', *args, **kwargs)

    async def index_history_eod(self, *args, **kwargs):
        return await self._execute_with_reconnect('index_history_eod', *args, **kwargs)

    async def index_history_ohlc(self, *args, **kwargs):
        return await self._execute_with_reconnect('index_history_ohlc', *args, **kwargs)

    async def index_history_price(self, *args, **kwargs):
        return await self._execute_with_reconnect('index_history_price', *args, **kwargs)

    async def index_snapshot_ohlc(self, *args, **kwargs):
        return await self._execute_with_reconnect('index_snapshot_ohlc', *args, **kwargs)

    async def index_snapshot_price(self, *args, **kwargs):
        return await self._execute_with_reconnect('index_snapshot_price', *args, **kwargs)

    # Passthrough properties
    @property
    def base_url(self):
        return self._client.base_url

    @property
    def api_key(self):
        return self._client.api_key

    # Context manager support
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.__aexit__(exc_type, exc_val, exc_tb)

    async def close(self):
        """Close the underlying client session."""
        await self._client.close()
