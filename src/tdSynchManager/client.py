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
        self.status = status
        self.url = url
        self.raw = raw_response
        self.original_exc = original_exc
        super().__init__(
            f"HTTP error {status}: {url}\n"
            f"Response (truncated): {raw_response[:1000]}"
        )


class ThetaDataV3Client:
    def __init__(self, base_url: str = None, data_dir: str = "./data",
                 max_concurrent_requests: int = 10,
                 timeout_total: float = 300.0,
                 timeout_connect: float = 10.0,
                 timeout_sock_read: float = 120.0):
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
        self.session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Any, str]:
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

    
    # =====================================
    # STOCK ENDPOINTS - Following API docs order
    # Reference: https://docs.thetadata.us/docs/rest-api-v3/stock
    # =====================================
    
    # --- LIST ENDPOINTS ---
    
    async def stock_list_symbols(self, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        List all available stock symbols.
        
        Endpoint: GET /stock/list/symbols
        Reference: https://docs.thetadata.us/docs/rest-api-v3/stock/list/symbols
        
        Args:
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[List[str] | str, str]: List of stock symbols and request URL
            
        Description:
            Returns complete list of all stock symbols available in the database.
            Useful for discovering supported tickers before requesting specific data.
            Response includes symbol, description, and exchange information.
        """
        params = {"format": format_type}
        return await self._make_request("/stock/list/symbols", params)
    
    async def stock_list_dates(self, symbol: str, data_type: Literal["trade", "quote"] = "trade",
                              format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        List available dates for a specified stock symbol.
        
        Endpoint: GET /stock/list/dates
        Reference: https://docs.thetadata.us/docs/rest-api-v3/stock/list/dates
        
        Args:
            symbol (str): Stock symbol (e.g., "AAPL", "MSFT")
            data_type (str): Data type ("trade" or "quote"). Defaults to "trade".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[List[str] | str, str]: List of available dates (UTC format) and request URL
            
        Description:
            Returns list of dates for which trade or quote data is available for the symbol.
            Dates are returned in UTC timezone. Useful for verifying temporal coverage
            before requesting historical data ranges.
        """
        params = {
            "symbol": symbol,
            "format": format_type
        }
        return await self._make_request(f"/stock/list/dates/{data_type}", params)
    
    # --- SNAPSHOT ENDPOINTS ---
    
    async def stock_snapshot_ohlc(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Get current OHLC snapshot for a stock symbol.
        
        Endpoint: GET /stock/snapshot/ohlc
        Reference: https://docs.thetadata.us/docs/rest-api-v3/stock/snapshot/ohlc
        
        Args:
            symbol (str): Stock symbol
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Current OHLC snapshot data and request URL
            
        Description:
            Returns most recent Open, High, Low, Close data available for the symbol.
            Includes timestamp (UTC), volume, and other real-time or near real-time market metrics.
            Data represents current trading session or most recent completed session.
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/stock/snapshot/ohlc", params)
    
    async def stock_snapshot_trade(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Get last trade snapshot for a stock symbol.
        
        Endpoint: GET /stock/snapshot/trade
        Reference: https://docs.thetadata.us/docs/rest-api-v3/stock/snapshot/trade
        
        Args:
            symbol (str): Stock symbol
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Last trade data and request URL
            
        Description:
            Returns information about the most recent trade executed for the symbol.
            Includes price, size, timestamp (UTC), exchange, and trade conditions.
            Essential for real-time price discovery and market activity monitoring.
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/stock/snapshot/trade", params)
    
    async def stock_snapshot_quote(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Get last quote snapshot (bid/ask) for a stock symbol.
        
        Endpoint: GET /stock/snapshot/quote
        Reference: https://docs.thetadata.us/docs/rest-api-v3/stock/snapshot/quote
        
        Args:
            symbol (str): Stock symbol
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Last quote data and request URL
            
        Description:
            Returns current best bid and ask available for the symbol with timestamp (UTC),
            sizes, exchange, and current bid-ask spread. Critical for understanding
            current market liquidity and order book state.
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/stock/snapshot/quote", params)
    
    # --- HISTORY ENDPOINTS ---
    
    async def stock_history_eod(self, symbol: str, start_date: str, end_date: str,
                               format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Get historical end-of-day data for a stock symbol.
        
        Endpoint: GET /stock/history/eod
        Reference: https://docs.thetadata.us/docs/rest-api-v3/stock/history/eod
        
        Args:
            symbol (str): Stock symbol
            start_date (str): Start date in YYYY-MM-DD format (UTC)
            end_date (str): End date in YYYY-MM-DD format (UTC)
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[List[Dict] | str, str]: Historical EOD data and request URL
            
        Description:
            Returns historical end-of-day time series containing:
            open, high, low, close, volume, adjusted close for the specified date range.
            All timestamps are in UTC. Includes corporate action adjustments.
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
        Get historical OHLC bars for a stock symbol on a specific date.
        
        Endpoint: GET /stock/history/ohlc
        Reference: https://docs.thetadata.us/operations/stock_history_ohlc.html
        
        Args:
            symbol (str): Stock symbol (required), e.g., "AAPL".
            date (str): Trading date in YYYY-MM-DD or YYYYMMDD format.
            interval (Interval): Bar size (required), e.g., "1m", "5m", "1h".
            start_time (str, optional): Intraday start time (HH:MM:SS), default 09:30:00.
            end_time (str, optional): Intraday end time (HH:MM:SS), default 16:00:00.
            format_type (str, optional): Response format ("json", "csv", "ndjson"). Defaults to "csv".
            
        Returns:
            Tuple[List[Dict] | str, str]: OHLC bars and request URL
            
        Description:
            Aggregates trades into OHLC bars using SIP rules. Bar timestamp equals the bar's
            open time. Trades are included if open_time <= trade_time < open_time + interval.
            Essential for stock price movement analysis and backtesting strategies.
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
        """
        Get historical trades for a stock symbol (v3).
    
        Endpoint: GET /stock/history/trade
        Reference: https://docs.thetadata.us/operations/stock_history_trade.html
    
        Args:
            symbol (str): Stock symbol (e.g., "AAPL").
            date (str): Trading date in YYYYMMDD.
            start_time (str, optional): "HH:MM:SS[.SSS]" (ET). Default 09:30:00.
            end_time (str, optional): "HH:MM:SS[.SSS]" (ET). Default 16:00:00.
            venue (str, optional): "nqb" or "utp_cta". Default server behavior is "nqb".
            format_type (str, optional): "json", "csv", or "ndjson". Defaults to "json".
    
        Returns:
            Tuple[List[Dict] | str, str]: Trades and request URL.
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
        """
        Get historical NBBO quotes for a stock symbol (v3).
    
        Endpoint: GET /stock/history/quote
        Reference: https://docs.thetadata.us/operations/stock_history_quote.html
    
        Args:
            symbol (str): Stock symbol (e.g., "AAPL").
            date (str): Trading date to fetch in YYYYMMDD.
            interval (str): Aggregation size. One of:
                tick, 10ms, 100ms, 500ms, 1s, 5s, 10s, 15s, 30s, 1m, 5m, 10m, 15m, 30m, 1h.
            start_time (str, optional): Start time within the day, "HH:MM:SS[.SSS]" (ET). Defaults to 09:30:00.
            end_time (str, optional): End time within the day, "HH:MM:SS[.SSS]" (ET). Defaults to 16:00:00.
            venue (str, optional): "nqb" (Nasdaq Basic) or "utp_cta" (merged SIPs). Default server behavior is "nqb".
            format_type (str, optional): "json", "csv", or "ndjson". Defaults to "json".
    
        Returns:
            Tuple[List[Dict] | str, str]: Quotes (per interval) and request URL.
    
        Description:
            Returns every NBBO quote or the last quote per interval for the given day.
            Use `venue="nqb"` for Nasdaq Basic current-day data (requires stocks standard/pro).
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
    
    
    # --- STOCK HISTORY / TRADE_QUOTE (V3) ---
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
        """
        Get historical trade+quote pairs for a stock symbol (v3).
    
        Endpoint: GET /stock/history/trade_quote
        Reference: https://docs.thetadata.us/operations/stock_history_trade_quote.html
    
        Args:
            symbol (str): Stock symbol (e.g., "AAPL").
            date (str): Trading date to fetch in YYYYMMDD.
            start_time (str, optional): Start time "HH:MM:SS[.SSS]" (ET). Default 09:30:00.
            end_time (str, optional): End time "HH:MM:SS[.SSS]" (ET). Default 16:00:00.
            exclusive (bool, optional): If True, match quotes with timestamp < trade time;
                if False, allow <=. Defaults to True (server default).
            venue (str, optional): "nqb" or "utp_cta". Default server behavior is "nqb".
            format_type (str, optional): "json", "csv", or "ndjson". Defaults to "json".
    
        Returns:
            Tuple[List[Dict] | str, str]: Trade-quote pairs (tick level) and request URL.
    
        Description:
            Returns each trade paired with the last BBO quote at the time of trade.
            Toggle `exclusive` to control < vs <= matching behavior.
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

        
    
    # --- AT-TIME ENDPOINTS ---    
    
    async def stock_at_time_trade(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        venue: Optional[Literal["nqb","utp_cta"]] = None,
        format_type: Optional[Literal["json","csv","ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """
        Get the last trade at a specific time-of-day (v3).
    
        Endpoint: GET /stock/at_time/trade
        Reference: https://docs.thetadata.us/operations/stock_at_time_trade.html
    
        Args:
            symbol (str): Stock symbol (e.g., "SPY").
            start_date (str): Start date (inclusive) in YYYYMMDD.
            end_date (str): End date (inclusive) in YYYYMMDD.
            time_of_day (str): Time within the day "HH:MM:SS[.SSS]" (America/New_York).
            venue (str, optional): "nqb" or "utp_cta". Default server behavior is "nqb".
            format_type (str, optional): "json", "csv", or "ndjson". Defaults to "json".
    
        Returns:
            Tuple[List[Dict] | str, str]: Trades at the given time-of-day and request URL.
    
        Description:
            Returns the last trade reported by UTP/CTA at the specified millisecond-of-day.
            Useful for time-synchronized analytics and replay at specific times.
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
    
    
    # --- STOCK AT_TIME / QUOTE (V3) ---
    async def stock_at_time_quote(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        venue: Optional[Literal["nqb","utp_cta"]] = None,
        format_type: Optional[Literal["json","csv","ndjson"]] = "json",
    ) -> Tuple[Any, str]:
        """
        Get the last NBBO quote at a specific time-of-day (v3).
    
        Endpoint: GET /stock/at_time/quote
        Reference: https://docs.thetadata.us/operations/stock_at_time_quote.html
    
        Args:
            symbol (str): Stock symbol (e.g., "SPY").
            start_date (str): Start date (inclusive) in YYYYMMDD.
            end_date (str): End date (inclusive) in YYYYMMDD.
            time_of_day (str): Time within the day "HH:MM:SS[.SSS]" (America/New_York).
            venue (str, optional): "nqb" or "utp_cta". Default server behavior is "nqb".
            format_type (str, optional): "json", "csv", or "ndjson". Defaults to "json".
    
        Returns:
            Tuple[List[Dict] | str, str]: Quotes at the given time-of-day and request URL.
    
        Description:
            Returns the last NBBO quote reported by UTP/CTA at the specified time-of-day.
            Ideal for aligning quotes with other time-based datasets.
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
    # OPTION ENDPOINTS - Following API docs order
    # Reference: https://docs.thetadata.us/docs/rest-api-v3/option
    # =====================================
    
    # --- LIST ENDPOINTS ---
    
    async def option_list_symbols(self, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        List all available option symbol symbols.
        
        Endpoint: GET /option/list/symbols
        Reference: https://docs.thetadata.us/docs/rest-api-v3/option/list/symbols
        
        Args:
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[List[str] | str, str]: List of option symbol symbols and request URL
            
        Description:
            Returns complete list of all option symbol symbols available in the database.
            These are the underlying symbols for which option contracts are available.
            Essential for discovering supported option chains before requesting specific data.
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
        """
        List available dates for option *trade* or *quote* data for a specific symbol and expiration (v3).
    
        Endpoint:
            GET /option/list/dates/{request_type}
            Docs: https://docs.thetadata.us/operations/option_list_dates.html
    
        Args:
            symbol (str): Underlying symbol (e.g., "AAPL", "SPY").
            request_type (Literal["trade","quote"]): Which data you want dates for. Default "trade".
            expiration (str): Expiration in "YYYY-MM-DD" or "YYYYMMDD". **Required by API**.
            strike (str | float, optional): Strike in dollars (e.g., 150.00) or "*" for all. Defaults to "*".
            right (Literal["call","put","both"]): Option right. Default "both".
            format_type (Literal["csv","json","ndjson","html"], optional): Response format. Default "json".
    
        Returns:
            Tuple[List[dict] | str, str]: If JSON, a list of records with fields:
                {symbol, expiration, strike, right, date}. Second element is the request URL.
    
        Notes:
            - The v3 route includes {request_type} in the path (not a "type" query param).
            - The API accepts expiration as YYYY-MM-DD *or* YYYYMMDD.
            - When `strike` is omitted, "*" (all strikes) is used.
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
        """
        List available expiration dates for an option underlying (v3).
    
        Endpoint:
            GET /option/list/expirations
            Docs: https://docs.thetadata.us/operations/option_list_expirations.html
    
        Args:
            symbol (str): Underlying symbol (e.g., "AAPL", "SPY").
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "json".
    
        Returns:
            Tuple[List[str] | str, str]: If JSON, a list of ISO dates ("YYYY-MM-DD").
            Second element is the request URL.
    
        Notes:
            - Only `symbol` is required for this endpoint in v3.
            - Dates returned are ISO-8601 (UTC).
        """
        if not symbol:
            raise ValueError("`symbol` is required for /option/list/expirations.")
    
        params = {"symbol": symbol, "format": format_type or "json"}
        return await self._make_request("/option/list/expirations", params)


        
    
    async def option_list_strikes(self, symbol: str, expiration: str,
                                 format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        List available strike prices for a specified option symbol and expiration.
        
        Endpoint: GET /option/list/strikes
        Reference: https://docs.thetadata.us/docs/rest-api-v3/option/list/strikes
        
        Args:
            symbol (str): Option symbol symbol
            expiration (str): Expiration date in YYYY-MM-DD format
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[List[float] | str, str]: List of strike prices and request URL
            
        Description:
            Returns all strike prices available for the specified option symbol and
            expiration combination. Strikes are returned as decimal values.
            Essential for constructing complete option chains and understanding available strikes.
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
        """
        List all option contracts for a specified request type and date.

        Endpoint: GET /option/list/contracts/{request_type}
        Reference: https://docs.thetadata.us/operations/option_list_contracts.html

        Args:
            request_type (Literal["trade", "quote"]): Request flavor.
                "trade" lists option contracts that traded on the given date.
                "quote" lists option contracts that had quotes on the given date.
            date (str): Target date, recommended format YYYYMMDD (e.g., "20220930").
            symbol (str | Sequence[str], optional): Underlying filter (e.g., "AAPL").
                You can pass a single string or a list/tuple of underlyings; these
                will be sent as a comma-separated list. If omitted, the endpoint
                returns all contracts for the date.
            format_type (Literal["csv", "json", "ndjson"], optional): Response format.
                Default is "csv" (v3 default).

        Returns:
            Tuple[List[Dict] | str, str]: Contract data (list of dicts for JSON/NDJSON,
                string for CSV) and the requested URL.

        Description:
            Returns a complete list of option contracts (calls and puts) that were
            active for the specified request type ("trade" or "quote") on the given date,
            optionally filtered by underlying. Each item typically includes: contract
            symbol, expiration (YYYY-MM-DD), OPTION_STRIKE, and right (C/P). Requires a
            locally running Theta Terminal v3 instance to service REST requests.
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


        
    # --- SNAPSHOT ENDPOINTS ---
    
    async def option_snapshot_ohlc(self, symbol: str, expiration: str, strike: float, right: str = "C",
                                   format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Get current OHLC snapshot for an option contract.
        
        Endpoint: GET /option/snapshot/ohlc
        Reference: https://docs.thetadata.us/docs/rest-api-v3/option/snapshot/ohlc
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD format
            strike (float): Strike price (e.g., 150.00)
            right (str): Option right "C" for call or "P" for put. Defaults to "C".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Current OHLC snapshot and request URL
            
        Description:
            Returns most recent Open, High, Low, Close data for the option contract.
            Requires separate symbol, OPTION_EXPIRATION, OPTION_STRIKE, and right parameters in v3.
            Includes timestamp (UTC), volume, open interest, and other metrics.
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
        Get last trade snapshot for an option contract.
        
        Endpoint: GET /option/snapshot/trade
        Reference: https://docs.thetadata.us/operations/option_snapshot_trade.html
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD or YYYYMMDD format
            strike (float): Strike price (e.g., 150.00)
            right (str): Option right "call", "put", or "both". Defaults to "both".
            format_type (str, optional): Response format ("json", "csv", or "ndjson"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Last trade data and request URL
            
        Description:
            Returns the most recent trade executed for the option contract.
            Snapshot endpoints are real-time (cache resets nightly).
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
        Get last quote snapshot for an option contract.
        
        Endpoint: GET /option/snapshot/quote
        Reference: https://docs.thetadata.us/operations/option_snapshot_quote.html
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD or YYYYMMDD format (or "*" for all)
            strike (float): Strike price (e.g., 150.00) (or "*" for all)
            right (str): Option right "call", "put", or "both". Defaults to "both".
            format_type (str, optional): Response format ("json", "csv", or "ndjson"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Last quote data and request URL
            
        Description:
            Returns the current best bid/ask (NBBO) for the option contract with sizes,
            exchange and conditions. Snapshot endpoints are real-time (cache resets nightly).
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
        Get current open interest snapshot for an option contract.
        
        Endpoint: GET /option/snapshot/open_interest
        Reference: https://docs.thetadata.us/docs/rest-api-v3/option/snapshot/open_interest
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD format
            strike (float): Strike price (e.g., 150.00)
            right (str): Option right "C" for call or "P" for put. Defaults to "C".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Open interest data and request URL
            
        Description:
            Returns current open interest (total outstanding contracts) for the option.
            Requires separate symbol, OPTION_EXPIRATION, OPTION_STRIKE, and right parameters in v3.
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
        Get current implied volatility snapshot for an option contract.
        
        Endpoint: GET /option/snapshot/implied_volatility
        Reference: https://docs.thetadata.us/operations/option_snapshot_greeks_implied_volatility.html
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD format
            strike (float): Strike price (e.g., 150.00)
            right (str): Option right "C" for call or "P" for put. Defaults to "C".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Implied volatility data and request URL
            
        Description:
            Returns current open interest (total outstanding contracts) for the option.
            Requires separate symbol, OPTION_EXPIRATION, OPTION_STRIKE, and right parameters in v3.
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
        Get current snapshot of all Greeks for an option contract.
        
        Endpoint: GET /option/snapshot/greeks/all
        Reference: https://docs.thetadata.us/operations/option_snapshot_greeks_all.html
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD format
            strike (float): Strike price (e.g., 150.00)
            right (str): Option right "C" for call or "P" for put. Defaults to "C".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: All Greeks data and request URL
            
        Description:
            Returns comprehensive Greeks snapshot including Delta, Gamma, Theta, Vega,
            Rho, and higher-order Greeks. All calculations with timestamp (UTC).
            Essential for comprehensive option risk assessment and portfolio management.
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
        Get current first-order Greeks snapshot for an option contract.
        
        Endpoint: GET /option/snapshot/greeks/first_order
        Reference: https://docs.thetadata.us/operations/option_snapshot_greeks_first_order.html
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD format
            strike (float): Strike price (e.g., 150.00)
            right (str): Option right "C" for call or "P" for put. Defaults to "C".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: First-order Greeks data and request URL
            
        Description:
            Returns first-order Greeks including Delta, Gamma, Theta, Vega, and Rho.
            All calculations with timestamp (UTC). Essential for basic option
            risk management and sensitivity analysis.
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
        Get current second-order Greeks snapshot for an option contract.
        
        Endpoint: GET /option/snapshot/greeks/second_order
        Reference: https://docs.thetadata.us/operations/option_snapshot_greeks_second_order.html
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD format
            strike (float): Strike price (e.g., 150.00)
            right (str): Option right "C" for call or "P" for put. Defaults to "C".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Second-order Greeks data and request URL
            
        Description:
            Returns second-order Greeks including Vomma, Veta, and other second derivatives.
            All calculations with timestamp (UTC). Used for advanced option risk
            management and convexity analysis.
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
        Get current third-order Greeks snapshot for an option contract.
        
        Endpoint: GET /option/snapshot/greeks/third_order
        Reference: https://docs.thetadata.us/operations/option_snapshot_greeks_third_order.html
        
        Args:
            symbol (str): Option root symbol (e.g., "AAPL")
            expiration (str): Expiration date in YYYY-MM-DD format
            strike (float): Strike price (e.g., 150.00)
            right (str): Option right "C" for call or "P" for put. Defaults to "C".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Third-order Greeks data and request URL
            
        Description:
            Returns third-order Greeks including Speed, Color, and other third derivatives.
            All calculations with timestamp (UTC). Used for sophisticated option
            portfolio management and higher-order risk analysis.
        """
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "strike": strike,
            "right": right,
            "format": format_type
        }
        return await self._make_request("/option/snapshot/greeks/third_order", params)
        
    
    # --- HISTORY ENDPOINTS ---


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
        Get End-of-Day (EOD) report for option contracts (v3).
    
        Endpoint:
            GET /option/history/eod
            Docs: https://docs.thetadata.us/operations/option_history_eod.html
    
        Notes
        -----
        - Supports expiration="*" for all expirations.
        - If 'date' is provided (legacy), it is treated as start_date=end_date=date.
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
        """
        Get every NBBO quote (or last quote per interval) for option contracts.

        Endpoint: GET /option/history/quote
        Reference: https://http-docs.thetadata.us/operations/hist-option-quote.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241104" or "2024-11-04".
            interval (Interval): Tick or bar interval (required; default server is 1s).
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Quote ticks/bars and request URL.

        Description:
            Returns all NBBO quotes; if `interval` provided, returns the last quote
            at each interval timestamp. Essential for understanding bid-ask spreads
            and market liquidity for option contracts.
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
        """
        Get aggregated intraday OHLC bars for option contracts.

        Endpoint: GET /option/history/ohlc
        Reference: https://http-docs.thetadata.us/operations/hist-option-ohlc.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105" or "2024-11-05".
            interval (Interval): Bar size (e.g., "1m", "5m", "1h", or "tick").
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS), default 09:30:00.
            end_time (str, optional): Intraday end time (HH:MM:SS), default 16:00:00.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: OHLC bars and request URL.

        Description:
            Aggregates trades into bars using SIP rules; bar timestamp equals the bar's
            open time; trades are included if open_time <= trade_time < open_time + interval.
            Critical for option price movement analysis and backtesting strategies.
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

    async def option_history_open_interest(
        self,
        symbol: str,
        expiration: str,
        date: str,
        strike: Optional[Union[float, str]] = "*",
        right: Literal["call", "put", "both"] = "both",
        format_type: Literal["csv", "json", "ndjson"] = "csv",
    ) -> Tuple[Any, str]:
        """
        Get historical open interest for option contracts.

        Endpoint: GET /option/history/open_interest
        Reference: https://http-docs.thetadata.us/operations/hist-option-open-interest.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date for which OI is reported (represents prior-day OI).
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Open interest rows and request URL.

        Description:
            OPRA typically reports OI once per day; the value reflects prior-day EOD OI.
            Essential for understanding option liquidity and market maker positioning.
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
        """
        Get every trade for option contracts on a given date.

        Endpoint: GET /option/history/trade
        Reference: https://http-docs.thetadata.us/operations/hist-option-trade.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241104" or "2024-11-04".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS), default 09:30:00.
            end_time (str, optional): Intraday end time (HH:MM:SS), default 16:00:00.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Trade ticks and request URL.

        Description:
            Returns every OPRA trade message for the specified filters. Essential for
            analyzing option trading activity, volume, and price discovery mechanisms.
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
        """
        Get every trade paired with the latest NBBO quote at the trade time.

        Endpoint: GET /option/history/trade_quote
        Reference: https://http-docs.thetadata.us/operations/hist-option-trade-quote.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241104" or "2024-11-04".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            exclusive (bool, optional): If True, match quotes with timestamp < trade time.
                Default True (recommended).
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Trade-quote rows and request URL.

        Description:
            For each trade, returns the last NBBO whose timestamp is <= (or < if `exclusive`)
            the trade timestamp. Critical for market impact analysis and execution quality studies.
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
        """
        Get implied volatility (bid/mid/ask) at fixed intervals.

        Endpoint: GET /option/history/greeks/implied_volatility
        Reference: https://http-docs.thetadata.us/operations/hist-option-iv.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            interval (Interval): Bar size (required), e.g., "1m", "5m", "1h".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Implied volatility bars and request URL.

        Description:
            Returns implied volatility calculated from option mid prices at specified intervals.
            Essential for volatility trading and risk management strategies.
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
        """
        Get End-of-Day Greeks for option contracts (v3).
    
        Endpoint:
            GET /option/history/greeks/eod
            Docs: https://docs.thetadata.us/operations/option_history_greeks_eod.html
    
        Args:
            symbol (str): Underlying/root symbol (e.g., "AAPL").
            expiration (str): Contract expiration "YYYY-MM-DD" or "YYYYMMDD", or "*" for all expirations.
            start_date (str): Inclusive start date ("YYYY-MM-DD" or "YYYYMMDD").
            end_date (str): Inclusive end date ("YYYY-MM-DD" or "YYYYMMDD").
            strike (float | str, optional): Specific strike or "*" (default).
            right ({"call","put","both"}): Option right filter (default "both").
            annual_dividend (float, optional): Annualized dividend override for Greeks.
            rate_type (Literal[…], optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate % override.
            format_type ({"csv","json","ndjson","html"}): Output format. Default "csv".
    
        Returns:
            Tuple[List[Dict] | str, str]: Payload (CSV/JSON) and the fully-qualified request URL.
    
        Notes:
            - Uses Theta Data's EOD reports (generated at 17:15 ET) and closing prices.
            - Join keys with daily OHLC typically include:
              ["timestamp","symbol","expiration","strike","right"].
            - When expiration="*", requests must be issued **day-by-day**.
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
        """
        Get first-order Greeks calculated at each trade (tick-based).

        Endpoint: GET /option/history/trade_greeks/first_order
        Reference: https://http-docs.thetadata.us/operations/hist-option-trade-greeks.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Trade-based first-order Greeks rows and request URL.

        Description:
            Returns Delta, Gamma, Theta, Vega, and Rho computed per-trade using the last 
            underlying price at the row's timestamp. No interval parameter needed.
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
        """
        Get second-order Greeks calculated at each trade (tick-based).

        Endpoint: GET /option/history/trade_greeks/second_order
        Reference: https://http-docs.thetadata.us/operations/hist-option-trade-greeks-second.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Trade-based second-order Greeks rows and request URL.

        Description:
            Returns Vomma, Veta, and other second derivatives computed per-trade using the 
            last underlying price at the row's timestamp. No interval parameter needed.
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
        """
        Get third-order Greeks calculated at each trade (tick-based).

        Endpoint: GET /option/history/trade_greeks/third_order
        Reference: https://http-docs.thetadata.us/operations/hist-option-trade-greeks-third.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Trade-based third-order Greeks rows and request URL.

        Description:
            Returns Speed, Color, and other third derivatives computed per-trade using the 
            last underlying price at the row's timestamp. No interval parameter needed.
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
        """
        Get implied volatility calculated at each trade (tick-based).

        Endpoint: GET /option/history/trade_greeks/implied_volatility
        Reference: https://http-docs.thetadata.us/operations/hist-option-trade-iv.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Trade-based implied volatility rows and request URL.

        Description:
            Returns implied volatility computed per-trade using the last underlying price 
            at the row's timestamp. No interval parameter needed since it's trade-based.
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
        """
        Get first-order Greeks at fixed intervals for option contracts.

        Endpoint: GET /option/history/greeks/first_order
        Reference: https://http-docs.thetadata.us/operations/hist-option-greeks.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            interval (Interval): Bar size (required), e.g., "1m", "5m", "1h".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: First-order Greeks bars and request URL.

        Description:
            Returns Delta, Gamma, Theta, Vega, and Rho calculated from option mid prices.
            Essential for option risk management and delta-neutral strategies.
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
        """
        Get second-order Greeks at fixed intervals for option contracts.

        Endpoint: GET /option/history/greeks/second_order
        Reference: https://http-docs.thetadata.us/operations/hist-option-greeks-second.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            interval (Interval): Bar size (required), e.g., "1m", "5m", "1h".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Second-order Greeks bars and request URL.

        Description:
            Returns Vomma, Veta, and other second derivatives calculated from option mid prices.
            Used for advanced option risk management and convexity analysis.
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
        """
        Get third-order Greeks at fixed intervals for option contracts.

        Endpoint: GET /option/history/greeks/third_order
        Reference: https://http-docs.thetadata.us/operations/hist-option-greeks-third.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            interval (Interval): Bar size (required), e.g., "1m", "5m", "1h".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: Third-order Greeks bars and request URL.

        Description:
            Returns Speed, Color, and other third derivatives calculated from option mid prices.
            Used for sophisticated option portfolio management and higher-order risk analysis.
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
        """
        Get all-order Greeks at fixed intervals for option contracts.

        Endpoint: GET /option/history/greeks/all
        Reference: https://http-docs.thetadata.us/operations/hist-option-greeks-all.html

        Args:
            symbol (str): Underlying symbol (required), e.g., "AAPL".
            expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
            date (str): Trading date, e.g., "20241105".
            interval (Interval): Bar size (required), e.g., "1m", "5m", "1h".
            strike (float | str, optional): Strike or "*". Default "*".
            right (Literal["call","put","both"], optional): Option right. Default "both".
            start_time (str, optional): Intraday start time (HH:MM:SS).
            end_time (str, optional): Intraday end time (HH:MM:SS).
            annual_dividend (float, optional): Annualized dividend for calculation.
            rate_type (str, optional): Interest rate curve (default "sofr").
            rate_value (float, optional): Interest rate percent for calculation.
            format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".

        Returns:
            Tuple[List[Dict] | str, str]: All Greeks bars and request URL.

        Description:
            Returns comprehensive Greeks (first, second, and third order) calculated from 
            option/underlying mid prices. Terminal v3 required for computation.
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
       """
       Get all-order Greeks calculated at each trade (tick-based).
    
       Endpoint: GET /option/history/trade_greeks/all
       Reference: https://http-docs.thetadata.us/operations/hist-option-trade-greeks-all.html
    
       Args:
           symbol (str): Underlying symbol (required), e.g., "AAPL".
           expiration (str): Expiration in YYYY-MM-DD or YYYYMMDD, or "*" for all.
           date (str): Trading date, e.g., "20241105".
           strike (float | str, optional): Strike or "*". Default "*".
           right (Literal["call","put","both"], optional): Option right. Default "both".
           start_time (str, optional): Intraday start time (HH:MM:SS).
           end_time (str, optional): Intraday end time (HH:MM:SS).
           annual_dividend (float, optional): Annualized dividend for calculation.
           rate_type (str, optional): Interest rate curve (default "sofr").
           rate_value (float, optional): Interest rate percent for calculation.
           format_type (Literal["csv","json","ndjson"], optional): Response format. Default "csv".
    
       Returns:
           Tuple[List[Dict] | str, str]: Trade-based all Greeks rows and request URL.
    
       Description:
           Returns comprehensive Greeks computed per-trade using the last underlying 
           price at the row's timestamp. No interval parameter needed.
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


    # --- AT-TIME ENDPOINTS ---
        
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
        Get the last option trade at a specific time-of-day (v3).
    
        Endpoint: GET /option/at_time/trade
    
        Args:
            symbol (str): Option root (e.g., "AAPL").
            expiration (str): Expiration in YYYYMMDD.
            strike (float): Strike price (e.g., 150.00).
            right (str): "C" for call, "P" for put.
            start_date (str): Start date (inclusive) in YYYYMMDD.
            end_date (str): End date (inclusive) in YYYYMMDD.
            time_of_day (str): "HH:MM:SS[.SSS]" (America/New_York).
            format_type (str, optional): "json" or "csv". Defaults to "json".
    
        Returns:
            Tuple[List[Dict] | str, str]: Trades at the given time-of-day and request URL.
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
    
    
    # --- OPTION AT_TIME / QUOTE (V3) ---
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
        Get the last option NBBO quote at a specific time-of-day (v3).
    
        Endpoint: GET /option/at_time/quote
    
        Args:
            symbol (str): Option root (e.g., "AAPL").
            expiration (str): Expiration in YYYYMMDD.
            strike (float): Strike price (e.g., 150.00).
            right (str): "C" for call, "P" for put.
            start_date (str): Start date (inclusive) in YYYYMMDD.
            end_date (str): End date (inclusive) in YYYYMMDD.
            time_of_day (str): "HH:MM:SS[.SSS]" (America/New_York).
            format_type (str, optional): "json" or "csv". Defaults to "json".
    
        Returns:
            Tuple[List[Dict] | str, str]: Quotes at the given time-of-day and request URL.
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
    # INDEX ENDPOINTS - Following API docs order  
    # Reference: https://docs.thetadata.us/docs/rest-api-v3/index
    # =====================================
    
    # --- LIST ENDPOINTS ---
    
    async def index_list_symbols(self, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        List all available index symbols.
        
        Endpoint: GET /index/list/symbols
        Reference: https://docs.thetadata.us/docs/rest-api-v3/index/list/symbols
        
        Args:
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[List[str] | str, str]: List of index symbols and request URL
            
        Description:
            Returns complete list of all index symbols available in the database.
            Includes major indices like SPX, NDX, RUT, etc. Essential for discovering
            supported index data before requesting specific historical or real-time data.
        """
        params = {"format": format_type}
        return await self._make_request("/index/list/symbols", params)
    
    async def index_list_dates(self, symbol: str, data_type: Literal["price", "ohlc"] = "price",
                              format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        List available dates for index data for a specified symbol.
        
        Endpoint: GET /index/list/dates
        Reference: https://docs.thetadata.us/docs/rest-api-v3/index/list/dates
        
        Args:
            symbol (str): Index symbol (e.g., "SPX", "NDX")
            data_type (str): Data type ("price" or "ohlc"). Defaults to "price".
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[List[str] | str, str]: List of available dates (UTC) and request URL
            
        Description:
            Returns list of dates for which index price or OHLC data is available
            for the specified symbol. All dates in UTC timezone. Useful for verifying
            temporal coverage before requesting historical index data.
        """
        params = {
            "symbol": symbol,
            "type": data_type,
            "format": format_type
        }
        return await self._make_request("/index/list/dates", params)
    
    # --- SNAPSHOT ENDPOINTS ---
    
    async def index_snapshot_ohlc(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Get current OHLC snapshot for an index.
        
        Endpoint: GET /index/snapshot/ohlc
        Reference: https://docs.thetadata.us/docs/rest-api-v3/index/snapshot/ohlc
        
        Args:
            symbol (str): Index symbol
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Current OHLC snapshot and request URL
            
        Description:
            Returns most recent Open, High, Low, Close data for the index with
            timestamp (UTC). Essential for real-time index monitoring and
            current market state assessment.
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/index/snapshot/ohlc", params)
    
    async def index_snapshot_price(self, symbol: str, format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Get current price snapshot for an index.
        
        Endpoint: GET /index/snapshot/price
        Reference: https://docs.thetadata.us/docs/rest-api-v3/index/snapshot/price
        
        Args:
            symbol (str): Index symbol
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[Dict | str, str]: Current price snapshot and request URL
            
        Description:
            Returns most recent price level for the index with timestamp (UTC).
            Critical for real-time index value monitoring and market analysis.
        """
        params = {"symbol": symbol, "format": format_type}
        return await self._make_request("/index/snapshot/price", params)
    
    # --- HISTORY ENDPOINTS ---
    
    async def index_history_eod(self, symbol: str, start_date: str, end_date: str,
                               format_type: Optional[Literal["csv", "json", "ndjson"]] = "json") -> Tuple[Any, str]:
        """
        Get historical end-of-day data for an index.
        
        Endpoint: GET /index/history/eod
        Reference: https://docs.thetadata.us/docs/rest-api-v3/index/history/eod
        
        Args:
            symbol (str): Index symbol
            start_date (str): Start date in YYYY-MM-DD format (UTC)
            end_date (str): End date in YYYY-MM-DD format (UTC)
            format_type (str, optional): Response format ("json" or "csv"). Defaults to "json".
            
        Returns:
            Tuple[List[Dict] | str, str]: Historical EOD index data and request URL
            
        Description:
            Returns historical end-of-day index values including open, high, low,
            close for the specified date range. All timestamps in UTC.
            Essential for index backtesting and long-term market analysis.
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
            interval: Interval = "1s",  # v3 default; usa l'alias globale
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            format_type: Optional[Literal["csv", "json", "ndjson", "html"]] = "csv",
        ) -> Tuple[Any, str]:
            """
            Get historical OHLC bars for an index with customizable interval (v3).
        
            Endpoint: GET /index/history/ohlc     (NB: base_url include già /v3)
            Reference: https://docs.thetadata.us/operations/index_history_ohlc.html
        
            Args:
                symbol (str): Index symbol (es. "SPX").
                start_date (str): Data inizio inclusiva, consigliato YYYYMMDD.
                end_date (str): Data fine inclusiva, consigliato YYYYMMDD.
                interval (Interval): Dimensione barra (tick, 1s, 5s, 1m, ...).
                start_time (str, optional): HH:MM:SS (default server 09:30:00).
                end_time (str, optional): HH:MM:SS (default server 16:00:00).
                format_type (str, optional): csv/json/ndjson/html (default "csv").
        
            Returns:
                Tuple[List[Dict] | str, str]: OHLC bars e URL.
        
            Note:
                Ogni record include: timestamp (YYYY-MM-DDTHH:mm:ss.SSS), open, high,
                low, close, volume, count, vwap.
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
        Get historical price ticks for an index on a specific date (v3).
    
        Endpoint: GET /index/history/price   (NB: base_url include già /v3)
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


    # --- AT-TIME ENDPOINTS ---
    async def index_at_time_price(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        time_of_day: str,
        format_type: Optional[Literal["json", "csv", "ndjson"]] = "csv",
    ) -> Tuple[Any, str]:
        """
        Retrieve the index price at a specific time-of-day across a date range (v3).
    
        Endpoint:
            GET /index/at_time/price   (note: self.base_url already includes /v3)
    
        Reference:
            https://docs.thetadata.us/operations/index_at_time_price.html
    
        Args:
            symbol (str):
                Index ticker (e.g., "SPX"). Single symbol supported.
            start_date (str):
                Inclusive start date. Recommended format: YYYYMMDD (e.g., "20241104").
            end_date (str):
                Inclusive end date. Recommended format: YYYYMMDD (e.g., "20241108").
            time_of_day (str):
                The time within the trading day in the America/New_York timezone.
                Format: "HH:MM:SS[.SSS]" (e.g., "09:30:00" or "09:30:01.000").
            format_type (Literal["csv","json","ndjson"], optional):
                Response format. v3 default is "csv". All listed formats are supported.
    
        Returns:
            Tuple[Any, str]:
                - The response payload:
                  * CSV / NDJSON: text string.
                  * JSON: a list of records (dicts), one per date in the range.
                - The full request URL (for logging/audit).
    
        Typical response schema (per date):
            - timestamp (str): ISO-8601 "YYYY-MM-DDTHH:MM:SS.SSS" in UTC.
            - symbol    (str): index symbol.
            - price     (float): index price at the requested `time_of_day`.
    
        Notes:
            - If there is no tick exactly at `time_of_day`, the endpoint returns the
              last available price as of that moment (per the endpoint’s matching rules).
            - The date range is inclusive of both `start_date` and `end_date`.
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
    Extract meaningful error text from HTML error response.
    
    Args:
        error_text (str): Raw error text from HTTP response
        
    Returns:
        str: Cleaned and readable error text
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