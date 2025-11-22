"""Download retry helper with integrated validation.

This module provides a centralized retry mechanism for downloads with validation,
ensuring that data is both successfully downloaded AND passes validation checks
before being accepted.
"""

import asyncio
from typing import Callable, Tuple, Any
import pandas as pd


async def download_with_retry_and_validation(
    download_func: Callable,
    parse_func: Callable,
    validate_func: Callable,
    retry_policy,
    logger,
    context: dict
) -> Tuple[pd.DataFrame, bool]:
    """Download data with retry and validation.

    Attempts to download and validate data N times (configured in retry_policy).
    If validation fails, retries the entire download operation.

    Parameters
    ----------
    download_func : callable
        Async function that downloads data (returns (result, url))
    parse_func : callable
        Function that parses result into DataFrame
    validate_func : callable
        Async function that validates DataFrame (returns bool)
    retry_policy : RetryPolicy
        Retry configuration
    logger : DataConsistencyLogger
        Logger instance
    context : dict
        Context dict with {symbol, asset, interval, date_range, sink, ...}

    Returns
    -------
    tuple
        (DataFrame, validation_success: bool)

    Example Usage
    -------------
    df, success = await download_with_retry_and_validation(
        download_func=lambda: client.stock_history_eod(...),
        parse_func=lambda result: pd.read_csv(StringIO(result[0])),
        validate_func=lambda df: manager._validate_downloaded_data(df, ...),
        retry_policy=manager.cfg.retry_policy,
        logger=manager.logger,
        context={'symbol': 'AAPL', 'asset': 'stock', ...}
    )
    """
    df = None
    symbol = context.get('symbol')
    asset = context.get('asset')
    interval = context.get('interval')
    date_range = context.get('date_range', ('', ''))

    for attempt in range(retry_policy.max_attempts):
        try:
            # Download data
            result, url = await download_func()
            df = parse_func(result)

            # Validate
            validation_ok = await validate_func(df)

            if validation_ok:
                if attempt > 0:
                    logger.log_resolution(
                        symbol=symbol,
                        asset=asset,
                        interval=interval,
                        date_range=date_range,
                        message=f"Download and validation succeeded after {attempt + 1} attempts",
                        details={}
                    )
                return df, True

            # Validation failed
            if attempt < retry_policy.max_attempts - 1:
                logger.log_retry_attempt(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    attempt=attempt + 1,
                    error_msg="Validation failed, retrying download",
                    details={}
                )
                await asyncio.sleep(retry_policy.delay_seconds)

        except Exception as e:
            if attempt < retry_policy.max_attempts - 1:
                logger.log_retry_attempt(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    attempt=attempt + 1,
                    error_msg=f"Download failed: {str(e)}, retrying",
                    details={'error': str(e), 'error_type': type(e).__name__}
                )
                await asyncio.sleep(retry_policy.delay_seconds)
            else:
                logger.log_failure(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    message=f"Download failed after {retry_policy.max_attempts} attempts: {str(e)}",
                    details={'error': str(e), 'error_type': type(e).__name__}
                )
                # Return None, False instead of raising to allow graceful continuation
                return None, False

    # All retries failed validation
    logger.log_failure(
        symbol=symbol,
        asset=asset,
        interval=interval,
        date_range=date_range,
        message=f"Validation failed after {retry_policy.max_attempts} attempts",
        details={}
    )
    return df, False
