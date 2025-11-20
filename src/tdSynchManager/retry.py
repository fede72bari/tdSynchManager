"""Retry utilities with configurable policy and automatic logging."""

import asyncio
from typing import Any, Awaitable, Callable, Dict, Tuple, Type, TypeVar

from .config import RetryPolicy
from .logger import DataConsistencyLogger

__all__ = ["retry_with_policy"]

T = TypeVar('T')


async def retry_with_policy(
    coro_func: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    logger: DataConsistencyLogger,
    context: Dict[str, Any],
    error_types: Tuple[Type[Exception], ...] = (Exception,)
) -> Tuple[T | None, bool]:
    """Execute an asynchronous operation with configurable retry logic and automatic logging.

    This function wraps any async operation with retry capabilities, automatically logging
    each attempt, failure, and resolution. It respects the configured retry policy including
    maximum attempts and delays between retries. On success after retries, it logs the
    resolution. On final failure, it logs the permanent failure. This provides a consistent
    retry pattern across all data operations.

    Parameters
    ----------
    coro_func : Callable[[], Awaitable[T]]
        An async callable (typically a lambda) that returns a coroutine to execute. Should
        take no arguments and return the result of the async operation. Example:
        lambda: client.stock_history_eod("AAPL", "2024-01-01", "2024-01-31", "csv")
    policy : RetryPolicy
        Retry policy configuration specifying max_attempts, delay_seconds, and other retry
        parameters. Controls how many times to retry and how long to wait between attempts.
    logger : DataConsistencyLogger
        Logger instance for recording retry attempts, resolutions, and failures. All retry
        activity is automatically logged with appropriate context.
    context : Dict[str, Any]
        Context dictionary containing operation details for logging. Must include keys:
        'symbol' (str), 'asset' (str), 'interval' (str), and 'date_range' (tuple of two strings).
        Used to provide context in log entries.
    error_types : Tuple[Type[Exception], ...], optional
        Default: (Exception,)

        Tuple of exception types to catch and retry. Only exceptions matching these types
        will trigger retry logic. Other exceptions will be re-raised immediately. Use this
        to specify which errors are considered retryable.

    Returns
    -------
    Tuple[T | None, bool]
        A tuple of (result, success_flag):
        - If successful: (operation_result, True) where operation_result is the return
          value from coro_func.
        - If failed after all retries: (None, False) indicating permanent failure.

    Example Usage
    -------------
    ```python
    # Retry a ThetaData API call with automatic logging
    result, success = await retry_with_policy(
        coro_func=lambda: client.stock_history_eod(
            "AAPL", "2024-01-01", "2024-01-31", "csv"
        ),
        policy=config.retry_policy,
        logger=logger,
        context={
            'symbol': 'AAPL',
            'asset': 'stock',
            'interval': '1d',
            'date_range': ('2024-01-01', '2024-01-31')
        }
    )

    if success:
        csv_data, url = result
        # Process successful result
    else:
        # Handle permanent failure
        print("Failed to retrieve data after all retries")
    ```
    """
    last_error: Exception | None = None
    symbol = context.get('symbol', 'UNKNOWN')
    asset = context.get('asset', 'UNKNOWN')
    interval = context.get('interval', 'UNKNOWN')
    date_range = context.get('date_range', ('', ''))

    for attempt in range(policy.max_attempts):
        try:
            result = await coro_func()

            # Success
            if attempt > 0:
                # Log resolution if this was a retry
                logger.log_resolution(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    message=f"Resolved after {attempt + 1} attempts",
                    details={'total_attempts': attempt + 1}
                )

            return (result, True)

        except error_types as e:
            last_error = e

            if attempt < policy.max_attempts - 1:
                # Log retry attempt
                logger.log_retry_attempt(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    attempt=attempt + 1,
                    error_msg=str(e),
                    details={'error_type': type(e).__name__}
                )

                # Wait before retry
                await asyncio.sleep(policy.delay_seconds)
            else:
                # Final failure after all retries
                logger.log_failure(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    message=f"Failed after {policy.max_attempts} attempts: {e}",
                    details={
                        'error_type': type(e).__name__,
                        'total_attempts': policy.max_attempts
                    }
                )

        except asyncio.CancelledError:
            # Always re-raise CancelledError
            raise

    # All retries exhausted
    return (None, False)
