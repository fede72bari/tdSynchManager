"""
Manual test of ThetaData API endpoints to debug blocking issue
"""

import asyncio
import time
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager import ThetaDataV3Client


async def test_stock_list_dates():
    """Test stock_list_dates endpoint"""
    print("=" * 80)
    print("TEST 1: Stock list_dates endpoint")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        symbol = "AAPL"

        print(f"\n[TEST] Calling stock_list_dates({symbol}, data_type='trade')...")
        t0 = time.time()

        try:
            response, url = await asyncio.wait_for(
                client.stock_list_dates(symbol, data_type="trade", format_type="json"),
                timeout=10.0
            )

            elapsed = time.time() - t0
            dates = response.get("date", response) if isinstance(response, dict) else response

            print(f"[SUCCESS] Received {len(dates) if dates else 0} dates in {elapsed:.2f}s")
            print(f"[URL] {url}")
            if dates:
                print(f"[SAMPLE] First 5: {dates[:5]}")
                print(f"[SAMPLE] Last 5: {dates[-5:]}")

        except asyncio.TimeoutError:
            print(f"[TIMEOUT] stock_list_dates took >10 seconds")
        except Exception as e:
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()


async def test_index_list_dates():
    """Test index_list_dates endpoint"""
    print("\n" + "=" * 80)
    print("TEST 2: Index list_dates endpoint")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        symbol = "SPX"

        print(f"\n[TEST] Calling index_list_dates({symbol}, data_type='ohlc')...")
        t0 = time.time()

        try:
            response, url = await asyncio.wait_for(
                client.index_list_dates(symbol, data_type="ohlc", format_type="json"),
                timeout=10.0
            )

            elapsed = time.time() - t0
            dates = response.get("date", response) if isinstance(response, dict) else response

            print(f"[SUCCESS] Received {len(dates) if dates else 0} dates in {elapsed:.2f}s")
            print(f"[URL] {url}")
            if dates:
                print(f"[SAMPLE] First 5: {dates[:5]}")
                print(f"[SAMPLE] Last 5: {dates[-5:]}")

        except asyncio.TimeoutError:
            print(f"[TIMEOUT] index_list_dates took >10 seconds")
        except Exception as e:
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()


async def test_option_list_expirations():
    """Test option_list_expirations endpoint"""
    print("\n" + "=" * 80)
    print("TEST 3: Option list_expirations endpoint")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        symbol = "AAL"

        print(f"\n[TEST] Calling option_list_expirations({symbol})...")
        t0 = time.time()

        try:
            response, url = await asyncio.wait_for(
                client.option_list_expirations(symbol, format_type="json"),
                timeout=10.0
            )

            elapsed = time.time() - t0
            expirations = response.get("expiration", response) if isinstance(response, dict) else response

            print(f"[SUCCESS] Received {len(expirations) if expirations else 0} expirations in {elapsed:.2f}s")
            print(f"[URL] {url}")
            if expirations:
                print(f"[SAMPLE] First 5: {expirations[:5]}")
                print(f"[SAMPLE] Last 5: {expirations[-5:]}")

        except asyncio.TimeoutError:
            print(f"[TIMEOUT] option_list_expirations took >10 seconds")
        except Exception as e:
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()


async def test_option_list_dates():
    """Test option_list_dates endpoint"""
    print("\n" + "=" * 80)
    print("TEST 4: Option list_dates endpoint")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        symbol = "AAL"
        expiration = "2024-12-06"  # Recent expiration

        print(f"\n[TEST] Calling option_list_dates({symbol}, expiration={expiration})...")
        t0 = time.time()

        try:
            response, url = await asyncio.wait_for(
                client.option_list_dates(
                    symbol=symbol,
                    request_type="quote",
                    expiration=expiration,
                    format_type="json"
                ),
                timeout=5.0
            )

            elapsed = time.time() - t0
            dates = response.get("date", response) if isinstance(response, dict) else response

            print(f"[SUCCESS] Received {len(dates) if dates else 0} dates in {elapsed:.2f}s")
            print(f"[URL] {url}")
            if dates:
                print(f"[SAMPLE] First 5: {dates[:5]}")
                print(f"[SAMPLE] Last 5: {dates[-5:]}")

        except asyncio.TimeoutError:
            print(f"[TIMEOUT] option_list_dates took >5 seconds")
        except Exception as e:
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()


async def test_parallel_option_expirations():
    """Test PARALLEL option_list_expirations calls (AAL + XOM)"""
    print("\n" + "=" * 80)
    print("TEST 5: PARALLEL option_list_expirations (AAL + XOM)")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        symbols = ["AAL", "XOM"]

        print(f"\n[TEST] Calling option_list_expirations for {symbols} IN PARALLEL...")
        t0 = time.time()

        async def fetch_expirations(symbol):
            try:
                print(f"[{symbol}] Starting option_list_expirations...")
                t_start = time.time()

                response, url = await asyncio.wait_for(
                    client.option_list_expirations(symbol, format_type="json"),
                    timeout=10.0
                )

                elapsed = time.time() - t_start
                expirations = response.get("expiration", response) if isinstance(response, dict) else response

                print(f"[{symbol}] SUCCESS - {len(expirations) if expirations else 0} expirations in {elapsed:.2f}s")
                print(f"[{symbol}] URL: {url}")
                return symbol, expirations

            except asyncio.TimeoutError:
                print(f"[{symbol}] TIMEOUT - took >10 seconds")
                return symbol, None
            except Exception as e:
                print(f"[{symbol}] ERROR - {e}")
                import traceback
                traceback.print_exc()
                return symbol, None

        # Run in parallel
        results = await asyncio.gather(
            *[fetch_expirations(sym) for sym in symbols]
        )

        total_elapsed = time.time() - t0

        print(f"\n[PARALLEL] Total elapsed time: {total_elapsed:.2f}s")
        for symbol, expirations in results:
            count = len(expirations) if expirations else 0
            print(f"[PARALLEL] {symbol}: {count} expirations")


async def main():
    print("\n" + "=" * 80)
    print("ThetaData API Endpoints - Manual Testing")
    print("=" * 80)

    # Test individual endpoints
    await test_stock_list_dates()
    await test_index_list_dates()
    await test_option_list_expirations()
    await test_option_list_dates()

    # Test parallel calls (THIS IS WHERE THE BLOCKING MIGHT HAPPEN)
    await test_parallel_option_expirations()

    print("\n" + "=" * 80)
    print("ALL TESTS COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
