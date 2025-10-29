"""
Main entrypoint for the async Playwright scraper with proxy rotation.

Run:
    python index.py
"""

import asyncio
import time
from loguru import logger
from scraper_module.scraper import scrape_url


async def single_selector_example():
    """Example: Scrape one selector from a page."""
    url = "https://httpbin.org/html"
    selector = "h1"  # Simple selector for demo
    logger.info("🧭 Running single selector scrape (no proxy)...")

    result = await scrape_url(url, selectors=selector, use_proxy=False)
    soup = result.get(selector)

    if soup:
        logger.success(
            f"Extracted content for selector '{selector}' from {url}")
        print("\n=== Extracted Content Preview ===\n")
        print(soup.prettify()[:400])
        print(result)
        print("\n=================================\n")
    else:
        logger.warning(f"No content found for selector '{selector}'")


async def multiple_selectors_example():
    """Example: Scrape multiple selectors concurrently from one page."""
    url = "https://httpbin.org/html"
    selectors = ["h1", "p"]
    logger.info("🌐 Running multi-selector scrape (with proxy if available)...")

    result = await scrape_url(url, selectors=selectors, use_proxy=False)

    for selector, soup in result.items():
        if soup:
            logger.success(f"✅ Selector '{selector}' extracted successfully.")
            print(f"\n[{selector}] Preview:\n{str(soup)[:300]}...\n")
        else:
            logger.warning(f"❌ Selector '{selector}' not found or timed out.")


async def main():
    """Run both examples sequentially."""
    start = time.time()
    logger.info("🚀 Starting scraper examples...")

    # await single_selector_example()
    await multiple_selectors_example()

    duration = time.time() - start
    logger.info(
        f"🏁 Completed all scraping examples in {duration:.2f} seconds.")


if __name__ == "__main__":
    asyncio.run(main())
