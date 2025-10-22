# scraper_module/scraper.py
import asyncio
import random
import time
from typing import Optional, Dict, List, Union

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from .logger import get_logger
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, TimeoutError as PlaywrightTimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

from .proxy import ProxyRotator
logger = get_logger("logs/scraper.log")

# Configuration
MAX_RETRIES = 4
MIN_WAIT = 1
MAX_WAIT = 8
PAGE_LOAD_TIMEOUT = 60000  # ms
SELECTOR_WAIT_TIMEOUT = 10000  # ms per selector


class ScrapingError(Exception):
    """Raised when a scrape fails after retries."""
    pass


# --- Shared Proxy Rotator Singleton ---
_proxy_rotator: Optional[ProxyRotator] = None


def get_proxy_rotator() -> ProxyRotator:
    """Singleton pattern for global proxy rotator reuse."""
    global _proxy_rotator
    if _proxy_rotator is None:
        _proxy_rotator = ProxyRotator()
    return _proxy_rotator


# --- Scraper Class ---
class AsyncPlaywrightScraper:
    """
    Async Playwright Scraper supporting multiple CSS selectors.
    Returns a dict of {selector: BeautifulSoup or None}.
    """

    def __init__(self, browser_type: str = "chromium"):
        self.browser_type = browser_type
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.ua = UserAgent()
        self.pages_fetched = 0

    async def _ensure_browser(self):
        """Launch browser if not already initialized (without proxy config)."""
        if self.playwright is None:
            self.playwright = await async_playwright().start()

        # Launch browser without proxy (proxy will be set per-context)
        launch_args = [
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-popup-blocking",
            "--disable-background-timer-throttling",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-sandbox",
        ]

        if self.browser is None:
            if self.browser_type == "firefox":
                self.browser = await self.playwright.firefox.launch(
                    headless=False,
                    args=launch_args
                )
            else:
                self.browser = await self.playwright.chromium.launch(
                    headless=False,
                    args=launch_args
                )
            logger.info(
                f"Browser ({self.browser_type}) launched successfully.")

    async def _create_context(self, proxy_server: Optional[str] = None) -> BrowserContext:
        """Create a new browser context with optional proxy."""
        ua = self.ua.random
        viewport = {
            "width": random.randint(1200, 1920),
            "height": random.randint(720, 1080)
        }

        proxy_config = {"server": proxy_server} if proxy_server else None

        context = await self.browser.new_context(
            user_agent=ua,
            viewport=viewport,
            java_script_enabled=True,
            ignore_https_errors=True,
            proxy=proxy_config,
        )
        await context.route("**/*", self._route_handler)
        return context

    async def _route_handler(self, route):
        """Block unnecessary resources to speed up scraping."""
        req = route.request
        if req.resource_type in ("image", "font", "media", "stylesheet"):
            await route.abort()
        else:
            await route.continue_()

    async def close(self):
        """Cleanly close all browser resources."""
        try:
            if self.browser:
                await self.browser.close()
                self.browser = None
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            logger.info("Browser closed successfully.")
        except Exception as e:
            logger.warning(f"Error closing resources: {e}")

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=MIN_WAIT, max=MAX_WAIT),
        retry=retry_if_exception_type(ScrapingError),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    async def fetch_sections(
        self,
        url: str,
        selectors: Union[str, List[str]],
        use_proxy: bool = False,
        timeout: int = PAGE_LOAD_TIMEOUT,
    ) -> Dict[str, Optional[BeautifulSoup]]:
        """
        Fetch page and extract HTML for provided CSS selectors.
        Returns dict {selector: BeautifulSoup or None}
        """

        rotator = get_proxy_rotator() if use_proxy else None
        proxy = None
        if rotator and use_proxy:
            proxy = await rotator.get_proxy()
            if proxy:
                logger.info(f"Using proxy: {proxy}")
            else:
                logger.info(
                    "No proxy available, continuing direct connection.")

        # Ensure browser is running
        await self._ensure_browser()

        # Create new context with proxy for this request
        context: Optional[BrowserContext] = None
        page: Optional[Page] = None
        results: Dict[str, Optional[BeautifulSoup]] = {}

        try:
            context = await self._create_context(proxy_server=proxy)
            page = await context.new_page()
            page.set_default_timeout(timeout)

            logger.info(f"Navigating to {url}")
            response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout)

            # Check if page loaded successfully via HTTP status
            if response and response.status >= 400:
                logger.warning(f"HTTP error {response.status} for {url}")
                raise ScrapingError(f"HTTP {response.status} error")

            # Normalize selectors input
            if isinstance(selectors, str):
                selectors = [selectors]

            for selector in selectors:
                try:
                    logger.info(f"Waiting for selector: {selector}")
                    await page.wait_for_selector(selector, timeout=SELECTOR_WAIT_TIMEOUT)
                    element = await page.query_selector(selector)
                    if element:
                        html_snippet = await element.inner_html()
                        results[selector] = BeautifulSoup(
                            html_snippet, "html.parser")
                        logger.success(f"Extracted content for {selector}")
                    else:
                        results[selector] = None
                        logger.warning(f"Selector not found: {selector}")
                except PlaywrightTimeoutError:
                    results[selector] = None
                    logger.warning(f"Timeout waiting for selector: {selector}")
                except Exception as e:
                    results[selector] = None
                    logger.error(f"Error extracting selector {selector}: {e}")

            self.pages_fetched += 1
            if rotator and proxy:
                await rotator.mark_success(proxy)

            return results

        except ScrapingError:
            # Mark proxy as failed and re-raise for retry
            if rotator and proxy:
                await rotator.mark_failure(proxy)
            raise

        except Exception as e:
            # Mark proxy as failed and wrap in ScrapingError for retry
            if rotator and proxy:
                await rotator.mark_failure(proxy)
            logger.error(f"Scrape failed for {url}: {e}")
            raise ScrapingError(str(e))

        finally:
            # Only close the page and context, keep browser alive for reuse
            if page:
                try:
                    await page.close()
                except Exception as e:
                    logger.warning(f"Error closing page: {e}")
            if context:
                try:
                    await context.close()
                except Exception as e:
                    logger.warning(f"Error closing context: {e}")


# --- Context Manager Wrapper ---
class AsyncScraperContext:
    """Async context wrapper for safe initialization & cleanup."""

    def __init__(self, browser_type: str = "chromium"):
        self.scraper = AsyncPlaywrightScraper(browser_type=browser_type)

    async def __aenter__(self):
        return self.scraper

    async def __aexit__(self, exc_type, exc, tb):
        await self.scraper.close()


# --- High-Level Helper ---
async def scrape_url(
    url: str,
    selectors: Union[str, List[str]],
    use_proxy: bool = False,
    browser_type: str = "chromium",
    timeout: int = PAGE_LOAD_TIMEOUT,
) -> Dict[str, Optional[BeautifulSoup]]:
    """
    Scrape one page and extract elements matching selectors.
    Returns dict of {selector: BeautifulSoup or None}.
    """

    async with AsyncScraperContext(browser_type=browser_type) as scraper:
        return await scraper.fetch_sections(
            url=url,
            selectors=selectors,
            use_proxy=use_proxy,
            timeout=timeout,
        )
