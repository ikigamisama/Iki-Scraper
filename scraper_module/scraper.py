import asyncio
import random
from typing import Optional, Dict, List, Union

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, TimeoutError as PlaywrightTimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

from .proxy import ProxyRotator
from .logger import get_logger

logger = get_logger("logs/scraper.log")

# Configuration
MAX_RETRIES = 4
MIN_WAIT = 1
MAX_WAIT = 8
PAGE_LOAD_TIMEOUT = 120000
SELECTOR_WAIT_TIMEOUT = 300000

# Desktop User-Agent to prevent mobile redirects
DESKTOP_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


class ScrapingError(Exception):
    """Raised when a scrape fails after retries."""
    pass


_proxy_rotator: Optional[ProxyRotator] = None


def get_proxy_rotator() -> ProxyRotator:
    """Singleton pattern for global proxy rotator reuse."""
    global _proxy_rotator
    if _proxy_rotator is None:
        _proxy_rotator = ProxyRotator()
    return _proxy_rotator


class AsyncPlaywrightScraper:
    """
    Async Playwright Scraper with human-like behavior and anti-detection.
    Returns dict of {selector: BeautifulSoup or None}.
    """

    def __init__(self, browser_type: str = "chromium"):
        self.browser_type = browser_type
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.pages_fetched = 0
        self.session_referers = []

    def get_headers(self, url: str = None) -> Dict[str, str]:
        """Generate realistic desktop headers with referer chain"""

        referer = None
        if self.session_referers:
            referer = self.session_referers[-1]
        elif url:
            # Random search engine referer for first visit
            referer = random.choice([
                "https://www.google.com/",
                "https://www.bing.com/",
            ])

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "User-Agent": DESKTOP_UA,
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none" if not referer else "cross-site",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

        if referer:
            headers["Referer"] = referer
            # Check if same domain for accurate Sec-Fetch-Site
            if url and referer:
                from urllib.parse import urlparse
                referer_domain = urlparse(referer).netloc
                url_domain = urlparse(url).netloc
                if referer_domain == url_domain:
                    headers["Sec-Fetch-Site"] = "same-origin"

        return headers

    async def _ensure_browser(self):
        """Launch browser with anti-detection settings."""
        if self.playwright is None:
            self.playwright = await async_playwright().start()

        if self.browser is None:
            args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]

            if self.browser_type == "firefox":
                self.browser = await self.playwright.firefox.launch(
                    headless=True,
                    args=args
                )
            else:
                self.browser = await self.playwright.chromium.launch(
                    headless=True,
                    args=args,
                )
            logger.info(
                f"Browser ({self.browser_type}) launched successfully.")

    async def _create_context(self, proxy_server: Optional[str] = None, url: str = None) -> BrowserContext:
        """Create browser context with desktop viewport and anti-detection."""
        headers = self.get_headers(url=url)

        # Desktop viewport to prevent mobile redirects
        viewport = {
            "width": random.randint(1366, 1920),
            "height": random.randint(768, 1080)
        }

        proxy_config = {"server": proxy_server} if proxy_server else None

        context = await self.browser.new_context(
            user_agent=DESKTOP_UA,
            extra_http_headers=headers,
            viewport=viewport,
            proxy=proxy_config,
            locale='en-US',
            ignore_https_errors=True,
        )

        await context.route("**/*", self._route_handler)

        return context

    async def _route_handler(self, route):
        """Block unnecessary resources to speed up scraping."""
        if route.request.resource_type in ("image", "font", "media"):
            await route.abort()
        else:
            await route.continue_()

    async def _human_like_interaction(self, page: Page):
        """Simulate human behavior with mouse movements and scrolling"""
        try:
            await asyncio.sleep(random.uniform(0.5, 1.5))

            viewport = page.viewport_size

            # Random mouse movements
            for _ in range(random.randint(2, 4)):
                x = random.randint(100, viewport['width'] - 100)
                y = random.randint(100, viewport['height'] - 100)
                await page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.3))

            # Natural scrolling
            scroll_distance = random.randint(200, 500)
            await page.evaluate(f"window.scrollBy(0, {scroll_distance})")
            await asyncio.sleep(random.uniform(0.3, 0.7))

        except Exception as e:
            logger.debug(f"Human interaction simulation error: {e}")

    async def _check_mobile_redirect(self, page: Page) -> bool:
        """Check if page redirected to mobile subdomain and redirect back to desktop"""
        current_url = page.url

        # Common mobile subdomain patterns
        mobile_patterns = ['m.', 'mobile.', 'wap.']

        for pattern in mobile_patterns:
            if pattern in current_url:
                logger.warning(
                    f"Mobile redirect detected in URL: {current_url}")
                desktop_url = current_url.replace(pattern, 'www.')
                logger.info(f"Redirecting to desktop version: {desktop_url}")
                try:
                    await page.goto(desktop_url, wait_until="do", timeout=PAGE_LOAD_TIMEOUT)
                    return True
                except Exception as e:
                    logger.error(f"Failed to redirect to desktop version: {e}")
                    return False

        return False

    async def close(self):
        """Close browser resources."""
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
        simulate_human: bool = True,
        check_mobile_redirect: bool = True,
    ) -> Dict[str, Optional[BeautifulSoup]]:
        """
        Fetch page and extract HTML for provided CSS selectors.

        Args:
            url: Target URL to scrape
            selectors: CSS selector(s) to extract
            use_proxy: Whether to use proxy rotation
            timeout: Page load timeout in milliseconds
            simulate_human: Simulate human-like interactions
            check_mobile_redirect: Auto-detect and redirect from mobile subdomains

        Returns:
            Dict mapping selectors to BeautifulSoup objects (or None if not found)
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

        await self._ensure_browser()

        context: Optional[BrowserContext] = None
        page: Optional[Page] = None
        results: Dict[str, Optional[BeautifulSoup]] = {}

        try:
            context = await self._create_context(proxy_server=proxy, url=url)
            page = await context.new_page()
            page.set_default_timeout(timeout)

            # Track referer chain
            self.session_referers.append(url)
            if len(self.session_referers) > 10:
                self.session_referers.pop(0)

            logger.info(f"Navigating to {url}")

            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=timeout
            )

            if response and response.status >= 400:
                logger.warning(f"HTTP error {response.status} for {url}")
                raise ScrapingError(f"HTTP {response.status} error")

            # Check and handle mobile redirects if enabled
            if check_mobile_redirect:
                await self._check_mobile_redirect(page)

            # Simulate human behavior if enabled
            if simulate_human:
                await self._human_like_interaction(page)

            # Additional wait for dynamic content
            await asyncio.sleep(random.uniform(1.0, 2.0))

            # Normalize selectors input
            if isinstance(selectors, str):
                selectors = [selectors]

            # Extract content for each selector
            for selector in selectors:
                try:
                    logger.info(f"Waiting for selector: {selector}")
                    await page.wait_for_selector(selector, timeout=SELECTOR_WAIT_TIMEOUT)
                    element = await page.query_selector(selector)
                    if element:
                        html_snippet = await element.evaluate("el => el.outerHTML")
                        results[selector] = BeautifulSoup(
                            html_snippet, "html.parser")
                        logger.success(f"Extracted content for {selector}")
                    else:
                        results[selector] = None
                        logger.warning(f"Selector not found: {selector}")
                except PlaywrightTimeoutError as e:
                    results[selector] = None
                    logger.warning(f"Timeout waiting for selector: {selector}")
                    raise ScrapingError(
                        f"Timeout waiting for selector: {selector}") from e
                except Exception as e:
                    results[selector] = None
                    logger.error(f"Error extracting selector {selector}: {e}")

            self.pages_fetched += 1
            if rotator and proxy:
                await rotator.mark_success(proxy)

            return results

        except ScrapingError:
            if rotator and proxy:
                await rotator.mark_failure(proxy)
            raise

        except Exception as e:
            if rotator and proxy:
                await rotator.mark_failure(proxy)
            logger.error(f"Scrape failed for {url}: {e}")
            raise ScrapingError(str(e))

        finally:
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


class AsyncScraperContext:
    """Async context wrapper for safe initialization & cleanup."""

    def __init__(self, browser_type: str = "chromium"):
        self.scraper = AsyncPlaywrightScraper(browser_type=browser_type)

    async def __aenter__(self):
        return self.scraper

    async def __aexit__(self, exc_type, exc, tb):
        await self.scraper.close()


async def scrape_url(
    url: str,
    selectors: Union[str, List[str]],
    use_proxy: bool = False,
    browser_type: str = "chromium",
    timeout: int = PAGE_LOAD_TIMEOUT,
    simulate_human: bool = True,
    check_mobile_redirect: bool = True,
) -> Dict[str, Optional[BeautifulSoup]]:
    """
    Scrape one page and extract elements matching selectors.

    Args:
        url: Target URL to scrape
        selectors: CSS selector(s) to extract
        use_proxy: Whether to use proxy rotation
        browser_type: "chromium" or "firefox"
        timeout: Page load timeout in milliseconds
        simulate_human: Simulate human-like interactions (mouse, scroll)
        check_mobile_redirect: Auto-detect and redirect from mobile subdomains

    Returns:
        Dict mapping selectors to BeautifulSoup objects (or None if not found)
    """

    async with AsyncScraperContext(browser_type=browser_type) as scraper:
        return await scraper.fetch_sections(
            url=url,
            selectors=selectors,
            use_proxy=use_proxy,
            timeout=timeout,
            simulate_human=simulate_human,
            check_mobile_redirect=check_mobile_redirect,
        )
