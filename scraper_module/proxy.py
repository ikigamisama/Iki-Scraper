# scraper_module/proxy.py
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Optional

import requests
from fake_useragent import UserAgent
from fp.fp import FreeProxy
from bs4 import BeautifulSoup
from .logger import get_logger

PROXY_CACHE_SIZE = 50
PROXY_VALIDATION_TIMEOUT = 5
PROXY_REFRESH_INTERVAL = 300  # seconds

logger = get_logger("logs/proxy.log")


@dataclass
class ProxyInfo:
    proxy: str
    last_used: float = 0.0
    success_count: int = 0
    failure_count: int = 0
    is_working: bool = True

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        return (self.success_count / total) if total > 0 else 0.0


class ProxyRotator:
    """
    Async-friendly proxy rotator with simple validation and caching.
    Usage:
        rotator = ProxyRotator()
        p = await rotator.get_proxy()
    """

    def __init__(self, cache_size: int = PROXY_CACHE_SIZE):
        self.cache_size = cache_size
        self.proxies: List[ProxyInfo] = []
        self._lock = asyncio.Lock()
        self._index = 0
        self._last_refresh = 0
        self._ua = UserAgent()

    async def get_proxy(self) -> Optional[str]:
        """
        Returns a working proxy (http://ip:port) or None if none available.
        Refreshes pool if empty or stale.
        """
        async with self._lock:
            now = time.time()
            if not self.proxies or now - self._last_refresh > PROXY_REFRESH_INTERVAL:
                await self._refresh_proxies_locked()

            if not self.proxies:
                logger.warning("ProxyRotator: no proxies available")
                return None

            # Round-robin with simple skip of failing proxies
            for _ in range(len(self.proxies)):
                p = self.proxies[self._index % len(self.proxies)]
                self._index += 1
                if p.is_working:
                    p.last_used = now
                    return p.proxy

            # Fallback: return first proxy
            return self.proxies[0].proxy

    async def _refresh_proxies_locked(self):
        """Called under lock to refresh the proxy list"""
        logger.info("ProxyRotator: refreshing proxy pool...")
        self._last_refresh = time.time()
        fresh = await self._get_fresh_proxies()
        if not fresh:
            logger.warning("ProxyRotator: no fresh proxies found")
            return

        valid = await self._validate_proxies_batch(fresh)
        # Add unique proxies, keep limited size and sort by success_rate
        existing = {p.proxy for p in self.proxies}
        for pr in valid:
            if pr not in existing:
                self.proxies.append(ProxyInfo(proxy=pr))

        # keep only top cache_size (simple sort by success_rate)
        self.proxies = sorted(self.proxies, key=lambda x: x.success_rate, reverse=True)[
            : self.cache_size]
        logger.info(
            f"ProxyRotator: pool size after refresh: {len(self.proxies)}")

    async def _get_fresh_proxies(self) -> List[str]:
        """Try multiple sources for proxy IPs (lightweight)."""
        proxies = []

        # Source 1: FreeProxy lib
        try:
            p = FreeProxy(rand=True, timeout=2).get()
            if p:
                proxies.append(p if p.startswith("http") else f"http://{p}")
        except Exception:
            logger.debug("ProxyRotator: FreeProxy source failed")

        # Source 2: proxy-list.download simple API
        try:
            resp = requests.get(
                "https://www.proxy-list.download/api/v1/get?type=http", timeout=8)
            if resp.status_code == 200 and resp.text.strip():
                for line in resp.text.strip().splitlines():
                    line = line.strip()
                    if line:
                        proxies.append(
                            f"http://{line}" if not line.startswith("http") else line)
        except Exception:
            logger.debug("ProxyRotator: proxy-list.download API failed")

        # Source 3: fallback scrape free-proxy-list.net (small HTML parse)
        if not proxies:
            try:
                resp = requests.get("https://free-proxy-list.net/", timeout=8)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    table = soup.find("table", {"id": "proxylisttable"})
                    if table:
                        rows = table.find("tbody").find_all("tr")[:30]
                        for r in rows:
                            cols = r.find_all("td")
                            # https only
                            if len(cols) >= 7 and cols[6].text.strip().lower() == "yes":
                                ip = cols[0].text.strip()
                                port = cols[1].text.strip()
                                proxies.append(f"http://{ip}:{port}")
            except Exception:
                logger.debug("ProxyRotator: scraping free-proxy-list failed")

        # Deduplicate and return
        return list(dict.fromkeys(proxies))

    def _validate_proxy_sync(self, proxy: str) -> bool:
        """Synchronous validation used inside threadpool (requests)."""
        try:
            headers = {"User-Agent": self._ua.random}
            resp = requests.get("http://httpbin.org/ip", proxies={"http": proxy, "https": proxy},
                                timeout=PROXY_VALIDATION_TIMEOUT, headers=headers)
            return resp.status_code == 200
        except Exception:
            return False

    async def _validate_proxies_batch(self, proxy_list: List[str]) -> List[str]:
        """Validate proxies using a ThreadPoolExecutor to keep sync requests out of the event loop."""
        valid = []
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=10) as ex:
            tasks = [loop.run_in_executor(
                ex, self._validate_proxy_sync, p) for p in proxy_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for p, ok in zip(proxy_list, results):
                if ok is True:
                    valid.append(p)
        logger.info(
            f"ProxyRotator: validated {len(valid)} / {len(proxy_list)}")
        return valid

    # Optional helpers to mark success/failure for proxies
    async def mark_success(self, proxy: str):
        async with self._lock:
            for p in self.proxies:
                if p.proxy == proxy:
                    p.success_count += 1
                    p.is_working = True
                    return

    async def mark_failure(self, proxy: str):
        async with self._lock:
            for p in self.proxies:
                if p.proxy == proxy:
                    p.failure_count += 1
                    # mark as not working if failures are many
                    if p.failure_count >= 3 and p.failure_count > p.success_count:
                        p.is_working = False
                    return
