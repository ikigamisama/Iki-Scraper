# 🕸️ Async Playwright Scraper with Proxy Rotation

A robust, production-ready asynchronous web scraper built using **Playwright**, **Tenacity**, and a custom **ProxyRotator**. Designed for scalability and integration into ETL or Airflow pipelines.

---

## 📁 Project Structure

```
project_root/
├── scraper_module/
│   ├── __init__.py
│   ├── proxy.py        # Async Proxy Manager
│   └── scraper.py      # Async Playwright Scraper (multi-selector support)
└── index.py             # Main entrypoint (single + multiple selector examples)
```

---

## 🚀 Features

✅ **Asynchronous Architecture** — built entirely on asyncio for concurrency.
✅ **Smart Proxy Rotation** — validates and caches working proxies automatically.
✅ **Playwright Integration** — renders JavaScript-heavy sites safely.
✅ **Tenacity Retries** — exponential backoff for failed scrapes.
✅ **Multi-Selector Support** — extract multiple sections by CSS selectors in one request.
✅ **Automatic Cleanup** — browser and context close even on errors.
✅ **ETL-Ready Design** — returns BeautifulSoup objects directly for structured extraction.

---

## ⚙️ Installation

1. Clone or copy the repo.
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Install Playwright browsers:

   ```bash
   playwright install
   ```

---

## 📦 Example Usage

### ▶️ Run the main example

```bash
python index.py
```

### 🧭 Example Output

```
INFO     🧭 Running single selector scrape (no proxy)...
SUCCESS  Extracted content for selector 'h1' from https://httpbin.org/html
INFO     🌐 Running multi-selector scrape (with proxy if available)...
SUCCESS  ✅ Selector 'h1' extracted successfully.
SUCCESS  ✅ Selector 'p' extracted successfully.
🏁 Completed all scraping examples in 10.31 seconds.
```

---

## 🧩 Module Overview

### `scraper_module.proxy`

- Handles proxy fetching from public sources.
- Validates proxies via `httpbin.org/ip`.
- Caches and rotates proxies automatically.

#### Example:

```python
from scraper_module.proxy import ProxyRotator

rotator = ProxyRotator()
proxy = await rotator.get_proxy()
print(proxy)
```

### `scraper_module.scraper`

- Manages Playwright browser lifecycle.
- Fetches specific HTML sections using CSS selectors.
- Returns a dictionary `{selector: BeautifulSoup or None}`.

#### Example (Single Selector):

```python
from scraper_module.scraper import scrape_url

result = await scrape_url(
    url="https://httpbin.org/html",
    selectors="h1",
    use_proxy=False
)

soup = result.get("h1")
if soup:
    print(soup.prettify())
```

#### Example (Multiple Selectors):

```python
from scraper_module.scraper import scrape_url

result = await scrape_url(
    url="https://example.com",
    selectors=["h1", "p", "a"],
    use_proxy=True
)

for selector, soup in result.items():
    if soup:
        print(f"[{selector}]\n", soup.prettify()[:300])
    else:
        print(f"[{selector}] not found or timed out.")
```

---

## ⚙️ Configuration

Adjust constants in `scraper_module/scraper.py`:

```python
MAX_RETRIES = 4              # Retry attempts
MIN_WAIT = 1                 # Minimum retry delay (seconds)
MAX_WAIT = 8                 # Maximum retry delay (seconds)
PAGE_LOAD_TIMEOUT = 60000    # Playwright timeout in ms
SELECTOR_WAIT_TIMEOUT = 10000  # Timeout per selector in ms
```

Adjust proxy behavior in `scraper_module/proxy.py`:

```python
PROXY_CACHE_SIZE = 50           # Max proxies to keep in memory
PROXY_VALIDATION_TIMEOUT = 5    # Seconds per proxy validation
PROXY_REFRESH_INTERVAL = 300    # Seconds before fetching new proxies
```

---

## 🧠 Notes

- **Default browser:** Chromium (use `browser_type="firefox"` for Firefox).
- **Proxy sources:** [proxy-list.download](https://www.proxy-list.download/) and [free-proxy-list.net](https://free-proxy-list.net/)
- **Playwright browsers:** ensure you’ve installed Chromium or Firefox.
- **Logging** via [Loguru](https://github.com/Delgan/loguru).

---

## 🧾 Dependencies (requirements.txt)

```
playwright>=1.46.0
loguru>=0.7.2
fake-useragent>=1.5.1
tenacity>=8.3.0
fp-finder>=0.3.1
beautifulsoup4>=4.12.3
requests>=2.32.3
```

---

## 🧩 Future Enhancements

- [ ] Save extracted sections to JSON or database.
- [ ] Improve selector performance scoring.
- [ ] Integrate with Airflow DAGs or Prefect Flows.
- [ ] Add proxy health analytics.

---

## 🧑‍💻 Author

**Franz Monzales (Iki)**
Aspiring Data Engineer and Data Scientist

---

## 🏁 Quick Run Recap

```bash
pip install -r requirements.txt
playwright install
python index.py
```
