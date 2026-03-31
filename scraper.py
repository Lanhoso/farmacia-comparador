"""
Pharmacy price scraper — Cruz Verde (Chile)
Extracts drug prices using Playwright to handle the JS-heavy Angular site.
"""

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout


SEARCH_URL = "https://www.cruzverde.cl/search?query={query}"
OUTPUT_DIR = Path(__file__).parent / "data"

# CSS selectors verified against live site (Angular + Tailwind).
# If scraping breaks, run: python debug_scraper.py "<drug>" to inspect current DOM.
SELECTORS = {
    "product_name":   "h2.mt-4",
    "price_current":  "ml-price-tag-v2 p[class*='font-poppins']",
    "price_original": "ml-price-tag-v2 p[class*='line-through']",
}

# Shared browser context options — also imported by debug_scraper.py
BROWSER_CONTEXT_OPTS = {
    "user_agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "viewport": {"width": 1280, "height": 800},
    "locale": "es-CL",
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_price(raw: str) -> Optional[int]:
    """Strip non-digits from CLP strings like '$ 5.032' → 5032."""
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else None


async def scrape_cruzverde(page, drug_query: str) -> List[dict]:
    url = SEARCH_URL.format(query=quote(drug_query))
    results = []

    print(f"  Navigating to: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except PlaywrightTimeout:
        print("  ERROR: Page load timed out")
        return results

    try:
        await page.wait_for_selector(SELECTORS["product_name"], timeout=15000)
    except PlaywrightTimeout:
        print("  WARNING: No product cards found within timeout")
        return results

    # Wait for at least one price to render (lazy-loaded after product names)
    try:
        await page.wait_for_selector(SELECTORS["price_current"], timeout=8000)
    except PlaywrightTimeout:
        pass  # proceed anyway; some results may be missing prices

    name_elements = await page.query_selector_all(SELECTORS["product_name"])
    print(f"  Found {len(name_elements)} product cards")

    scraped_at = _utcnow()

    for name_el in name_elements:
        try:
            # Reach the card container in one JS round-trip instead of four
            container = await name_el.evaluate_handle(
                "el => el.parentElement.parentElement.parentElement.parentElement"
            )

            name = (await name_el.inner_text()).strip()

            price_el = await container.query_selector(SELECTORS["price_current"])
            price_raw = (await price_el.inner_text()).strip() if price_el else ""

            orig_el = await container.query_selector(SELECTORS["price_original"])
            orig_raw = (await orig_el.inner_text()).strip() if orig_el else ""

            price = _parse_price(price_raw)
            if not name or price is None:
                continue

            orig_price = _parse_price(orig_raw)

            results.append({
                "pharmacy": "Cruz Verde",
                "drug_name": name,
                "price_clp": price,
                "original_price_clp": orig_price,
                "url": url,
                "scraped_at": scraped_at,
            })
        except Exception as exc:
            print(f"  WARNING: Error parsing card: {exc}")
            continue

    return results


async def run_scraper(drug_query: str = "Metformina 850mg") -> List[dict]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(**BROWSER_CONTEXT_OPTS)
        page = await context.new_page()
        # Mask the WebDriver flag so the site doesn't detect headless Chrome
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        print(f"\n=== Scraping Cruz Verde for: {drug_query} ===")
        results = await scrape_cruzverde(page, drug_query)
        print(f"  Extracted {len(results)} results")

        await browser.close()

    return results


def save_results(results: List[dict], drug_query: str) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    slug = re.sub(r"[^\w]", "_", drug_query.lower())
    out_path = OUTPUT_DIR / f"{slug}.json"
    payload = {
        "query": drug_query,
        "generated_at": _utcnow(),
        "count": len(results),
        "results": results,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"Saved {len(results)} results → {out_path}")
    return out_path, payload


def embed_data_in_html(payload: dict) -> None:
    """Inject scraped data inline into index.html so it renders without a server."""
    html_path = Path(__file__).parent / "index.html"
    if not html_path.exists():
        return
    html = html_path.read_text()
    inline = (
        f'<script>window.SCRAPED_DATA = '
        f'{json.dumps(payload, ensure_ascii=False)};</script>'
    )
    html = re.sub(
        r"<!-- INLINE_DATA_START -->.*?<!-- INLINE_DATA_END -->",
        f"<!-- INLINE_DATA_START -->\n{inline}\n<!-- INLINE_DATA_END -->",
        html,
        flags=re.DOTALL,
    )
    html_path.write_text(html)
    print("Embedded data → index.html")


async def main():
    drug = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Metformina 850mg"
    results = await run_scraper(drug)

    if not results:
        print("\nNo results found — run debug_scraper.py to inspect live selectors.")
    else:
        print("\nResults preview:")
        for r in results[:5]:
            disc = f" (orig ${r['original_price_clp']:,})" if r.get("original_price_clp") else ""
            print(f"  {r['drug_name']} — ${r['price_clp']:,} CLP{disc}")

    _path, payload = save_results(results, drug)
    embed_data_in_html(payload)


if __name__ == "__main__":
    asyncio.run(main())
