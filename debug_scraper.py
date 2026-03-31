"""
Debug helper — opens Cruz Verde in a visible browser window and dumps the
current DOM structure so you can update SELECTORS in scraper.py if the site
has changed its layout.

Usage:
    python debug_scraper.py "Metformina 850mg"
"""

import asyncio
import sys

from playwright.async_api import async_playwright

from scraper import BROWSER_CONTEXT_OPTS, SEARCH_URL
from urllib.parse import quote


async def debug(drug_query: str) -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(**BROWSER_CONTEXT_OPTS)
        page = await context.new_page()

        url = SEARCH_URL.format(query=quote(drug_query))
        print(f"Opening: {url}")
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(2000)

        # Report leaf elements that contain price-like text
        hits = await page.evaluate("""
            () => {
                const priceRe = /\\$[\\s\\d.,]+/;
                const seen = new Set();
                const out = [];
                for (const el of document.querySelectorAll('*')) {
                    if (el.children.length > 0) continue;
                    const text = (el.innerText || '').trim();
                    if (!priceRe.test(text) || text.length > 30) continue;
                    const cls = el.className;
                    if (seen.has(cls)) continue;
                    seen.add(cls);
                    out.push({ tag: el.tagName, cls, text });
                }
                return out.slice(0, 30);
            }
        """)

        print("\n--- Elements containing prices ---")
        for item in hits:
            print(f"  <{item['tag']} class='{item['cls']}'> → {item['text']}")

        print(f"\nPage title : {await page.title()}")
        print(f"Final URL  : {page.url}")
        print("\nPress Enter to close the browser...")
        input()
        await browser.close()


if __name__ == "__main__":
    drug = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Metformina 850mg"
    asyncio.run(debug(drug))
