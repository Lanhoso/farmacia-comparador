"""
cruzverde.py — Scraper da Cruz Verde para farmaciabarata.cl

Herda BaseScraper e implementa scrape() usando Playwright para navegar
no site Angular da Cruz Verde (cruzverde.cl).

Seletores CSS verificados contra o site ao vivo (atualizar se o layout mudar).
Use debug_scraper.py para inspecionar o DOM atual caso os seletores parem de funcionar.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

from scrapers.base_scraper import BaseScraper

# ── Configuração ──────────────────────────────────────────────────────────────

SEARCH_URL = "https://www.cruzverde.cl/search?query={query}"

# CSS selectors verificados contra o site ao vivo (Angular + Tailwind)
SELECTORS = {
    "product_name":   "h2.mt-4",
    "price_current":  "ml-price-tag-v2 p[class*='font-poppins']",
    "price_original": "ml-price-tag-v2 p[class*='line-through']",
}

# Configuração do contexto do browser — compartilhada com debug_scraper.py
BROWSER_CONTEXT_OPTS = {
    "user_agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "viewport": {"width": 1280, "height": 800},
    "locale": "es-CL",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_price(raw: str) -> Optional[int]:
    """Extrai inteiro CLP de strings formatadas. Ex: '$ 5.032' → 5032."""
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else None


def _extract_sku(url: Optional[str]) -> Optional[str]:
    """
    Extrai o SKU do produto a partir da URL da Cruz Verde.

    Ex: 'https://www.cruzverde.cl/metformina-850-mg-60-comprimidos/270505.html'
         → '270505'
    """
    if not url:
        return None
    match = re.search(r"/(\d+)\.html$", url)
    return match.group(1) if match else None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ── Scraper ───────────────────────────────────────────────────────────────────

class CruzVerdeScraper(BaseScraper):
    """Scraper da Cruz Verde — https://www.cruzverde.cl"""

    def __init__(self) -> None:
        super().__init__(farmacia_id="cruz_verde")

    async def scrape(self, query: str) -> list[dict]:
        """
        Navega na página de busca da Cruz Verde e extrai os produtos listados.

        Retorna lista de dicts mapeados para os 16 campos do MedicamentoRecord.
        Campos não extraídos pelo scraper atual retornam None:
            ean_code, principio_activo, laboratorio, presentacion,
            cantidad, dosis, is_bioequivalente, requiere_receta, url_image

        Args:
            query: Nome do medicamento. Ex: "Metformina 850mg"

        Returns:
            Lista de dicts brutos (validate_record() é chamado em run())
        """
        from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

        url = SEARCH_URL.format(query=quote(query))
        results: list[dict] = []

        self.logger.info("Navegando para: %s", url)

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(**BROWSER_CONTEXT_OPTS)
            page = await context.new_page()

            # Máscara anti-detecção de headless
            await page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            # ── Navegação ──
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            except PlaywrightTimeout:
                self.logger.error("Timeout ao carregar a página: %s", url)
                await browser.close()
                return results

            # ── Esperar produtos ──
            try:
                await page.wait_for_selector(SELECTORS["product_name"], timeout=15_000)
            except PlaywrightTimeout:
                self.logger.warning("Nenhum card de produto encontrado (timeout 15s).")
                await browser.close()
                return results

            # Aguarda preços renderizarem (lazy-loaded)
            try:
                await page.wait_for_selector(SELECTORS["price_current"], timeout=8_000)
            except PlaywrightTimeout:
                pass  # Continua mesmo sem preços — alguns resultados podem ter

            # ── Extração ──
            name_elements = await page.query_selector_all(SELECTORS["product_name"])
            self.logger.info("%d cards de produto encontrados.", len(name_elements))

            scraped_at = _utcnow_iso()

            for name_el in name_elements:
                try:
                    # Sobe 4 níveis para o container do card
                    container = await name_el.evaluate_handle(
                        "el => el.parentElement.parentElement.parentElement.parentElement"
                    )

                    # Nome do produto
                    name = (await name_el.inner_text()).strip()
                    if not name:
                        continue

                    # Preços
                    price_el = await container.query_selector(SELECTORS["price_current"])
                    price_raw = (await price_el.inner_text()).strip() if price_el else ""

                    orig_el = await container.query_selector(SELECTORS["price_original"])
                    orig_raw = (await orig_el.inner_text()).strip() if orig_el else ""

                    precio_actual   = _parse_price(price_raw)
                    precio_original = _parse_price(orig_raw)

                    if precio_actual is None:
                        continue  # Sem preço → descarta

                    # Se não há preço riscado, precio_original = precio_actual
                    if precio_original is None:
                        precio_original = precio_actual

                    # URL do produto (âncora dentro do card)
                    product_url = await container.evaluate(
                        "el => { const a = el.querySelector('a[href]'); "
                        "return a ? a.href : null; }"
                    )

                    # SKU extraído do número final da URL do produto
                    sku = _extract_sku(product_url)

                    # ── Mapeamento para MedicamentoRecord (16 campos) ──
                    results.append({
                        # Identificação
                        "sku":               sku,
                        "ean_code":          None,           # TODO: buscar em ld+json

                        # Produto
                        "nombre_producto":   name,
                        "principio_activo":  None,           # TODO: extrair da página do produto
                        "laboratorio":       None,           # TODO: extrair da página do produto
                        "presentacion":      None,           # TODO: extrair da página do produto
                        "cantidad":          None,           # TODO: extrair do nome ou página
                        "dosis":             None,           # TODO: extrair do nome ou página

                        # Regulatório
                        "is_bioequivalente": False,          # TODO: detectar selo na página
                        "requiere_receta":   False,          # TODO: detectar aviso na página

                        # Farmácia e preços
                        "farmacia_id":       self.farmacia_id,
                        "precio_original":   precio_original,
                        "precio_actual":     precio_actual,

                        # URLs
                        "url_product":       product_url or url,
                        "url_image":         None,           # TODO: extrair src da imagem do card

                        # Metadados
                        "scraped_at":        scraped_at,
                    })

                except Exception as exc:
                    self.logger.warning("Erro ao parsear card: %s", exc)
                    continue

            await browser.close()

        return results
