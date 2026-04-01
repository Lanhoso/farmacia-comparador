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

# Limite de segurança para evitar loops infinitos na paginação
MAX_PRODUCTS = 200

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


def _parse_int(value: Optional[str]) -> Optional[int]:
    """Extrai o primeiro inteiro de uma string. Ex: '30 comprimidos' → 30."""
    if not value:
        return None
    match = re.search(r"\d+", str(value))
    return int(match.group()) if match else None


# ── Scraper ───────────────────────────────────────────────────────────────────

class CruzVerdeScraper(BaseScraper):
    """Scraper da Cruz Verde — https://www.cruzverde.cl"""

    def __init__(self) -> None:
        super().__init__(farmacia_id="cruz_verde")

    async def scrape(self, query: str) -> list[dict]:
        """
        Navega na página de busca da Cruz Verde, carrega todos os resultados
        via scroll infinito e visita cada página de produto para enriquecer os dados.

        Paginação: scroll infinito até estabilizar ou atingir MAX_PRODUCTS.
        Enriquecimento: visita individual de cada produto para extrair
            url_image, ean_code, principio_activo, laboratorio, presentacion,
            cantidad, dosis, is_bioequivalente, requiere_receta.

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
                pass

            # ── Paginação via scroll infinito ──────────────────────────────
            self.logger.info("Carregando todos os resultados via scroll...")
            previous_count = 0
            stable_rounds  = 0

            while True:
                current_count = len(await page.query_selector_all(SELECTORS["product_name"]))

                if current_count >= MAX_PRODUCTS:
                    self.logger.info("Limite de %d produtos atingido.", MAX_PRODUCTS)
                    break

                if current_count == previous_count:
                    stable_rounds += 1
                    if stable_rounds >= 3:
                        # Contagem estável por 3 rounds consecutivos → sem mais resultados
                        break
                else:
                    stable_rounds = 0

                previous_count = current_count

                # Scroll até o fim da página
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)

            name_elements = await page.query_selector_all(SELECTORS["product_name"])
            self.logger.info(
                "Total de cards após scroll: %d (limite: %d)",
                len(name_elements), MAX_PRODUCTS
            )

            # ── Extração da página de busca ────────────────────────────────
            scraped_at = _utcnow_iso()
            raw_cards: list[dict] = []

            for name_el in name_elements[:MAX_PRODUCTS]:
                try:
                    container = await name_el.evaluate_handle(
                        "el => el.parentElement.parentElement.parentElement.parentElement"
                    )

                    name = (await name_el.inner_text()).strip()
                    if not name:
                        continue

                    price_el  = await container.query_selector(SELECTORS["price_current"])
                    price_raw = (await price_el.inner_text()).strip() if price_el else ""

                    orig_el  = await container.query_selector(SELECTORS["price_original"])
                    orig_raw = (await orig_el.inner_text()).strip() if orig_el else ""

                    precio_actual   = _parse_price(price_raw)
                    precio_original = _parse_price(orig_raw)

                    if precio_actual is None:
                        continue

                    if precio_original is None:
                        precio_original = precio_actual

                    product_url = await container.evaluate(
                        "el => { const a = el.querySelector('a[href]'); "
                        "return a ? a.href : null; }"
                    )

                    # url_image da página de busca (thumbnail do card)
                    url_image_search = await container.evaluate(
                        "el => { const img = el.querySelector('img[src]'); "
                        "return img ? img.src : null; }"
                    )

                    raw_cards.append({
                        "sku":             _extract_sku(product_url),
                        "nombre_producto": name,
                        "precio_original": precio_original,
                        "precio_actual":   precio_actual,
                        "url_product":     product_url or url,
                        "url_image":       url_image_search,
                    })

                except Exception as exc:
                    self.logger.warning("Erro ao parsear card da busca: %s", exc)
                    continue

            self.logger.info(
                "%d produtos extraídos da página de busca. Iniciando visitas individuais...",
                len(raw_cards)
            )

            # ── Visita individual para enriquecimento ──────────────────────
            detail_page = await context.new_page()
            await detail_page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            for i, card in enumerate(raw_cards):
                product_url = card.get("url_product")

                # Inicializa campos de detalhe com None
                detail: dict = {
                    "ean_code":          None,
                    "principio_activo":  None,
                    "laboratorio":       None,
                    "presentacion":      None,
                    "cantidad":          None,
                    "dosis":             None,
                    "is_bioequivalente": False,
                    "requiere_receta":   False,
                    "url_image":         card.get("url_image"),  # fallback thumbnail
                }

                # Só visita se tiver URL de produto individual (não search URL)
                if not product_url or "search?" in product_url:
                    results.append(self._build_record(card, detail, scraped_at))
                    continue

                try:
                    await detail_page.goto(
                        product_url, wait_until="domcontentloaded", timeout=20_000
                    )
                    await detail_page.wait_for_timeout(1000)

                    # url_image — imagem principal em alta resolução
                    try:
                        img_url = await detail_page.evaluate("""
                            () => {
                                const img = document.querySelector(
                                    'img.product-image, .product-gallery img, '
                                    + '.product-detail img, picture img, '
                                    + 'img[class*="product"], img[class*="main"]'
                                );
                                return img ? (img.dataset.src || img.src) : null;
                            }
                        """)
                        if img_url:
                            detail["url_image"] = img_url
                    except Exception as exc:
                        self.logger.warning("[%d] url_image: %s", i, exc)

                    # ean_code — atributo data-ean / data-barcode / ld+json gtin13
                    try:
                        ean = await detail_page.evaluate("""
                            () => {
                                // Tenta data attributes
                                const el = document.querySelector(
                                    '[data-ean], [data-barcode], [data-gtin]'
                                );
                                if (el) return el.dataset.ean || el.dataset.barcode || el.dataset.gtin;
                                // Tenta ld+json
                                const scripts = document.querySelectorAll(
                                    'script[type="application/ld+json"]'
                                );
                                for (const s of scripts) {
                                    try {
                                        const data = JSON.parse(s.textContent);
                                        if (data.gtin13) return data.gtin13;
                                        if (data.gtin)   return data.gtin;
                                    } catch(e) {}
                                }
                                return null;
                            }
                        """)
                        if ean:
                            detail["ean_code"] = str(ean).strip() or None
                    except Exception as exc:
                        self.logger.warning("[%d] ean_code: %s", i, exc)

                    # Extrai texto de seções de especificação do produto
                    # usando uma função genérica que busca por label e retorna o valor
                    try:
                        specs = await detail_page.evaluate("""
                            () => {
                                const result = {};
                                // Padrão 1: <dt>Label</dt><dd>Valor</dd>
                                document.querySelectorAll('dt').forEach(dt => {
                                    const label = dt.textContent.trim().toLowerCase();
                                    const dd = dt.nextElementSibling;
                                    if (dd) result[label] = dd.textContent.trim();
                                });
                                // Padrão 2: divs/spans com class contendo label
                                document.querySelectorAll(
                                    '[class*="spec"], [class*="detail"], [class*="attribute"], '
                                    + '[class*="feature"], [class*="info"]'
                                ).forEach(el => {
                                    const text = el.textContent.trim();
                                    const lower = text.toLowerCase();
                                    // Captura pares "Label: Valor" em texto corrido
                                    const match = text.match(/^([^:]{3,40}):\\s*(.+)$/);
                                    if (match) result[match[1].trim().toLowerCase()] = match[2].trim();
                                });
                                return result;
                            }
                        """)

                        def _find(keys: list[str]) -> Optional[str]:
                            """Busca valor no dict de specs por múltiplos sinônimos."""
                            for key in keys:
                                for spec_key, val in specs.items():
                                    if key in spec_key and val:
                                        return val.strip() or None
                            return None

                        detail["principio_activo"] = _find([
                            "principio activo", "principio", "composici", "activo"
                        ])
                        detail["laboratorio"] = _find([
                            "laboratorio", "fabricante", "laborat"
                        ])
                        detail["presentacion"] = _find([
                            "forma farmacéutica", "forma farmaceutica",
                            "presentaci", "forma"
                        ])
                        cantidad_raw = _find(["cantidad", "unidades", "contenido"])
                        detail["cantidad"] = _parse_int(cantidad_raw)
                        detail["dosis"] = _find([
                            "concentración", "concentracion", "dosis", "dosificaci"
                        ])

                    except Exception as exc:
                        self.logger.warning("[%d] specs: %s", i, exc)

                    # is_bioequivalente — texto ou imagen com "bioequivalente"
                    try:
                        is_bio = await detail_page.evaluate("""
                            () => {
                                const text = document.body.innerText.toLowerCase();
                                if (text.includes('bioequivalente')) return true;
                                const imgs = document.querySelectorAll('img[alt], img[src]');
                                for (const img of imgs) {
                                    const alt = (img.alt || img.src || '').toLowerCase();
                                    if (alt.includes('bioequivalente')) return true;
                                }
                                return false;
                            }
                        """)
                        detail["is_bioequivalente"] = bool(is_bio)
                    except Exception as exc:
                        self.logger.warning("[%d] is_bioequivalente: %s", i, exc)

                    # requiere_receta — texto "requiere receta" ou "venta bajo receta"
                    try:
                        req_receta = await detail_page.evaluate("""
                            () => {
                                const text = document.body.innerText.toLowerCase();
                                return (
                                    text.includes('requiere receta') ||
                                    text.includes('venta bajo receta') ||
                                    text.includes('receta médica')
                                );
                            }
                        """)
                        detail["requiere_receta"] = bool(req_receta)
                    except Exception as exc:
                        self.logger.warning("[%d] requiere_receta: %s", i, exc)

                except PlaywrightTimeout:
                    self.logger.warning(
                        "[%d] Timeout na página do produto: %s", i, product_url
                    )
                except Exception as exc:
                    self.logger.warning(
                        "[%d] Erro ao visitar produto %s: %s", i, product_url, exc
                    )

                results.append(self._build_record(card, detail, scraped_at))

                if (i + 1) % 10 == 0:
                    self.logger.info("Progresso: %d/%d produtos enriquecidos", i + 1, len(raw_cards))

            await detail_page.close()
            await browser.close()

        self.logger.info("Extração concluída — %d produtos no total.", len(results))
        return results

    @staticmethod
    def _build_record(card: dict, detail: dict, scraped_at: str) -> dict:
        """Monta o dict final com os 16 campos do MedicamentoRecord."""
        return {
            # Identificação
            "sku":               card.get("sku"),
            "ean_code":          detail.get("ean_code"),

            # Produto
            "nombre_producto":   card.get("nombre_producto"),
            "principio_activo":  detail.get("principio_activo"),
            "laboratorio":       detail.get("laboratorio"),
            "presentacion":      detail.get("presentacion"),
            "cantidad":          detail.get("cantidad"),
            "dosis":             detail.get("dosis"),

            # Regulatório
            "is_bioequivalente": detail.get("is_bioequivalente", False),
            "requiere_receta":   detail.get("requiere_receta", False),

            # Farmácia e preços
            "farmacia_id":       "cruz_verde",
            "precio_original":   card.get("precio_original"),
            "precio_actual":     card.get("precio_actual"),

            # URLs
            "url_product":       card.get("url_product"),
            "url_image":         detail.get("url_image"),

            # Metadados
            "scraped_at":        scraped_at,
        }
