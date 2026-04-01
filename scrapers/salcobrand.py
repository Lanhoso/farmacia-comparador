"""
salcobrand.py — Scraper da Salcobrand para farmaciabarata.cl

Status: Em desenvolvimento — scrape() lança NotImplementedError.
O método run() em BaseScraper captura isso e retorna lista vazia graciosamente.

TODO (para implementar):
    1. Inspecionar o DOM de https://salcobrand.cl com debug_scraper.py
    2. Definir SEARCH_URL e SELECTORS para os cards de produto
    3. Implementar a lógica de extração seguindo o padrão de CruzVerdeScraper
    4. Mapear os campos extraídos para os 16 campos do MedicamentoRecord
"""

from __future__ import annotations

from scrapers.base_scraper import BaseScraper

# ── Configuração (preencher ao implementar) ───────────────────────────────────

SEARCH_URL = "https://salcobrand.cl/t?q={query}"  # Placeholder — confirmar URL real

SELECTORS = {
    # TODO: inspecionar DOM e preencher os seletores corretos
    "product_name":   "",
    "price_current":  "",
    "price_original": "",
}


class SalcobrandScraper(BaseScraper):
    """Scraper da Salcobrand — https://salcobrand.cl"""

    def __init__(self) -> None:
        super().__init__(farmacia_id="salcobrand")

    async def scrape(self, query: str) -> list[dict]:
        """
        TODO: Implementar scraping da Salcobrand com Playwright.

        Passos sugeridos:
            1. Navegar para SEARCH_URL.format(query=quote(query))
            2. Aguardar os cards de produto (SELECTORS["product_name"])
            3. Para cada card, extrair: nombre_producto, precio_actual,
               precio_original, url_product, url_image
            4. Mapear para os 16 campos do MedicamentoRecord
            5. Retornar lista de dicts (validate_record() é chamado em run())

        Ver cruzverde.py como referência de implementação.
        """
        raise NotImplementedError(
            "SalcobrandScraper.scrape() ainda não implementado. "
            "Ver TODO em scrapers/salcobrand.py"
        )
