"""
main.py — Orquestrador principal do farmaciabarata.cl

Fluxo de execução:
    1. Carrega configuração via variáveis de ambiente (GitHub Actions Secrets)
    2. Executa os scrapers implementados para cada query
    3. Concatena resultados em um DataFrame Pandas
    4. Ação A: UPSERT no Supabase (precos_hoy)
    5. Ação B: Upload Parquet particionado no Cloudflare R2
    6. Imprime resumo final

Uso:
    python main.py                          # Usa queries padrão
    python main.py "Losartan 50mg"          # Busca query específica
    python main.py "Metformina 850mg" "Ibuprofeno 400mg"
"""

from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import asdict
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("main")

# ── Queries padrão ────────────────────────────────────────────────────────────

DEFAULT_QUERIES = [
    "Metformina 850mg",
]

# ── Scrapers ativos (adicionar novos aqui quando implementados) ───────────────

def get_active_scrapers():
    """Retorna instâncias dos scrapers a executar."""
    from scrapers import CruzVerdeScraper, SalcobrandScraper, AhumadaScraper
    return [
        CruzVerdeScraper(),
        SalcobrandScraper(),   # Retorna [] até ser implementado
        AhumadaScraper(),      # Retorna [] até ser implementado
    ]


# ── Orquestração principal ────────────────────────────────────────────────────

async def run_all_scrapers(queries: list[str]) -> list:
    """
    Executa todos os scrapers para todas as queries.

    Scrapers que lançam NotImplementedError são ignorados automaticamente
    (BaseScraper.run() captura e retorna lista vazia).

    Returns:
        Lista flat de MedicamentoRecord de todos os scrapers e queries.
    """
    scrapers = get_active_scrapers()
    all_records = []

    for query in queries:
        logger.info("━━━ Query: '%s' ━━━", query)
        for scraper in scrapers:
            records = await scraper.run(query)
            all_records.extend(records)
            logger.info(
                "  %s → %d records válidos", scraper.farmacia_id, len(records)
            )

    return all_records


def build_dataframe(records: list):
    """Converte lista de MedicamentoRecord para DataFrame Pandas."""
    try:
        import pandas as pd
    except ImportError:
        logger.error("pandas não instalado. Execute: pip install pandas")
        return None

    if not records:
        logger.warning("Nenhum record para construir DataFrame.")
        return None

    rows = [asdict(r) for r in records]
    df = pd.DataFrame(rows)
    return df


def persist(records: list, df) -> dict[str, bool]:
    """
    Persiste os dados nas duas camadas de armazenamento.

    Returns:
        dict com resultado de cada operação: {"supabase": bool, "r2": bool}
    """
    from database_manager import upsert_to_supabase, upload_to_r2

    results = {"supabase": False, "r2": False}

    # ── Supabase ──
    logger.info("Enviando %d records para o Supabase...", len(records))
    results["supabase"] = upsert_to_supabase(records)

    # ── R2 — upload por farmácia ──
    if df is not None and len(df) > 0:
        farmacias = df["farmacia_id"].unique()
        r2_ok = True
        for farmacia_id in farmacias:
            df_farmacia = df[df["farmacia_id"] == farmacia_id].copy()
            logger.info(
                "Upload R2: %s → %d records", farmacia_id, len(df_farmacia)
            )
            ok = upload_to_r2(df_farmacia, farmacia_id)
            if not ok:
                r2_ok = False
        results["r2"] = r2_ok
    else:
        logger.warning("DataFrame vazio — upload R2 ignorado.")

    return results


def print_summary(
    queries: list[str],
    records: list,
    persist_results: dict[str, bool],
    elapsed: float,
) -> None:
    """Imprime resumo final da execução."""
    print()
    print("=" * 60)
    print("  RESUMO DA EXECUÇÃO — farmaciabarata.cl")
    print("=" * 60)
    print(f"  Queries executadas : {len(queries)}")
    print(f"  Queries            : {', '.join(queries)}")
    print(f"  Records extraídos  : {len(records)}")

    if records:
        from collections import Counter
        contagem = Counter(r.farmacia_id for r in records)
        for farmacia, count in sorted(contagem.items()):
            print(f"    • {farmacia:<15}: {count} records")

    print(f"  Supabase upsert    : {'✓ OK' if persist_results.get('supabase') else '✗ ERRO'}")
    print(f"  R2 upload          : {'✓ OK' if persist_results.get('r2') else '✗ ERRO'}")
    print(f"  Tempo total        : {elapsed:.1f}s")
    print("=" * 60)
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    start = datetime.now(timezone.utc)

    # Queries: argumentos CLI ou lista padrão
    queries = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_QUERIES
    logger.info("Iniciando execução — %d queries: %s", len(queries), queries)

    # ── Scraping ──
    records = await run_all_scrapers(queries)
    logger.info("Total de records válidos extraídos: %d", len(records))

    # ── DataFrame ──
    df = build_dataframe(records)

    # ── Persistência ──
    persist_results = {"supabase": False, "r2": False}
    if records:
        persist_results = persist(records, df)
    else:
        logger.warning("Nenhum record extraído — persistência ignorada.")

    # ── Resumo ──
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print_summary(queries, records, persist_results, elapsed)


if __name__ == "__main__":
    asyncio.run(main())
