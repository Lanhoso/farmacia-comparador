"""
base_scraper.py — Classe abstrata base para todos os scrapers do farmaciabarata.cl

Cada scraper concreto (CruzVerde, Salcobrand, Ahumada) herda BaseScraper e
implementa apenas o método scrape(). A orquestração, validação e logging ficam aqui.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from schema import MedicamentoRecord

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Classe base abstrata para scrapers de farmácias chilenas.

    Subclasses devem implementar:
        scrape(self, query: str) -> list[dict]
            Retorna lista de dicts brutos (antes da validação do schema).

    O método run() orquestra a execução completa:
        1. Chama scrape() para extrair dados brutos
        2. Valida cada record com validate_record() do schema.py
        3. Registros inválidos são descartados sem derrubar os demais
        4. Retorna lista de MedicamentoRecord validados
    """

    def __init__(self, farmacia_id: str) -> None:
        """
        Args:
            farmacia_id: Identificador canônico da farmácia.
                         Deve ser um dos valores em FARMACIAS_VALIDAS.
        """
        from schema import FARMACIAS_VALIDAS
        if farmacia_id not in FARMACIAS_VALIDAS:
            raise ValueError(
                f"farmacia_id inválida: '{farmacia_id}'. "
                f"Valores aceitos: {FARMACIAS_VALIDAS}"
            )
        self.farmacia_id = farmacia_id
        self.logger = logging.getLogger(f"scrapers.{farmacia_id}")

    @abstractmethod
    async def scrape(self, query: str) -> list[dict]:
        """
        Executa o scraping para a query fornecida e retorna dados brutos.

        Args:
            query: Nome do medicamento a buscar. Ex: "Metformina 850mg"

        Returns:
            Lista de dicts com os campos brutos extraídos do site.
            Os campos devem corresponder ao schema MedicamentoRecord,
            mas podem ser strings não convertidas (validate_record() cuida disso).
        """
        ...

    async def run(self, query: str) -> list:
        """
        Executa o scraping completo com validação de schema.

        Fluxo:
            1. Loga início com timestamp
            2. Chama self.scrape(query) para obter dados brutos
            3. Valida cada record com validate_record() individualmente
            4. Registros inválidos são logados e descartados
            5. Loga fim com timestamp e contagem de resultados válidos

        Args:
            query: Nome do medicamento a buscar.

        Returns:
            Lista de MedicamentoRecord validados.
        """
        from schema import validate_record

        start = datetime.now(timezone.utc)
        self.logger.info(
            "[%s] Iniciando scraping — query: '%s' | %s",
            self.farmacia_id, query, start.isoformat()
        )

        # Extração dos dados brutos
        try:
            raw_records = await self.scrape(query)
        except NotImplementedError:
            self.logger.warning(
                "[%s] scrape() não implementado — pulando.", self.farmacia_id
            )
            return []
        except Exception as exc:
            self.logger.error(
                "[%s] Erro fatal em scrape(): %s", self.farmacia_id, exc, exc_info=True
            )
            return []

        # Validação record a record
        valid: list = []
        for i, raw in enumerate(raw_records):
            try:
                record = validate_record(raw)
                valid.append(record)
            except Exception as exc:
                self.logger.warning(
                    "[%s] Record %d descartado — erro de validação: %s",
                    self.farmacia_id, i, exc
                )

        end = datetime.now(timezone.utc)
        elapsed = (end - start).total_seconds()
        self.logger.info(
            "[%s] Scraping concluído — %d/%d records válidos | %.1fs | %s",
            self.farmacia_id, len(valid), len(raw_records), elapsed, end.isoformat()
        )

        return valid
