"""
scrapers/ — Módulo de scrapers modulares do farmaciabarata.cl

Importações convenientes:
    from scrapers import CruzVerdeScraper, SalcobrandScraper, AhumadaScraper
"""

from scrapers.cruzverde import CruzVerdeScraper
from scrapers.salcobrand import SalcobrandScraper
from scrapers.ahumada import AhumadaScraper

__all__ = ["CruzVerdeScraper", "SalcobrandScraper", "AhumadaScraper"]
