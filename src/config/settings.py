"""
Paramètres généraux pour l'application IMMO SCORE.
"""
import os
import logging
from datetime import datetime
from pathlib import Path

# Répertoires de base
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"

# Configuration des logs
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = BASE_DIR / "logs" / f"immo_score_{datetime.now().strftime('%Y%m%d')}.log"

# Paramètres d'extraction des données
DEFAULT_PERIOD = {
    "real_estate_data": "2024-Q4",  # Dernier trimestre disponible
    "economic_data": "2023",        # Dernière année complète
    "demographic_data": "2023",     # Dernière année complète
    "tax_data": "2022",            # Dernières données fiscales
    "construction_data": "2024-Q1", # Dernier trimestre disponible
    "cadastral_data": "2023"        # Dernière année complète
}

# Limites territoriales (vides = toutes les communes)
PROVINCES = [
    "Brabant wallon",
    "Liège",
    "Hainaut",
    "Namur",
    "Luxembourg"
]

COMMUNES = []  # Liste vide = toutes les communes des provinces spécifiées

# Formatage des nombres
NUMBER_FORMAT = {
    "decimal_separator": ",",
    "thousands_separator": ".",
    "decimal_places": 2,
    "price_decimal_places": 0,  # Prix immobiliers arrondis à l'unité
    "percentage_decimal_places": 1  # Pourcentages avec 1 décimale
}

# Paramètres de sortie JSON
JSON_FORMAT = {
    "indent": 2,
    "ensure_ascii": False,  # Pour gérer correctement les caractères accentués
    "sort_keys": False
}

# Préfixe des fichiers de sortie
OUTPUT_FILENAME_FORMAT = "immo_score_{commune_code}.json"

# Mode debug (plus de détails dans les logs)
DEBUG_MODE = True

# Version actuelle de la structure JSON
JSON_STRUCTURE_VERSION = "1.0"

# Valeurs minimales pour considérer les données comme valides
DATA_VALIDITY = {
    "min_transactions": 5,  # Nombre minimum de transactions pour analyse de prix
    "min_population": 100,  # Population minimale pour analyse démographique
    "min_enterprises": 10   # Nombre minimum d'entreprises pour analyse économique
}

# Seuils pour les alertes et indicateurs
THRESHOLDS = {
    "high_price_growth": 10.0,  # Croissance de prix > 10% = marché en forte hausse
    "low_price_growth": -5.0,   # Croissance de prix < -5% = marché en baisse
    "high_unemployment": 12.0,  # Chômage > 12% = taux élevé
    "high_density": 1000,       # Densité > 1000 hab/km² = zone densément peuplée
    "high_yield": 5.0,          # Rendement locatif > 5% = rendement attractif
    "high_price_income_ratio": 6.0  # Ratio prix/revenu > 6 = faible accessibilité
}

# Activation des sections du rapport
ENABLED_SECTIONS = {
    "real_estate_market": True,
    "building_development": True,
    "demographics": True,
    "economic_indicators": True,
    "geographical_context": True,
    "investment_analysis": True,
    "property_rights": True,
    "rental_market": True,
    "cadastral_data": True,
    "building_characteristics": True
}