#!/usr/bin/env python3
"""
Point d'entrée principal pour la génération de rapports d'analyse immobilière par commune.
"""
import argparse
import logging
import sys
import os
import datetime
from typing import Optional, Dict, List, Any

from src.generators.municipality import MunicipalityGenerator
from src.config.settings import LOG_LEVEL, LOG_FORMAT, LOG_FILE, DEFAULT_PERIOD, OUTPUT_DIR
from src.utils.json_utils import get_files_by_province, find_extreme_values, merge_json_files

# Configuration du logging
def setup_logging(log_to_file: bool = True):
    """Configure le système de logging."""
    handlers = []
    
    # Handler pour la console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handlers.append(console_handler)
    
    # Handler pour le fichier si demandé
    if log_to_file:
        # Créer le répertoire de logs si nécessaire
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        handlers.append(file_handler)
    
    # Configuration globale
    logging.basicConfig(
        level=LOG_LEVEL,
        handlers=handlers
    )

def parse_arguments():
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(description="Générateur de rapports d'analyse immobilière par commune")
    
    # Paramètres principaux
    parser.add_argument('-c', '--commune', help="Identifiant de la commune à analyser")
    parser.add_argument('-p', '--province', help="Province à analyser")
    
    # Périodes personnalisées
    parser.add_argument('--real-estate-period', help="Période pour les données immobilières (ex: 2024-Q4)")
    parser.add_argument('--economic-period', help="Période pour les données économiques (ex: 2023)")
    parser.add_argument('--demographic-period', help="Période pour les données démographiques (ex: 2023)")
    parser.add_argument('--tax-period', help="Période pour les données fiscales (ex: 2022)")
    parser.add_argument('--construction-period', help="Période pour les données de construction (ex: 2024-Q1)")
    
    # Options diverses
    parser.add_argument('--no-log-file', action='store_true', help="Désactive l'écriture des logs dans un fichier")
    parser.add_argument('--debug', action='store_true', help="Active le mode debug (logs plus détaillés)")
    
    # Commandes spéciales
    parser.add_argument('--stats', action='store_true', help="Génère des statistiques comparatives entre communes")
    parser.add_argument('--merge-province', action='store_true', 
                      help="Fusionne tous les rapports d'une province en un seul fichier")
    
    return parser.parse_args()

def get_custom_periods(args):
    """Extrait les périodes personnalisées des arguments."""
    periods = DEFAULT_PERIOD.copy()
    
    if args.real_estate_period:
        periods['real_estate_data'] = args.real_estate_period
        
    if args.economic_period:
        periods['economic_data'] = args.economic_period
        
    if args.demographic_period:
        periods['demographic_data'] = args.demographic_period
        
    if args.tax_period:
        periods['tax_data'] = args.tax_period
        
    if args.construction_period:
        periods['construction_data'] = args.construction_period
        
    return periods

def generate_stats(province: Optional[str] = None):
    """
    Génère des statistiques comparatives entre communes.
    
    Args:
        province: Province à analyser. Si None, analyse toutes les provinces.
    """
    logging.info("Génération des statistiques comparatives...")
    
    # Récupérer les fichiers par province
    province_files = get_files_by_province(OUTPUT_DIR)
    
    # Filtrer par province si spécifiée
    if province:
        if province in province_files:
            province_files = {province: province_files[province]}
        else:
            logging.error(f"Province {province} non trouvée")
            return
            
    # Métriques à comparer
    metrics = [
        "real_estate_market.municipality_overview.last_period.price_trends.median_price",
        "real_estate_market.municipality_overview.historical_trends.year_over_year.price_change_pct",
        "demographics.population_overview.population_density",
        "economic_indicators.unemployment.overall_rate",
        "investment_analysis.affordability_metrics.price_to_income_ratio",
        "investment_analysis.rental_market_potential.estimated_rental_yield"
    ]
    
    # Analyser les fichiers de chaque province
    for province_name, files in province_files.items():
        logging.info(f"Analyse de {len(files)} fichiers pour la province {province_name}...")
        
        # Construire les chemins complets des fichiers
        file_paths = [os.path.join(OUTPUT_DIR, province_name, f) for f in files]
        
        # Trouver les valeurs extrêmes
        extremes = find_extreme_values(file_paths, metrics)
        
        # Afficher les résultats
        logging.info(f"Résultats pour la province {province_name}:")
        
        for metric, values in extremes.items():
            min_value = values["min"]["value"]
            min_commune = values["min"]["commune"]
            max_value = values["max"]["value"]
            max_commune = values["max"]["commune"]
            
            metric_name = metric.split('.')[-1]
            logging.info(f"  {metric_name}:")
            logging.info(f"    - Minimum: {min_value} ({min_commune})")
            logging.info(f"    - Maximum: {max_value} ({max_commune})")
            logging.info("")
            
        # Créer un fichier JSON avec les résultats
        stats_file = os.path.join(OUTPUT_DIR, f"{province_name}_stats.json")
        with open(stats_file, 'w', encoding='utf-8') as f:
            import json
            json.dump({
                "province": province_name,
                "metrics": extremes,
                "generated_date": datetime.datetime.now().strftime("%Y-%m-%d")
            }, f, indent=2, ensure_ascii=False)
            
        logging.info(f"Statistiques enregistrées dans {stats_file}")

def merge_province_files(province: str):
    """
    Fusionne tous les rapports d'une province en un seul fichier.
    
    Args:
        province: Province à fusionner.
    """
    if not province:
        logging.error("Une province doit être spécifiée pour la fusion")
        return
        
    logging.info(f"Fusion des rapports pour la province {province}...")
    
    # Récupérer les fichiers de la province
    province_files = get_files_by_province(OUTPUT_DIR)
    
    if province not in province_files:
        logging.error(f"Province {province} non trouvée")
        return
        
    files = province_files[province]
    
    if not files:
        logging.error(f"Aucun fichier trouvé pour la province {province}")
        return
        
    # Construire les chemins complets des fichiers
    file_paths = [os.path.join(OUTPUT_DIR, province, f) for f in files]
    
    # Nom du fichier de sortie
    output_file = os.path.join(OUTPUT_DIR, f"{province}_all.json")
    
    # Fusionner les fichiers
    success = merge_json_files(file_paths, output_file)
    
    if success:
        logging.info(f"Fusion réussie! Fichier créé: {output_file}")
    else:
        logging.error(f"Échec de la fusion pour la province {province}")

def main():
    """Fonction principale."""
    # Parser les arguments
    args = parse_arguments()
    
    # Configurer le logging
    setup_logging(not args.no_log_file)
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Mode debug activé")
    
    logging.info("Démarrage du générateur de rapports d'analyse immobilière")
    
    # Traiter les commandes spéciales
    if args.stats:
        generate_stats(args.province)
        return
        
    if args.merge_province:
        merge_province_files(args.province)
        return
    
    # Extraire les périodes personnalisées
    periods = get_custom_periods(args)
    
    # Créer le générateur
    generator = MunicipalityGenerator(args.commune, args.province, periods)
    
    # Lancer la génération
    result = generator.generate()
    
    # Afficher un résumé
    if args.commune:
        if result:
            logging.info(f"Génération réussie pour la commune {args.commune}")
        else:
            logging.error(f"Échec de la génération pour la commune {args.commune}")
    else:
        success_count = sum(1 for r in result.values() if r)
        total_count = len(result)
        logging.info(f"Génération terminée: {success_count}/{total_count} rapports générés avec succès")
    
    logging.info("Fin du programme")

if __name__ == "__main__":
    main()