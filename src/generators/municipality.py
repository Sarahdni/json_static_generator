"""
Générateur de rapport complet pour une commune.
Coordonne les différents extracteurs et processeurs pour produire un fichier JSON complet.
"""
import logging
import json
import os
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from src.config.settings import DEFAULT_PERIOD, OUTPUT_DIR, JSON_FORMAT
from src.extractors.real_estate import RealEstateExtractor
from src.extractors.demographics import DemographicsExtractor
from src.extractors.economics import EconomicsExtractor
from src.extractors.building import BuildingExtractor
from src.extractors.geography import GeographyExtractor

from src.processors.base import BaseProcessor
from src.processors.real_estate import RealEstateProcessor
from src.processors.demographics import DemographicsProcessor
from src.processors.economics import EconomicsProcessor
from src.processors.building_dev import BuildingDevProcessor
from src.processors.investment import InvestmentProcessor

from src.utils.json_utils import DecimalEncoder

logger = logging.getLogger(__name__)

class MunicipalityGenerator:
    """Générateur de rapport complet pour une commune."""
    
    def __init__(self, commune_id: Optional[str] = None, province: Optional[str] = None, data_periods: Optional[Dict[str, str]] = None):
        """
        Initialise le générateur pour une commune spécifique ou toutes les communes d'une province.
        
        Args:
            commune_id: Identifiant de la commune. Si None, génère pour toutes les communes de la province.
            province: Province à traiter. Si None, génère pour toutes les provinces.
            data_periods: Périodes spécifiques pour chaque type de données.
        """
        self.commune_id = commune_id
        self.province = province
        self.data_periods = data_periods or DEFAULT_PERIOD
        self.base_processor = BaseProcessor()
        
        # Créer les répertoires de sortie s'ils n'existent pas
        self.ensure_output_dirs()
        
    def ensure_output_dirs(self):
        """Crée les répertoires de sortie s'ils n'existent pas."""
        provinces = ["brabant_wallon", "liege", "hainaut", "namur", "luxembourg"]
        
        # Créer le répertoire principal
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Créer les sous-répertoires par province
        for province in provinces:
            os.makedirs(os.path.join(OUTPUT_DIR, province), exist_ok=True)
            
        logger.info(f"Répertoires de sortie créés ou vérifiés dans {OUTPUT_DIR}")
        
    def get_commune_info(self, commune_id: str) -> Tuple[Dict[str, Any], str]:
        """
        Récupère les informations de base d'une commune.
        
        Args:
            commune_id: Identifiant de la commune.
            
        Returns:
            tuple: Informations de la commune et nom de fichier de sortie.
        """
        # Utiliser l'extracteur géographique pour récupérer les infos de la commune
        geo_extractor = GeographyExtractor(commune_id)
        geo_data = geo_extractor.extract_data()
        
        if not geo_data or "commune_info" not in geo_data:
            logger.error(f"Aucune information trouvée pour la commune {commune_id}")
            return {}, ""
            
        commune_info = geo_data["commune_info"]
        
        # Si certaines informations manquent, essayer avec l'extracteur de base
        if not commune_info.get('province'):
            with geo_extractor.get_db_session() as session:
                communes = geo_extractor.get_communes(session)
                if communes and len(communes) > 0:
                    # Compléter les informations manquantes
                    for key, value in communes[0].items():
                        if key not in commune_info or not commune_info[key]:
                            commune_info[key] = value
        
        # Déterminer le répertoire de sortie en fonction de la province
        province = commune_info.get('province', '').lower().replace(' ', '_')
        if not province or province == 'non_spécifiée':
            province = 'autres'
        
        # Créer le nom de fichier de sortie
        filename = f"immo_score_{commune_id}.json"
        output_path = os.path.join(OUTPUT_DIR, province, filename)
        
        return commune_info, output_path
        
    def extract_all_data(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait toutes les données pour une commune.
        
        Args:
            commune_id: Identifiant de la commune.
            
        Returns:
            dict: Toutes les données extraites pour la commune.
        """
        logger.info(f"Début de l'extraction des données pour la commune {commune_id}")
        
        # Initialiser les extracteurs
        re_extractor = RealEstateExtractor(commune_id, self.province, self.data_periods)
        demo_extractor = DemographicsExtractor(commune_id, self.province, self.data_periods)
        eco_extractor = EconomicsExtractor(commune_id, self.province, self.data_periods)
        building_extractor = BuildingExtractor(commune_id, self.province, self.data_periods)
        geography_extractor = GeographyExtractor(commune_id, self.province, self.data_periods)  # Nouvel extracteur
        
        # Extraire les données
        real_estate_data = re_extractor.extract_data()
        demographics_data = demo_extractor.extract_data()
        economics_data = eco_extractor.extract_data()
        building_data = building_extractor.extract_data()
        geography_data = geography_extractor.extract_data()  # Nouvelles données
        
        logger.info(f"Extraction des données terminée pour la commune {commune_id}")
        
        return {
            "real_estate": real_estate_data,
            "demographics": demographics_data,
            "economics": economics_data,
            "building": building_data,
            "geography": geography_data  # Ajout des données géographiques
        }
        
    def process_data(self, raw_data: Dict[str, Any], commune_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite toutes les données et génère la structure JSON finale.
        
        Args:
            raw_data: Données brutes extraites.
            commune_info: Informations de base de la commune.
            
        Returns:
            dict: Structure JSON finale.
        """
        logger.info(f"Début du traitement des données pour la commune {commune_info.get('commune_name')}")
        
        # Initialiser les processeurs
        re_processor = RealEstateProcessor()
        demo_processor = DemographicsProcessor()
        eco_processor = EconomicsProcessor()
        building_processor = BuildingDevProcessor()
        invest_processor = InvestmentProcessor()
        
        # Extraire les données brutes
        real_estate_raw = raw_data.get("real_estate", {})
        demographics_raw = raw_data.get("demographics", {})
        economics_raw = raw_data.get("economics", {})
        building_raw = raw_data.get("building", {})
        geography_raw = raw_data.get("geography", {})  # Nouvelles données géographiques
        
        # Obtenir la superficie de la commune à partir des données géographiques
        area_km2 = None
        if geography_raw and "commune_info" in geography_raw:
            area_km2 = geography_raw["commune_info"].get("area_km2")
            
            # Mettre à jour les informations de la commune avec les données géographiques
            if "commune_info" in geography_raw:
                geo_commune_info = geography_raw["commune_info"]
                commune_info.update({
                    'area_km2': geo_commune_info.get('area_km2'),
                    'province': geo_commune_info.get('province', commune_info.get('province')),
                    'region': geo_commune_info.get('region', commune_info.get('region'))
                })
        
        # Traiter les données sectionnelles avec protection contre les erreurs
        try:
            real_estate_processed = re_processor.process_data(real_estate_raw)
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données immobilières: {str(e)}")
            real_estate_processed = {}
            
        try:
            # Pour les données démographiques, passer la superficie
            demographics_result = demo_processor.process_data(demographics_raw, area_km2)
            # Extraire les sections du résultat démographique
            demographics_processed = demographics_result.get("demographics", {})
            geographical_processed = demographics_result.get("geographical_context", {})
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données démographiques: {str(e)}")
            demographics_processed = {}
            geographical_processed = {}
        
        # Intégrer les données des secteurs statistiques dans le contexte géographique
        if "statistical_sectors" in geography_raw and geographical_processed:
            if "sectors" not in geographical_processed:
                geographical_processed["sectors"] = {}
            geographical_processed["sectors"]["statistical_sectors"] = geography_raw["statistical_sectors"]
        
        try:
            economics_processed = eco_processor.process_data(economics_raw)
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données économiques: {str(e)}")
            economics_processed = {}
            
        try:
            building_processed = building_processor.process_data(building_raw, real_estate_processed)
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données de construction: {str(e)}")
            building_processed = {}
        
        # Pour l'analyse d'investissement, nous avons besoin des données traitées des autres sections
        try:
            investment_processed = invest_processor.process_data(
                real_estate_processed, 
                economics_processed,
                demographics_processed,
                building_processed
            )
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données d'investissement: {str(e)}")
            investment_processed = {}
        
        # Générer les métadonnées
        metadata = self.base_processor.create_metadata(commune_info, self.data_periods)
        
        # Structure JSON complète
        result = {
            "metadata": metadata,
            "real_estate_market": real_estate_processed,
            "building_development": building_processed,
            "demographics": demographics_processed,
            "economic_indicators": economics_processed,
            "geographical_context": geographical_processed,
            "investment_analysis": investment_processed
        }
        
        logger.info(f"Traitement des données terminé pour la commune {commune_info.get('commune_name')}")
        
        return result
        
    def format_json(self, data: Dict[str, Any]) -> str:
        """Formatte les données en JSON selon les paramètres définis."""
        return json.dumps(
            data,
            indent=JSON_FORMAT.get('indent', 2),
            ensure_ascii=JSON_FORMAT.get('ensure_ascii', False),
            sort_keys=JSON_FORMAT.get('sort_keys', False),
            cls=DecimalEncoder  # Ajouter cette ligne
        )
        
    def save_json(self, data: Dict[str, Any], output_path: str) -> bool:
        """Sauvegarde les données JSON dans un fichier."""
        try:
            # Créer le répertoire parent s'il n'existe pas
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Formater le JSON avec notre encodeur personnalisé
            json_string = json.dumps(
                data,
                indent=JSON_FORMAT.get('indent', 2),
                ensure_ascii=JSON_FORMAT.get('ensure_ascii', False),
                sort_keys=JSON_FORMAT.get('sort_keys', False),
                cls=DecimalEncoder
            )
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(json_string)
                
            logger.info(f"Fichier JSON sauvegardé avec succès: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du fichier JSON {output_path}: {str(e)}")
            return False
            
    def generate_for_commune(self, commune_id: str) -> bool:
        """
        Génère un rapport complet pour une commune spécifique.
        
        Args:
            commune_id: Identifiant de la commune.
            
        Returns:
            bool: True si l'opération a réussi, False sinon.
        """
        try:
            # Récupérer les informations de la commune et le chemin de sortie
            commune_info, output_path = self.get_commune_info(commune_id)
            
            if not commune_info:
                logger.error(f"Impossible de générer le rapport pour la commune {commune_id}: informations manquantes")
                return False
                
            # Extraire toutes les données
            raw_data = self.extract_all_data(commune_id)
            
            # Traiter les données et générer la structure JSON
            result = self.process_data(raw_data, commune_info)
            
            # Sauvegarder le JSON
            success = self.save_json(result, output_path)
            
            if success:
                logger.info(f"Rapport généré avec succès pour la commune {commune_info.get('commune_name')} ({commune_id})")
            else:
                logger.error(f"Échec de la génération du rapport pour la commune {commune_info.get('commune_name')} ({commune_id})")
                
            return success
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération du rapport pour la commune {commune_id}: {str(e)}")
            return False
            
    def generate_all(self) -> Dict[str, bool]:
        """
        Génère des rapports pour toutes les communes de la province spécifiée.
        
        Returns:
            dict: Dictionnaire avec les IDs de communes comme clés et les résultats comme valeurs.
        """
        # Récupérer la liste des communes à traiter
        re_extractor = RealEstateExtractor(province=self.province)
        
        with re_extractor.get_db_session() as session:
            communes = re_extractor.get_communes(session)
            
        if not communes or len(communes) == 0:
            logger.error(f"Aucune commune trouvée pour la province {self.province}")
            return {}
            
        results = {}
        total_communes = len(communes)
        
        logger.info(f"Début de la génération de rapports pour {total_communes} communes")
        
        # Générer un rapport pour chaque commune
        for i, commune in enumerate(communes, 1):
            commune_id = commune['commune_id']
            commune_name = commune['commune_name']
            
            logger.info(f"Génération du rapport pour {commune_name} ({commune_id}) - {i}/{total_communes}")
            
            result = self.generate_for_commune(commune_id)
            results[commune_id] = result
            
        # Afficher un résumé
        success_count = sum(1 for result in results.values() if result)
        
        logger.info(f"Génération terminée : {success_count}/{total_communes} rapports générés avec succès")
        
        return results
        
    def generate(self) -> Any:
        """
        Point d'entrée principal pour la génération de rapports.
        Génère un rapport pour une commune spécifique ou pour toutes les communes de la province.
        
        Returns:
            bool or dict: Résultat de la génération.
        """
        if self.commune_id:
            # Générer pour une commune spécifique
            return self.generate_for_commune(self.commune_id)
        else:
            # Générer pour toutes les communes de la province
            return self.generate_all()