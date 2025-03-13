"""
Extracteur pour les données géographiques.
Extrait les données des tables dim_geography et dim_statistical_sectors.
"""
import logging
from typing import Dict, List, Any, Optional

from src.extractors.base import BaseExtractor
from src.config.settings import DEFAULT_PERIOD

logger = logging.getLogger(__name__)

class GeographyExtractor(BaseExtractor):
    """Extracteur pour les données géographiques."""
    
    def __init__(self, commune_id=None, province=None, period=None):
        """
        Initialise l'extracteur de données géographiques.
        
        Args:
            commune_id (str, optional): Identifiant de la commune.
            province (str, optional): Province à extraire.
            period (dict, optional): Périodes d'extraction (non utilisé pour les données géographiques).
        """
        super().__init__(commune_id, province, period)

    def extract_commune_info(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les informations de base d'une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Informations de base de la commune.
        """
        self.log_extraction_start(f"informations géographiques (commune {commune_id})")
        
        query = """
            SELECT 
                g.id_geography,
                g.tx_name_fr AS commune_name,
                g.tx_name_nl AS commune_name_nl,
                g.cd_lau AS postal_code,
                g.cd_parent,
                g.cd_level,
                g.cd_refnis
            FROM 
                dw.dim_geography g
            WHERE 
                g.id_geography = :commune_id
                AND g.fl_current = TRUE
        """
        
        params = {'commune_id': commune_id}
        
        try:
            result = self.execute_query(query, params)
            
            if not result or len(result) == 0:
                logger.warning(f"Aucune information trouvée pour la commune {commune_id}")
                return {}
                
            commune_info = result[0]
            
            # Chercher la province associée à cette commune
            province_info = self.find_province_for_commune(commune_info['cd_parent'])
            
            # Compléter les informations avec les données de la province
            commune_info['province'] = province_info.get('province_name', 'Non spécifiée')
            commune_info['region'] = province_info.get('region_name', 'Non spécifiée')
            
            # Extraire les informations spatiales (superficie)
            spatial_info = self.extract_commune_spatial_data(commune_id)
            
            # Fusionner toutes les informations
            commune_data = {
                'commune_id': commune_info['id_geography'],
                'commune_name': commune_info['commune_name'],
                'commune_name_nl': commune_info['commune_name_nl'],
                'postal_code': commune_info['postal_code'],
                'cd_refnis': commune_info['cd_refnis'],
                'cd_parent': commune_info['cd_parent'],
                'province': commune_info['province'],
                'region': commune_info['region']
            }
            
            # Ajouter les informations spatiales si disponibles
            if spatial_info:
                commune_data.update(spatial_info)
                
            self.log_extraction_end(f"informations géographiques (commune {commune_id})", 1)
            
            return commune_data
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des informations géographiques: {str(e)}")
            return {}       
            
    def find_province_for_commune(self, parent_code: str) -> Dict[str, Any]:
        """
        Trouve la province associée à une commune à partir de son code parent.
        
        Args:
            parent_code (str): Code parent de la commune (format BE2xx).
            
        Returns:
            dict: Informations sur la province.
        """
        if not parent_code:
            return {}
            
        try:
            # Essayer de trouver la province directement par son code NUTS
            query = """
                SELECT 
                    g.id_geography AS province_id,
                    g.tx_name_fr AS province_name,
                    r.tx_name_fr AS region_name
                FROM 
                    dw.dim_geography g
                LEFT JOIN 
                    dw.dim_geography r ON g.cd_parent = r.cd_refnis
                WHERE 
                    g.cd_level = 2  -- niveau province
                    AND g.fl_current = TRUE
                    AND (g.cd_refnis = :parent_code OR g.cd_refnis LIKE :parent_pattern)
                LIMIT 1
            """
            
            # Si le parent_code est au format BE2xx, on cherche aussi des variantes
            parent_pattern = parent_code[:3] + '%'
            
            params = {
                'parent_code': parent_code,
                'parent_pattern': parent_pattern
            }
            
            result = self.execute_query(query, params)
            
            if result and len(result) > 0:
                return result[0]
                
            # Si on ne trouve pas la province, on retourne des infos par défaut
            return {
                'province_id': None,
                'province_name': 'Province inconnue',
                'region_name': 'Région inconnue'
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de la recherche de la province: {str(e)}")
            return {}
            
    def extract_commune_spatial_data(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données spatiales d'une commune (superficie, etc.).
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données spatiales de la commune.
        """
        try:
            # D'abord récupérer le cd_refnis de la commune
            query_refnis = """
                SELECT cd_refnis
                FROM dw.dim_geography
                WHERE id_geography = :commune_id AND fl_current = TRUE
            """
            
            params = {'commune_id': commune_id}
            
            refnis_result = self.execute_query(query_refnis, params)
            
            if not refnis_result or len(refnis_result) == 0:
                logger.warning(f"Impossible de trouver le code refnis pour la commune {commune_id}")
                return {}
                
            refnis = refnis_result[0]['cd_refnis']
            
            # Calculer la superficie totale en additionnant les secteurs statistiques
            query = """
                SELECT 
                    SUM(ss.ms_area_ha) / 100 AS area_km2
                FROM 
                    dw.dim_statistical_sectors ss
                WHERE 
                    ss.cd_refnis = :refnis
                    AND ss.dt_end IS NULL
            """
            
            params = {'refnis': refnis}
            
            result = self.execute_query(query, params)
            
            if result and len(result) > 0 and result[0]['area_km2'] is not None:
                return {'area_km2': result[0]['area_km2']}
                    
            return {}
                
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données spatiales: {str(e)}")
            return {}       
            
    def extract_statistical_sectors(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les secteurs statistiques d'une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données des secteurs statistiques regroupées par secteur.
        """
        self.log_extraction_start(f"secteurs statistiques (commune {commune_id})")
        
        try:
            # D'abord récupérer le cd_refnis de la commune
            query_refnis = """
                SELECT cd_refnis
                FROM dw.dim_geography
                WHERE id_geography = :commune_id AND fl_current = TRUE
            """
            
            params = {'commune_id': commune_id}
            
            refnis_result = self.execute_query(query_refnis, params)
            
            if not refnis_result or len(refnis_result) == 0:
                logger.warning(f"Impossible de trouver le code refnis pour la commune {commune_id}")
                return {}
                
            refnis = refnis_result[0]['cd_refnis']
            
            # Maintenant, récupérer les secteurs statistiques
            query = """
                SELECT 
                    ss.id_sector_sk,
                    ss.cd_sector,
                    ss.tx_sector_fr AS sector_name,
                    ss.tx_sector_nl AS sector_name_nl,
                    ss.ms_area_ha / 100 AS area_km2
                FROM 
                    dw.dim_statistical_sectors ss
                WHERE 
                    ss.cd_refnis = :refnis
                    AND ss.dt_end IS NULL
                ORDER BY
                    ss.tx_sector_fr
            """
            
            params = {'refnis': refnis}
            
            result = self.execute_query(query, params)
            
            sectors_data = {}
            for row in result:
                sector_id = row['id_sector_sk']
                sectors_data[sector_id] = {
                    'sector_id': sector_id,
                    'cd_sector': row['cd_sector'],
                    'sector_name': row['sector_name'],
                    'sector_name_nl': row['sector_name_nl'],
                    'area_km2': row['area_km2']
                }
                
            self.log_extraction_end(f"secteurs statistiques (commune {commune_id})", len(sectors_data))
            
            return sectors_data
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des secteurs statistiques: {str(e)}")
            return {}
            
    def extract_data(self, commune_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Extrait toutes les données géographiques pour une commune ou toutes les communes.
        
        Args:
            commune_id (str, optional): Identifiant de la commune. Si None, utilise la valeur définie dans l'instance.
            
        Returns:
            dict: Données extraites pour la géographie.
        """
        commune_id = commune_id or self.commune_id
        
        # Si commune_id est spécifié, extraire les données pour cette commune
        if commune_id:
            commune_info = self.extract_commune_info(commune_id)
            statistical_sectors = self.extract_statistical_sectors(commune_id)
            
            return {
                "commune_info": commune_info,
                "statistical_sectors": statistical_sectors
            }
        
        # Sinon, extraire les données pour toutes les communes de la province
        else:
            with self.get_db_session() as session:
                communes = self.get_communes(session)
                
            result = {}
            for commune in communes:
                commune_id = commune['commune_id']
                
                commune_info = self.extract_commune_info(commune_id)
                statistical_sectors = self.extract_statistical_sectors(commune_id)
                
                result[commune_id] = {
                    "commune_info": commune_info,
                    "statistical_sectors": statistical_sectors
                }
            
            return result