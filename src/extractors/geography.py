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
            admin_hierarchy = self.find_administrative_hierarchy(commune_info['cd_parent'])
            
            # Compléter les informations avec la hiérarchie administrative
            commune_info['district'] = admin_hierarchy.get('district_name', 'Arrondissement inconnu')
            commune_info['province'] = admin_hierarchy.get('province_name', 'Province inconnue')
            commune_info['region'] = admin_hierarchy.get('region_name', 'Région inconnue')
            
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
            
    def find_administrative_hierarchy(self, parent_code: str) -> Dict[str, Any]:
        """
        Trouve l'arrondissement, la province et la région associés à une commune.
        
        Args:
            parent_code (str): Code parent de la commune (code de l'arrondissement).
            
        Returns:
            dict: Informations sur l'arrondissement, la province et la région.
        """
        if not parent_code:
            return {}
            
        try:
            # Requête pour remonter la hiérarchie complète: commune -> arrondissement -> province -> région
            query = """
                WITH district AS (
                    SELECT 
                        d.id_geography AS district_id,
                        d.tx_name_fr AS district_name,
                        d.cd_lau AS district_code,
                        d.cd_parent AS province_code
                    FROM 
                        dw.dim_geography d
                    WHERE 
                        d.cd_lau = :parent_code
                        AND d.cd_level = 3  -- niveau arrondissement
                        AND d.fl_current = TRUE
                ),
                province AS (
                    SELECT 
                        d.district_id,
                        d.district_name,
                        d.district_code,
                        p.id_geography AS province_id,
                        p.tx_name_fr AS province_name,
                        p.cd_lau AS province_code,
                        p.cd_parent AS region_code
                    FROM 
                        district d
                    JOIN 
                        dw.dim_geography p ON p.cd_lau = d.province_code
                    WHERE 
                        p.cd_level = 2  -- niveau province
                        AND p.fl_current = TRUE
                )
                SELECT 
                    p.district_id,
                    p.district_name,
                    p.district_code,
                    p.province_id,
                    p.province_name,
                    p.province_code,
                    r.id_geography AS region_id,
                    r.tx_name_fr AS region_name,
                    r.cd_lau AS region_code
                FROM 
                    province p
                LEFT JOIN 
                    dw.dim_geography r ON r.cd_lau = p.region_code
                WHERE 
                    r.cd_level = 1  -- niveau région
                    AND r.fl_current = TRUE
            """
            
            params = {'parent_code': parent_code}
            result = self.execute_query(query, params)
            
            if result and len(result) > 0:
                return {
                    'district_id': result[0]['district_id'],
                    'district_name': result[0]['district_name'],
                    'district_code': result[0]['district_code'],
                    'province_id': result[0]['province_id'],
                    'province_name': result[0]['province_name'],
                    'province_code': result[0]['province_code'],
                    'region_id': result[0]['region_id'],
                    'region_name': result[0]['region_name'],
                    'region_code': result[0]['region_code']
                }
            
            # Si on ne trouve pas la hiérarchie complète, on retourne des infos par défaut
            return {
                'district_id': None,
                'district_name': 'Arrondissement inconnu',
                'district_code': parent_code,
                'province_id': None,
                'province_name': 'Province inconnue',
                'province_code': None,
                'region_id': None,
                'region_name': 'Région inconnue',
                'region_code': None
            }
        
        except Exception as e:
            self.logger.error(f"Erreur lors de la recherche de la hiérarchie administrative: {str(e)}")
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
                    AND ss.dt_end = (SELECT MAX(dt_end) FROM dw.dim_statistical_sectors WHERE cd_refnis = :refnis)
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