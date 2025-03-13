"""
Extracteur pour les données de développement immobilier.
Extrait les données des tables fact_building_permits_counts, fact_building_permits_surface 
et fact_building_permits_volume.
"""
import logging
from typing import Dict, List, Any, Optional

from src.extractors.base import BaseExtractor
from src.config.settings import DEFAULT_PERIOD

logger = logging.getLogger(__name__)

class BuildingExtractor(BaseExtractor):
    """Extracteur pour les données de développement immobilier."""
    
    def __init__(self, commune_id=None, province=None, period=None):
        """
        Initialise l'extracteur de données de développement immobilier.
        
        Args:
            commune_id (str, optional): Identifiant de la commune.
            province (str, optional): Province à extraire.
            period (dict, optional): Périodes d'extraction pour différents types de données.
        """
        super().__init__(commune_id, province, period)
        self.data_period = self.period.get('construction_data', DEFAULT_PERIOD['construction_data'])
        
    def extract_permits_counts(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données sur le nombre de permis de construire pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_building_permits_counts.
        """
        self.log_extraction_start(f"nombre de permis de construire (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'quarter')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
            
            # Récupération des données actuelles
            current_data = self.extract_permits_counts_for_period(commune_id, date_id)
            
            # Récupération des données de l'année précédente pour les comparaisons
            previous_year_period = f"{int(self.data_period[:4]) - 1}{self.data_period[4:]}"
            previous_year_date_id = self.get_date_id(session, previous_year_period, 'quarter')
            previous_year_data = {}
            if previous_year_date_id:
                previous_year_data = self.extract_permits_counts_for_period(commune_id, previous_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "previous_year_data": previous_year_data
        }
        
        self.log_extraction_end(f"nombre de permis de construire (commune {commune_id})", 
                              len(current_data.keys()))
        
        return result
    
    def extract_permits_counts_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données sur le nombre de permis de construire pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        query = """
            SELECT 
                pc.nb_buildings,
                pc.nb_dwellings,
                pc.nb_apartments,
                pc.nb_houses,
                pc.fl_residential,
                pc.fl_new_construction,
                d.cd_quarter,
                d.cd_year
            FROM 
                dw.fact_building_permits_counts pc
            JOIN
                dw.dim_date d ON pc.id_date = d.id_date
            WHERE 
                pc.id_geography = :commune_id
                AND pc.id_date = :date_id
            ORDER BY
                pc.fl_residential, pc.fl_new_construction
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            if not result:
                logger.warning(f"Aucune donnée de permis de construire trouvée pour la commune {commune_id} et la période {date_id}")
                return {}
            
            # Organisation des données par type de permis
            permits_data = {
                'year': result[0]['cd_year'],
                'quarter': result[0]['cd_quarter'],
                'residential': {
                    'new_construction': {},
                    'renovation': {}
                },
                'non_residential': {
                    'new_construction': {},
                    'renovation': {}
                }
            }
            
            # Total des permis
            total_buildings = 0
            total_dwellings = 0
            total_apartments = 0
            total_houses = 0
            
            for row in result:
                # Déterminer la catégorie de permis
                category = 'residential' if row['fl_residential'] else 'non_residential'
                type_activity = 'new_construction' if row['fl_new_construction'] else 'renovation'
                
                # Ajouter les données à la catégorie correspondante
                permits_data[category][type_activity] = {
                    'buildings': row['nb_buildings'],
                    'dwellings': row['nb_dwellings'] if row['fl_residential'] else None,
                    'apartments': row['nb_apartments'] if row['fl_residential'] else None,
                    'houses': row['nb_houses'] if row['fl_residential'] else None
                }
                
                # Incrémenter les totaux
                total_buildings += row['nb_buildings'] or 0
                total_dwellings += row['nb_dwellings'] or 0
                total_apartments += row['nb_apartments'] or 0
                total_houses += row['nb_houses'] or 0
            
            # Ajouter les totaux
            permits_data['total'] = {
                'buildings': total_buildings,
                'dwellings': total_dwellings,
                'apartments': total_apartments,
                'houses': total_houses
            }
            
            return permits_data
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données de nombre de permis: {str(e)}")
            return {}
    
    def extract_permits_surface(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données sur la surface des permis de construire résidentiels pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_building_permits_surface.
        """
        self.log_extraction_start(f"surface des permis résidentiels (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'quarter')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
            
            # Récupération des données actuelles
            current_data = self.extract_permits_surface_for_period(commune_id, date_id)
            
            # Récupération des données de l'année précédente pour les comparaisons
            previous_year_period = f"{int(self.data_period[:4]) - 1}{self.data_period[4:]}"
            previous_year_date_id = self.get_date_id(session, previous_year_period, 'quarter')
            previous_year_data = {}
            if previous_year_date_id:
                previous_year_data = self.extract_permits_surface_for_period(commune_id, previous_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "previous_year_data": previous_year_data
        }
        
        self.log_extraction_end(f"surface des permis résidentiels (commune {commune_id})", 
                              len(current_data.keys()))
        
        return result
    
    def extract_permits_surface_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données sur la surface des permis de construire résidentiels pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        query = """
            SELECT 
                ps.nb_surface_m2,
                ps.fl_residential,
                ps.fl_new_construction,
                d.cd_quarter,
                d.cd_year
            FROM 
                dw.fact_building_permits_surface ps
            JOIN
                dw.dim_date d ON ps.id_date = d.id_date
            WHERE 
                ps.id_geography = :commune_id
                AND ps.id_date = :date_id
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            if not result or len(result) == 0:
                logger.warning(f"Aucune donnée de surface de permis trouvée pour la commune {commune_id} et la période {date_id}")
                return {}
            
            row = result[0]  # Il ne devrait y avoir qu'une seule ligne par période
            
            # Récupérer le nombre de logements pour calculer la surface moyenne
            counts_query = """
                SELECT 
                    pc.nb_dwellings
                FROM 
                    dw.fact_building_permits_counts pc
                WHERE 
                    pc.id_geography = :commune_id
                    AND pc.id_date = :date_id
                    AND pc.fl_residential = TRUE
                    AND pc.fl_new_construction = TRUE
            """
            
            counts_result = self.execute_query(counts_query, params)
            nb_dwellings = counts_result[0]['nb_dwellings'] if counts_result and len(counts_result) > 0 else 0
            
            # Calculer la surface moyenne par logement
            avg_surface_per_dwelling = row['nb_surface_m2'] / nb_dwellings if nb_dwellings and nb_dwellings > 0 else 0
            
            return {
                'year': row['cd_year'],
                'quarter': row['cd_quarter'],
                'total_surface_m2': row['nb_surface_m2'],
                'avg_surface_per_dwelling_m2': avg_surface_per_dwelling,
                'dwellings_count': nb_dwellings
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données de surface des permis: {str(e)}")
            return {}
    
    def extract_permits_volume(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données sur le volume des permis de construire non résidentiels pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_building_permits_volume.
        """
        self.log_extraction_start(f"volume des permis non résidentiels (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'quarter')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
            
            # Récupération des données actuelles
            current_data = self.extract_permits_volume_for_period(commune_id, date_id)
            
            # Récupération des données de l'année précédente pour les comparaisons
            previous_year_period = f"{int(self.data_period[:4]) - 1}{self.data_period[4:]}"
            previous_year_date_id = self.get_date_id(session, previous_year_period, 'quarter')
            previous_year_data = {}
            if previous_year_date_id:
                previous_year_data = self.extract_permits_volume_for_period(commune_id, previous_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "previous_year_data": previous_year_data
        }
        
        self.log_extraction_end(f"volume des permis non résidentiels (commune {commune_id})", 
                              len(current_data.keys()))
        
        return result
    
    def extract_permits_volume_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données sur le volume des permis de construire non résidentiels pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        query = """
            SELECT 
                pv.nb_volume_m3,
                pv.fl_residential,
                pv.fl_new_construction,
                d.cd_quarter,
                d.cd_year
            FROM 
                dw.fact_building_permits_volume pv
            JOIN
                dw.dim_date d ON pv.id_date = d.id_date
            WHERE 
                pv.id_geography = :commune_id
                AND pv.id_date = :date_id
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            if not result or len(result) == 0:
                logger.warning(f"Aucune donnée de volume de permis trouvée pour la commune {commune_id} et la période {date_id}")
                return {}
            
            row = result[0]  # Il ne devrait y avoir qu'une seule ligne par période
            
            # Récupérer le nombre de bâtiments non résidentiels pour le contexte
            counts_query = """
                SELECT 
                    pc.nb_buildings
                FROM 
                    dw.fact_building_permits_counts pc
                WHERE 
                    pc.id_geography = :commune_id
                    AND pc.id_date = :date_id
                    AND pc.fl_residential = FALSE
                    AND pc.fl_new_construction = TRUE
            """
            
            counts_result = self.execute_query(counts_query, params)
            nb_buildings = counts_result[0]['nb_buildings'] if counts_result and len(counts_result) > 0 else 0
            
            # Calculer le volume moyen par bâtiment
            avg_volume_per_building = row['nb_volume_m3'] / nb_buildings if nb_buildings and nb_buildings > 0 else 0
            
            return {
                'year': row['cd_year'],
                'quarter': row['cd_quarter'],
                'total_volume_m3': row['nb_volume_m3'],
                'avg_volume_per_building_m3': avg_volume_per_building,
                'buildings_count': nb_buildings
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données de volume des permis: {str(e)}")
            return {}
    
    def extract_data(self, commune_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Extrait toutes les données de développement immobilier pour une commune ou toutes les communes.
        
        Args:
            commune_id (str, optional): Identifiant de la commune. Si None, utilise la valeur définie dans l'instance.
            
        Returns:
            dict: Données extraites pour le développement immobilier.
        """
        commune_id = commune_id or self.commune_id
        
        # Si commune_id est spécifié, extraire les données pour cette commune
        if commune_id:
            return {
                "permits_counts": self.extract_permits_counts(commune_id),
                "permits_surface": self.extract_permits_surface(commune_id),
                "permits_volume": self.extract_permits_volume(commune_id)
            }
        
        # Sinon, extraire les données pour toutes les communes de la province
        else:
            with self.get_db_session() as session:
                communes = self.get_communes(session)
                
            result = {}
            for commune in communes:
                commune_id = commune['commune_id']
                result[commune_id] = {
                    "permits_counts": self.extract_permits_counts(commune_id),
                    "permits_surface": self.extract_permits_surface(commune_id),
                    "permits_volume": self.extract_permits_volume(commune_id)
                }
            
            return result