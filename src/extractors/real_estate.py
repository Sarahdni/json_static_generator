"""
Extracteur pour les données du marché immobilier.
Extrait les données des tables fact_real_estate_municipality et fact_real_estate_sector.
"""
import logging
from typing import Dict, List, Any, Optional

from src.extractors.base import BaseExtractor
from src.config.settings import DEFAULT_PERIOD

logger = logging.getLogger(__name__)

class RealEstateExtractor(BaseExtractor):
    """Extracteur pour les données du marché immobilier."""
    
    def __init__(self, commune_id=None, province=None, period=None):
        """
        Initialise l'extracteur du marché immobilier.
        
        Args:
            commune_id (str, optional): Identifiant de la commune.
            province (str, optional): Province à extraire.
            period (dict, optional): Périodes d'extraction pour différents types de données.
        """
        super().__init__(commune_id, province, period)
        self.data_period = self.period.get('real_estate_data', DEFAULT_PERIOD['real_estate_data'])
        
    def extract_municipality_data(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données immobilières au niveau municipal pour une commune spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_real_estate_municipality.
        """
        self.log_extraction_start(f"marché immobilier municipal (commune {commune_id})")
        
        with self.get_db_session() as session:
            # Déterminer la période la plus récente disponible pour cette commune
            latest_period_query = """
                SELECT 
                    d.id_date,
                    d.cd_year,
                    d.cd_quarter
                FROM 
                    dw.fact_real_estate_municipality rem
                JOIN 
                    dw.dim_date d ON rem.id_date = d.id_date
                WHERE 
                    rem.id_geography = :commune_id
                    AND rem.fl_confidential = FALSE
                ORDER BY 
                    d.cd_year DESC, 
                    CASE WHEN d.cd_quarter IS NULL THEN 5 ELSE d.cd_quarter END DESC
                LIMIT 1
            """
            
            latest_params = {'commune_id': commune_id}
            latest_result = self.execute_query(latest_period_query, latest_params)
            
            if not latest_result or len(latest_result) == 0:
                logger.warning(f"Aucune donnée immobilière trouvée pour la commune {commune_id}")
                return {}
                
            # Utiliser la période la plus récente disponible
            date_id = latest_result[0]['id_date']
            latest_year = latest_result[0]['cd_year']
            latest_quarter = latest_result[0]['cd_quarter']
            
            self.logger.info(f"Période la plus récente trouvée: {latest_year}-Q{latest_quarter if latest_quarter else 'Année'}")
            
            # Récupérer les données actuelles
            current_data = self.extract_municipality_data_for_period(commune_id, date_id)
            
            # Déterminer la période pour l'année précédente
            prev_year_query = """
                SELECT 
                    d.id_date
                FROM 
                    dw.dim_date d
                JOIN
                    dw.fact_real_estate_municipality rem ON d.id_date = rem.id_date
                WHERE 
                    d.cd_year = :year
                    AND (d.cd_quarter = :quarter OR (d.cd_quarter IS NULL AND :quarter IS NULL))
                    AND rem.id_geography = :commune_id
                    AND rem.fl_confidential = FALSE
                ORDER BY
                    d.cd_year DESC,
                    CASE WHEN d.cd_quarter IS NULL THEN 5 ELSE d.cd_quarter END DESC
                LIMIT 1
            """
            
            prev_year_params = {
                'year': latest_year - 1,
                'quarter': latest_quarter,
                'commune_id': commune_id
            }
            
            prev_year_result = self.execute_query(prev_year_query, prev_year_params)
            previous_year_data = {}
            if prev_year_result and len(prev_year_result) > 0:
                previous_year_date_id = prev_year_result[0]['id_date']
                previous_year_data = self.extract_municipality_data_for_period(commune_id, previous_year_date_id)
            
            # Déterminer la période pour 5 ans en arrière
            five_year_query = """
                SELECT 
                    d.id_date
                FROM 
                    dw.dim_date d
                JOIN
                    dw.fact_real_estate_municipality rem ON d.id_date = rem.id_date
                WHERE 
                    d.cd_year = :year
                    AND (d.cd_quarter = :quarter OR (d.cd_quarter IS NULL AND :quarter IS NULL))
                    AND rem.id_geography = :commune_id
                    AND rem.fl_confidential = FALSE
                ORDER BY
                    d.cd_year DESC,
                    CASE WHEN d.cd_quarter IS NULL THEN 5 ELSE d.cd_quarter END DESC
                LIMIT 1
            """
            
            five_year_params = {
                'year': latest_year - 5,
                'quarter': latest_quarter,
                'commune_id': commune_id
            }
            
            five_year_result = self.execute_query(five_year_query, five_year_params)
            five_year_data = {}
            if five_year_result and len(five_year_result) > 0:
                five_year_date_id = five_year_result[0]['id_date']
                five_year_data = self.extract_municipality_data_for_period(commune_id, five_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "previous_year_data": previous_year_data,
            "five_year_data": five_year_data,
            "latest_period": {
                "year": latest_year,
                "quarter": latest_quarter
            }
        }
        
        self.log_extraction_end(f"marché immobilier municipal (commune {commune_id})", 
                                len(current_data) + len(previous_year_data) + len(five_year_data))
        
        return result
    
    def extract_municipality_data_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données immobilières municipales pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        query = """
            SELECT 
                rem.cd_building_type,
                bt.tx_building_type_fr AS building_type_description,
                rem.ms_total_transactions,
                rem.ms_total_price,
                rem.ms_total_surface,
                rem.ms_mean_price,
                rem.ms_price_p10,
                rem.ms_price_p25,
                rem.ms_price_p50,
                rem.ms_price_p75,
                rem.ms_price_p90,
                rem.fl_confidential
            FROM 
                dw.fact_real_estate_municipality rem
            JOIN 
                dw.dim_building_type bt ON rem.cd_building_type = bt.cd_building_type
            WHERE 
                rem.id_geography = :commune_id
                AND rem.id_date = :date_id
                AND rem.fl_confidential = FALSE
            ORDER BY
                bt.tx_building_type_fr
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            # Regroupement par type de bâtiment
            data_by_type = {}
            for row in result:
                building_type = row['cd_building_type']
                data_by_type[building_type] = row
            
            return data_by_type
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données immobilières municipales: {str(e)}")
            return {}
    
    def extract_sector_data(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données immobilières au niveau des secteurs statistiques pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_real_estate_sector.
        """
        self.log_extraction_start(f"marché immobilier par secteur (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'quarter')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
        
        query = """
            SELECT 
                res.id_sector_sk,
                ss.tx_sector_fr AS nm_sector,
                res.cd_residential_type,
                rt.tx_residential_type_fr AS residential_type_description,
                res.nb_transactions,
                res.ms_price_p10,
                res.ms_price_p25,
                res.ms_price_p50,
                res.ms_price_p75,
                res.ms_price_p90,
                res.fl_confidential,
                res.fl_aggregated_sectors,
                res.nb_aggregated_sectors
            FROM 
                dw.fact_real_estate_sector res
            JOIN 
                dw.dim_statistical_sectors ss ON res.id_sector_sk = ss.id_sector_sk
            JOIN 
                dw.dim_residential_building rt ON res.cd_residential_type = rt.cd_residential_type
            WHERE 
                res.id_geography = :commune_id
                AND res.id_date = :date_id
                AND res.fl_confidential = FALSE
            ORDER BY
                ss.tx_sector_fr, rt.tx_residential_type_fr
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            # Regroupement par secteur et type de bien
            sectors_data = {}
            for row in result:
                sector_id = row['id_sector_sk']
                if sector_id not in sectors_data:
                    sectors_data[sector_id] = {
                        'sector_name': row['nm_sector'],
                        'residential_types': {}
                    }
                
                residential_type = row['cd_residential_type']
                sectors_data[sector_id]['residential_types'][residential_type] = row
            
            self.log_extraction_end(f"marché immobilier par secteur (commune {commune_id})", len(result))
            return sectors_data
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données immobilières par secteur: {str(e)}")
            return {}
    
    def extract_building_stock(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données sur le stock de bâtiments pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_building_stock.
        """
        self.log_extraction_start(f"stock de bâtiments (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée - utilisons l'année pour le stock de bâtiments
        year_period = self.data_period[:4] if "-" in self.data_period else self.data_period
        
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, year_period, 'year')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {year_period}")
                return {}
            
            # Récupération des données actuelles
            current_data = self.extract_building_stock_for_period(commune_id, date_id)
            
            # Récupération des données d'il y a 5 ans pour les comparaisons
            five_year_ago = str(int(year_period) - 5)
            five_year_date_id = self.get_date_id(session, five_year_ago, 'year')
            five_year_data = {}
            if five_year_date_id:
                five_year_data = self.extract_building_stock_for_period(commune_id, five_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "five_year_data": five_year_data
        }
        
        self.log_extraction_end(f"stock de bâtiments (commune {commune_id})", 
                               len(current_data) + len(five_year_data))
        
        return result
    
    def extract_building_stock_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données du stock de bâtiments pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        query = """
            SELECT 
                bs.cd_building_type,
                bt.tx_building_type_fr AS building_type_description,
                bs.cd_statistic_type,
                bst.tx_statistic_type_fr AS statistic_type_description,
                bs.ms_building_count
            FROM 
                dw.fact_building_stock bs
            JOIN 
                dw.dim_building_type bt ON bs.cd_building_type = bt.cd_building_type
            JOIN 
                dw.dim_building_statistics bst ON bs.cd_statistic_type = bst.cd_statistic_type
            WHERE 
                bs.id_geography = :commune_id
                AND bs.id_date = :date_id
            ORDER BY
                bt.tx_building_type_fr, bst.tx_statistic_type_fr
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            # Regroupement par type de bâtiment et type de statistique
            data = {}
            for row in result:
                building_type = row['cd_building_type']
                statistic_type = row['cd_statistic_type']
                
                if building_type not in data:
                    data[building_type] = {
                        'description': row['building_type_description'],
                        'statistics': {}
                    }
                
                data[building_type]['statistics'][statistic_type] = {
                    'description': row['statistic_type_description'],
                    'count': row['ms_building_count']
                }
            
            return data
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données de stock de bâtiments: {str(e)}")
            return {}
    
    def extract_data(self, commune_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Extrait toutes les données immobilières pour une commune ou toutes les communes.
        
        Args:
            commune_id (str, optional): Identifiant de la commune. Si None, utilise la valeur définie dans l'instance.
            
        Returns:
            dict: Données extraites pour le marché immobilier.
        """
        commune_id = commune_id or self.commune_id
        
        # Si commune_id est spécifié, extraire les données pour cette commune
        if commune_id:
            return {
                "municipality_data": self.extract_municipality_data(commune_id),
                "sector_data": self.extract_sector_data(commune_id),
                "building_stock": self.extract_building_stock(commune_id)
            }
        
        # Sinon, extraire les données pour toutes les communes de la province
        else:
            with self.get_db_session() as session:
                communes = self.get_communes(session)
                
            result = {}
            for commune in communes:
                commune_id = commune['commune_id']
                result[commune_id] = {
                    "municipality_data": self.extract_municipality_data(commune_id),
                    "sector_data": self.extract_sector_data(commune_id),
                    "building_stock": self.extract_building_stock(commune_id)
                }
            
            return result