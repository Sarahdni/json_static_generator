"""
Extracteur pour les données démographiques.
Extrait les données des tables fact_population_structure et fact_household_cohabitation.
"""
import logging
from typing import Dict, List, Any, Optional

from src.extractors.base import BaseExtractor
from src.config.settings import DEFAULT_PERIOD

logger = logging.getLogger(__name__)

class DemographicsExtractor(BaseExtractor):
    """Extracteur pour les données démographiques."""
    
    def __init__(self, commune_id=None, province=None, period=None):
        """
        Initialise l'extracteur de données démographiques.
        
        Args:
            commune_id (str, optional): Identifiant de la commune.
            province (str, optional): Province à extraire.
            period (dict, optional): Périodes d'extraction pour différents types de données.
        """
        super().__init__(commune_id, province, period)
        self.data_period = self.period.get('demographic_data', DEFAULT_PERIOD['demographic_data'])
        
    def extract_population_structure(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données de structure de la population pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_population_structure.
        """
        self.log_extraction_start(f"structure de la population (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'year')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
            
            # Récupération des données actuelles
            current_data = self.extract_population_data_for_period(commune_id, date_id)
            
            # Récupération des données de l'année précédente pour les comparaisons
            previous_year = str(int(self.data_period) - 1)
            previous_year_date_id = self.get_date_id(session, previous_year, 'year')
            previous_year_data = {}
            if previous_year_date_id:
                previous_year_data = self.extract_population_data_for_period(commune_id, previous_year_date_id)
            
            # Récupération des données d'il y a 5 ans pour les comparaisons à long terme
            five_year_ago = str(int(self.data_period) - 5)
            five_year_date_id = self.get_date_id(session, five_year_ago, 'year')
            five_year_data = {}
            if five_year_date_id:
                five_year_data = self.extract_population_data_for_period(commune_id, five_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "previous_year_data": previous_year_data,
            "five_year_data": five_year_data
        }
        
        self.log_extraction_end(f"structure de la population (commune {commune_id})", 
                              len(current_data))
        
        return result
    
    def extract_population_data_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données de population pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        # Requête pour obtenir les totaux par sexe et âge
        query = """
            SELECT 
                ps.id_age,
                age.cd_age_group AS age_group,
                ag.cd_age_min AS min_age,
                ag.cd_age_max AS max_age,
                ps.cd_sex,
                sex.tx_sex_fr AS sex_description,
                SUM(ps.ms_population) AS total_population
            FROM 
                dw.fact_population_structure ps
            JOIN 
                dw.dim_age age ON ps.id_age = age.cd_age
            JOIN
                dw.dim_age_group ag ON age.cd_age_group = ag.cd_age_group
            JOIN
                dw.dim_sex sex ON ps.cd_sex = sex.cd_sex
            WHERE 
                ps.id_geography = :commune_id
                AND ps.id_date = :date_id
                AND ps.fl_current = TRUE
            GROUP BY
                ps.id_age, age.cd_age_group, ag.cd_age_min, ag.cd_age_max, ps.cd_sex, sex.tx_sex_fr
            ORDER BY
                ag.cd_age_min, ps.cd_sex
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            # Requête pour obtenir le total général de population
            total_query = """
                SELECT 
                    SUM(ps.ms_population) AS total_population
                FROM 
                    dw.fact_population_structure ps
                WHERE 
                    ps.id_geography = :commune_id
                    AND ps.id_date = :date_id
                    AND ps.fl_current = TRUE
            """
            
            total_result = self.execute_query(total_query, params)
            total_population = total_result[0]['total_population'] if total_result else 0
            
            # Requête pour obtenir les totaux par nationalité
            nationality_query = """
                SELECT 
                    ps.cd_nationality,
                    nat.tx_nationality_fr AS nationality_description,
                    nat.cd_country_group AS nationality_group,
                    SUM(ps.ms_population) AS total_population
                FROM 
                    dw.fact_population_structure ps
                JOIN 
                    dw.dim_nationality nat ON ps.cd_nationality = nat.cd_nationality
                WHERE 
                    ps.id_geography = :commune_id
                    AND ps.id_date = :date_id
                    AND ps.fl_current = TRUE
                GROUP BY
                    ps.cd_nationality, nat.tx_nationality_fr, nat.cd_country_group
                ORDER BY
                    nat.cd_country_group, SUM(ps.ms_population) DESC
            """
            
            nationality_result = self.execute_query(nationality_query, params)
            
            # Organisation des données par groupe d'âge et sexe
            age_groups = {}
            for row in result:
                age_group = row['id_age']
                if age_group not in age_groups:
                    age_groups[age_group] = {
                        'description': row['age_group'],
                        'min_age': row['min_age'],
                        'max_age': row['max_age'],
                        'sexes': {}
                    }
                
                sex = row['cd_sex']
                age_groups[age_group]['sexes'][sex] = {
                    'description': row['sex_description'],
                    'population': row['total_population']
                }
            
            # Organisation des données par nationalité
            nationalities = {}
            for row in nationality_result:
                nationality = row['cd_nationality']
                nationalities[nationality] = {
                    'description': row['nationality_description'],
                    'group': row['nationality_group'],
                    'population': row['total_population']
                }
            
            return {
                'total_population': total_population,
                'age_groups': age_groups,
                'nationalities': nationalities
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données de population: {str(e)}")
            return {}
    
    def extract_household_composition(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données de composition des ménages pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_household_cohabitation.
        """
        self.log_extraction_start(f"composition des ménages (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'year')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
        
        query = """
            SELECT 
            hc.cd_cohabitation,
            cs.cd_cohabitation AS cohabitation_description,  -- Changé de cs.tx_cohabitation_fr
            hc.cd_age_group,
            hc.cd_sex,
            sex.tx_sex_fr AS sex_description,
            hc.cd_nationality,
            nat.tx_nationality_fr AS nationality_description,
            SUM(hc.ms_count) AS total_count
        FROM 
            dw.fact_household_cohabitation hc
        JOIN 
            dw.dim_cohabitation_status cs ON hc.cd_cohabitation = cs.cd_cohabitation
        JOIN
            dw.dim_sex sex ON hc.cd_sex = sex.cd_sex
        JOIN
            dw.dim_nationality nat ON hc.cd_nationality = nat.cd_nationality
        WHERE 
            hc.id_geography = :commune_id
            AND hc.id_date = :date_id
        GROUP BY
            hc.cd_cohabitation, cs.cd_cohabitation, 
            hc.cd_age_group, hc.cd_sex, sex.tx_sex_fr,
            hc.cd_nationality, nat.tx_nationality_fr
        ORDER BY
            cs.cd_cohabitation, hc.cd_age_group, hc.cd_sex
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            # Requête pour obtenir le total des ménages
            total_query = """
                SELECT 
                    COUNT(DISTINCT hc.cd_cohabitation) AS total_household_types,
                    SUM(hc.ms_count) AS total_individuals_in_households
                FROM 
                    dw.fact_household_cohabitation hc
                WHERE 
                    hc.id_geography = :commune_id
                    AND hc.id_date = :date_id
            """
            
            total_result = self.execute_query(total_query, params)
            total_households = total_result[0]['total_household_types'] if total_result else 0
            total_individuals = total_result[0]['total_individuals_in_households'] if total_result else 0
            
            # Organisation des données par type de cohabitation
            cohabitation_types = {}
            for row in result:
                cohabitation = row['cd_cohabitation']
                if cohabitation not in cohabitation_types:
                    cohabitation_types[cohabitation] = {
                        'description': row['cohabitation_description'],
                        'total_count': 0,
                        'age_groups': {},
                        'sexes': {},
                        'nationalities': {}
                    }
                
                # Incrémenter le total pour ce type de cohabitation
                cohabitation_types[cohabitation]['total_count'] += row['total_count']
                
                # Ajouter les détails par groupe d'âge
                age_group = row['cd_age_group']
                if age_group not in cohabitation_types[cohabitation]['age_groups']:
                    cohabitation_types[cohabitation]['age_groups'][age_group] = 0
                cohabitation_types[cohabitation]['age_groups'][age_group] += row['total_count']
                
                # Ajouter les détails par sexe
                sex = row['cd_sex']
                if sex not in cohabitation_types[cohabitation]['sexes']:
                    cohabitation_types[cohabitation]['sexes'][sex] = {
                        'description': row['sex_description'],
                        'count': 0
                    }
                cohabitation_types[cohabitation]['sexes'][sex]['count'] += row['total_count']
                
                # Ajouter les détails par nationalité
                nationality = row['cd_nationality']
                if nationality not in cohabitation_types[cohabitation]['nationalities']:
                    cohabitation_types[cohabitation]['nationalities'][nationality] = {
                        'description': row['nationality_description'],
                        'count': 0
                    }
                cohabitation_types[cohabitation]['nationalities'][nationality]['count'] += row['total_count']
            
            self.log_extraction_end(f"composition des ménages (commune {commune_id})", len(result))
            
            return {
                'total_household_types': total_households,
                'total_individuals': total_individuals,
                'cohabitation_types': cohabitation_types
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données de composition des ménages: {str(e)}")
            return {}
    
    def extract_household_vehicles(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données sur les véhicules des ménages pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_household_vehicles.
        """
        self.log_extraction_start(f"véhicules des ménages (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'year')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
        
        # Requête pour obtenir les données par secteur
        query = """
            SELECT 
                hv.id_sector_sk,
                ss.tx_sector_fr AS sector_name,
                hv.ms_households,
                hv.ms_vehicles,
                hv.rt_vehicles_per_household
            FROM 
                dw.fact_household_vehicles hv
            JOIN 
                dw.dim_statistical_sectors ss ON hv.id_sector_sk = ss.id_sector_sk
            WHERE 
                hv.id_geography = :commune_id
                AND hv.id_date = :date_id
                AND hv.fl_current = TRUE
            ORDER BY
                ss.tx_sector_fr
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            # Requête pour obtenir les totaux au niveau de la commune
            commune_query = """
                SELECT 
                    SUM(hv.ms_households) AS total_households,
                    SUM(hv.ms_vehicles) AS total_vehicles,
                    SUM(hv.ms_vehicles) / NULLIF(SUM(hv.ms_households), 0) AS avg_vehicles_per_household
                FROM 
                    dw.fact_household_vehicles hv
                WHERE 
                    hv.id_geography = :commune_id
                    AND hv.id_date = :date_id
                    AND hv.fl_current = TRUE
            """
            
            commune_result = self.execute_query(commune_query, params)
            
            # Organisation des données par secteur
            sectors_data = {}
            for row in result:
                sector_id = row['id_sector_sk']
                sectors_data[sector_id] = {
                    'sector_name': row['sector_name'],
                    'households': row['ms_households'],
                    'vehicles': row['ms_vehicles'],
                    'vehicles_per_household': row['rt_vehicles_per_household']
                }
            
            # Extraction des totaux de la commune
            commune_totals = {}
            if commune_result and len(commune_result) > 0:
                commune_totals = {
                    'total_households': commune_result[0]['total_households'],
                    'total_vehicles': commune_result[0]['total_vehicles'],
                    'avg_vehicles_per_household': commune_result[0]['avg_vehicles_per_household']
                }
            
            self.log_extraction_end(f"véhicules des ménages (commune {commune_id})", len(result))
            
            return {
                'commune_totals': commune_totals,
                'sectors': sectors_data
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données de véhicules des ménages: {str(e)}")
            return {}
    
    def extract_data(self, commune_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Extrait toutes les données démographiques pour une commune ou toutes les communes.
        
        Args:
            commune_id (str, optional): Identifiant de la commune. Si None, utilise la valeur définie dans l'instance.
            
        Returns:
            dict: Données extraites pour la démographie.
        """
        commune_id = commune_id or self.commune_id
        
        # Si commune_id est spécifié, extraire les données pour cette commune
        if commune_id:
            return {
                "population_structure": self.extract_population_structure(commune_id),
                "household_composition": self.extract_household_composition(commune_id),
                "household_vehicles": self.extract_household_vehicles(commune_id)
            }
        
        # Sinon, extraire les données pour toutes les communes de la province
        else:
            with self.get_db_session() as session:
                communes = self.get_communes(session)
                
            result = {}
            for commune in communes:
                commune_id = commune['commune_id']
                result[commune_id] = {
                    "population_structure": self.extract_population_structure(commune_id),
                    "household_composition": self.extract_household_composition(commune_id),
                    "household_vehicles": self.extract_household_vehicles(commune_id)
                }
            
            return result