"""
Extracteur pour les données économiques.
Extrait les données des tables fact_tax_income, fact_unemployment et fact_vat_nace_employment.
"""
import logging
from typing import Dict, List, Any, Optional

from src.extractors.base import BaseExtractor
from src.config.settings import DEFAULT_PERIOD

logger = logging.getLogger(__name__)

class EconomicsExtractor(BaseExtractor):
    """Extracteur pour les données économiques."""
    
    def __init__(self, commune_id=None, province=None, period=None):
        """
        Initialise l'extracteur de données économiques.
        
        Args:
            commune_id (str, optional): Identifiant de la commune.
            province (str, optional): Province à extraire.
            period (dict, optional): Périodes d'extraction pour différents types de données.
        """
        super().__init__(commune_id, province, period)
        self.data_period = self.period.get('economic_data', DEFAULT_PERIOD['economic_data'])
        self.tax_period = self.period.get('tax_data', DEFAULT_PERIOD['tax_data'])
        
    def extract_tax_income(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données fiscales et de revenus pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_tax_income.
        """
        self.log_extraction_start(f"revenus et impôts (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.tax_period, 'year')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.tax_period}")
                return {}
            
            # Récupération des données actuelles
            current_data = self.extract_tax_data_for_period(commune_id, date_id)
            
            # Récupération des données de l'année précédente pour les comparaisons
            previous_year = str(int(self.tax_period) - 1)
            previous_year_date_id = self.get_date_id(session, previous_year, 'year')
            previous_year_data = {}
            if previous_year_date_id:
                previous_year_data = self.extract_tax_data_for_period(commune_id, previous_year_date_id)
            
            # Récupération des données d'il y a 5 ans pour les comparaisons à long terme
            five_year_ago = str(int(self.tax_period) - 5)
            five_year_date_id = self.get_date_id(session, five_year_ago, 'year')
            five_year_data = {}
            if five_year_date_id:
                five_year_data = self.extract_tax_data_for_period(commune_id, five_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "previous_year_data": previous_year_data,
            "five_year_data": five_year_data
        }
        
        self.log_extraction_end(f"revenus et impôts (commune {commune_id})", 
                              len(current_data.keys()))
        
        return result
    
    def extract_tax_data_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données fiscales pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        query = """
            SELECT 
                ti.ms_nbr_non_zero_inc,
                ti.ms_nbr_zero_inc,
                ti.ms_tot_net_taxable_inc,
                ti.ms_tot_net_inc,
                ti.ms_nbr_tot_net_inc,
                ti.ms_real_estate_net_inc,
                ti.ms_nbr_real_estate_net_inc,
                ti.ms_tot_net_mov_ass_inc,
                ti.ms_nbr_net_mov_ass_inc,
                ti.ms_tot_net_various_inc,
                ti.ms_nbr_net_various_inc,
                ti.ms_tot_net_prof_inc,
                ti.ms_nbr_net_prof_inc,
                ti.ms_sep_taxable_inc,
                ti.ms_nbr_sep_taxable_inc,
                ti.ms_joint_taxable_inc,
                ti.ms_nbr_joint_taxable_inc,
                ti.ms_tot_deduct_spend,
                ti.ms_nbr_deduct_spend,
                ti.ms_tot_state_taxes,
                ti.ms_nbr_state_taxes,
                ti.ms_tot_municip_taxes,
                ti.ms_nbr_municip_taxes,
                ti.ms_tot_suburbs_taxes,
                ti.ms_nbr_suburbs_taxes,
                ti.ms_tot_taxes,
                ti.ms_nbr_tot_taxes,
                ti.ms_tot_residents,
                d.cd_year
            FROM 
                dw.fact_tax_income ti
            JOIN
                dw.dim_date d ON ti.id_date = d.id_date
            WHERE 
                ti.id_geography = :commune_id
                AND ti.id_date = :date_id
                AND ti.fl_current = TRUE
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            result = self.execute_query(query, params)
            
            if not result or len(result) == 0:
                logger.warning(f"Aucune donnée fiscale trouvée pour la commune {commune_id} et la période {date_id}")
                return {}
            
            data = result[0]  # Il devrait y avoir qu'une seule ligne par commune/période
            
            # Calcul des moyennes et ratios pertinents
            avg_net_inc = data['ms_tot_net_inc'] / data['ms_nbr_tot_net_inc'] if data['ms_nbr_tot_net_inc'] else 0
            avg_net_taxable_inc = data['ms_tot_net_taxable_inc'] / data['ms_nbr_non_zero_inc'] if data['ms_nbr_non_zero_inc'] else 0
            avg_tax_burden = data['ms_tot_taxes'] / data['ms_tot_net_taxable_inc'] * 100 if data['ms_tot_net_taxable_inc'] else 0
            avg_inc_per_resident = data['ms_tot_net_inc'] / data['ms_tot_residents'] if data['ms_tot_residents'] else 0
            
            # Répartition des sources de revenus
            income_sources = {}
            total_income = data['ms_tot_net_inc'] or 1  # Éviter division par zéro
            
            if data['ms_tot_net_prof_inc']:
                income_sources['professional'] = {
                    'amount': data['ms_tot_net_prof_inc'],
                    'percentage': (data['ms_tot_net_prof_inc'] / total_income) * 100,
                    'declarations_count': data['ms_nbr_net_prof_inc']
                }
            
            if data['ms_real_estate_net_inc']:
                income_sources['real_estate'] = {
                    'amount': data['ms_real_estate_net_inc'],
                    'percentage': (data['ms_real_estate_net_inc'] / total_income) * 100,
                    'declarations_count': data['ms_nbr_real_estate_net_inc']
                }
            
            if data['ms_tot_net_mov_ass_inc']:
                income_sources['movable_assets'] = {
                    'amount': data['ms_tot_net_mov_ass_inc'],
                    'percentage': (data['ms_tot_net_mov_ass_inc'] / total_income) * 100,
                    'declarations_count': data['ms_nbr_net_mov_ass_inc']
                }
            
            if data['ms_tot_net_various_inc']:
                income_sources['various'] = {
                    'amount': data['ms_tot_net_various_inc'],
                    'percentage': (data['ms_tot_net_various_inc'] / total_income) * 100,
                    'declarations_count': data['ms_nbr_net_various_inc']
                }
            
            # Répartition des types de taxes
            tax_types = {}
            total_taxes = data['ms_tot_taxes'] or 1  # Éviter division par zéro
            
            if data['ms_tot_state_taxes']:
                tax_types['state'] = {
                    'amount': data['ms_tot_state_taxes'],
                    'percentage': (data['ms_tot_state_taxes'] / total_taxes) * 100,
                    'declarations_count': data['ms_nbr_state_taxes']
                }
            
            if data['ms_tot_municip_taxes']:
                tax_types['municipal'] = {
                    'amount': data['ms_tot_municip_taxes'],
                    'percentage': (data['ms_tot_municip_taxes'] / total_taxes) * 100,
                    'declarations_count': data['ms_nbr_municip_taxes']
                }
            
            if data['ms_tot_suburbs_taxes']:
                tax_types['suburbs'] = {
                    'amount': data['ms_tot_suburbs_taxes'],
                    'percentage': (data['ms_tot_suburbs_taxes'] / total_taxes) * 100,
                    'declarations_count': data['ms_nbr_suburbs_taxes']
                }
            
            return {
                'year': data['cd_year'],
                'total_declarations': data['ms_nbr_tot_taxes'],
                'declarations_with_income': data['ms_nbr_non_zero_inc'],
                'declarations_without_income': data['ms_nbr_zero_inc'],
                'total_population': data['ms_tot_residents'],
                'total_net_income': data['ms_tot_net_inc'],
                'total_taxable_income': data['ms_tot_net_taxable_inc'],
                'total_taxes': data['ms_tot_taxes'],
                'average_net_income': avg_net_inc,
                'average_taxable_income': avg_net_taxable_inc,
                'average_income_per_resident': avg_inc_per_resident,
                'average_tax_burden_percentage': avg_tax_burden,
                'income_sources': income_sources,
                'tax_types': tax_types
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données fiscales: {str(e)}")
            return {}
    
    def extract_unemployment(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données de chômage pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_unemployment.
        """
        self.log_extraction_start(f"chômage (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'year')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
            
            # Récupération des données actuelles
            current_data = self.extract_unemployment_data_for_period(commune_id, date_id)
            
            # Récupération des données de l'année précédente pour les comparaisons
            previous_year = str(int(self.data_period) - 1)
            previous_year_date_id = self.get_date_id(session, previous_year, 'year')
            previous_year_data = {}
            if previous_year_date_id:
                previous_year_data = self.extract_unemployment_data_for_period(commune_id, previous_year_date_id)
            
            # Récupération des données d'il y a 3 ans pour les comparaisons à moyen terme
            three_year_ago = str(int(self.data_period) - 3)
            three_year_date_id = self.get_date_id(session, three_year_ago, 'year')
            three_year_data = {}
            if three_year_date_id:
                three_year_data = self.extract_unemployment_data_for_period(commune_id, three_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "previous_year_data": previous_year_data,
            "three_year_data": three_year_data
        }
        
        self.log_extraction_end(f"chômage (commune {commune_id})", 
                              len(current_data.keys()))
        
        return result
    
    def extract_unemployment_data_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données de chômage pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        # Requête pour obtenir le taux de chômage global
        total_query = """
            SELECT 
                u.ms_unemployment_rate,
                d.cd_year
            FROM 
                dw.fact_unemployment u
            JOIN
                dw.dim_date d ON u.id_date = d.id_date
            WHERE 
                u.id_geography = :commune_id
                AND u.id_date = :date_id
                AND u.fl_total_sex = TRUE
                AND u.fl_total_age = TRUE
                AND u.fl_total_education = TRUE
                AND u.fl_valid = TRUE
                AND u.cd_unemp_type = 'TOTAL'
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            total_result = self.execute_query(total_query, params)
            
            if not total_result or len(total_result) == 0:
                logger.warning(f"Aucune donnée de chômage global trouvée pour la commune {commune_id} et la période {date_id}")
                return {}
            
            total_data = total_result[0]
            overall_rate = total_data['ms_unemployment_rate'] * 100 if total_data['ms_unemployment_rate'] else 0
            year = total_data['cd_year']
            
            # Requête pour obtenir le taux de chômage par sexe
            sex_query = """
                SELECT 
                    u.cd_sex,
                    s.tx_sex_fr AS sex_description,
                    u.ms_unemployment_rate
                FROM 
                    dw.fact_unemployment u
                JOIN
                    dw.dim_sex s ON u.cd_sex = s.cd_sex
                WHERE 
                    u.id_geography = :commune_id
                    AND u.id_date = :date_id
                    AND u.fl_total_age = TRUE
                    AND u.fl_total_education = TRUE
                    AND u.fl_valid = TRUE
                    AND u.cd_unemp_type = 'TOTAL'
                    AND u.fl_total_sex = FALSE
            """
            
            sex_result = self.execute_query(sex_query, params)
            
            # Requête pour obtenir le taux de chômage par groupe d'âge
            age_query = """
                SELECT 
                    u.cd_age_group,
                    ag.tx_age_group_fr AS age_group_description,
                    u.ms_unemployment_rate
                FROM 
                    dw.fact_unemployment u
                JOIN
                    dw.dim_age_group ag ON u.cd_age_group = ag.cd_age_group
                WHERE 
                    u.id_geography = :commune_id
                    AND u.id_date = :date_id
                    AND u.fl_total_sex = TRUE
                    AND u.fl_total_education = TRUE
                    AND u.fl_valid = TRUE
                    AND u.cd_unemp_type = 'TOTAL'
                    AND u.fl_total_age = FALSE
                ORDER BY
                    ag.nb_min_age
            """
            
            age_result = self.execute_query(age_query, params)
            
            # Requête pour obtenir le taux de chômage par niveau d'éducation
            education_query = """
                SELECT 
                    u.cd_education_level,
                    e.tx_education_level_fr AS education_description,
                    u.ms_unemployment_rate
                FROM 
                    dw.fact_unemployment u
                JOIN
                    dw.dim_education_level e ON u.cd_education_level = e.cd_education_level
                WHERE 
                    u.id_geography = :commune_id
                    AND u.id_date = :date_id
                    AND u.fl_total_sex = TRUE
                    AND u.fl_total_age = TRUE
                    AND u.fl_valid = TRUE
                    AND u.cd_unemp_type = 'TOTAL'
                    AND u.fl_total_education = FALSE
                ORDER BY
                    u.ms_unemployment_rate DESC
            """
            
            education_result = self.execute_query(education_query, params)
            
            # Organisation des données par sexe
            by_sex = {}
            for row in sex_result:
                sex = row['cd_sex']
                by_sex[sex] = {
                    'description': row['sex_description'],
                    'rate': row['ms_unemployment_rate'] * 100 if row['ms_unemployment_rate'] else 0
                }
            
            # Organisation des données par groupe d'âge
            by_age_group = {}
            for row in age_result:
                age_group = row['cd_age_group']
                by_age_group[age_group] = {
                    'description': row['age_group_description'],
                    'rate': row['ms_unemployment_rate'] * 100 if row['ms_unemployment_rate'] else 0
                }
            
            # Organisation des données par niveau d'éducation
            by_education = {}
            for row in education_result:
                education = row['cd_education_level']
                by_education[education] = {
                    'description': row['education_description'],
                    'rate': row['ms_unemployment_rate'] * 100 if row['ms_unemployment_rate'] else 0
                }
            
            return {
                'year': year,
                'overall_rate': overall_rate,
                'by_sex': by_sex,
                'by_age_group': by_age_group,
                'by_education': by_education
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données de chômage: {str(e)}")
            return {}
    
    def extract_business_activity(self, commune_id: str) -> Dict[str, Any]:
        """
        Extrait les données d'activité économique des entreprises pour une commune.
        
        Args:
            commune_id (str): Identifiant de la commune.
            
        Returns:
            dict: Données extraites de fact_vat_nace_employment.
        """
        self.log_extraction_start(f"activité économique (commune {commune_id})")
        
        # Obtenir l'ID de date pour la période spécifiée
        with self.get_db_session() as session:
            date_id = self.get_date_id(session, self.data_period, 'year')
            if not date_id:
                logger.warning(f"Aucune date trouvée pour la période {self.data_period}")
                return {}
            
            # Récupération des données actuelles
            current_data = self.extract_business_data_for_period(commune_id, date_id)
            
            # Récupération des données de l'année précédente pour les comparaisons
            previous_year = str(int(self.data_period) - 1)
            previous_year_date_id = self.get_date_id(session, previous_year, 'year')
            previous_year_data = {}
            if previous_year_date_id:
                previous_year_data = self.extract_business_data_for_period(commune_id, previous_year_date_id)
        
        # Construction du résultat avec les données actuelles et historiques
        result = {
            "current_data": current_data,
            "previous_year_data": previous_year_data
        }
        
        self.log_extraction_end(f"activité économique (commune {commune_id})", 
                              len(current_data.keys()))
        
        return result
    
    def extract_business_data_for_period(self, commune_id: str, date_id: int) -> Dict[str, Any]:
        """
        Extrait les données d'activité économique pour une période spécifique.
        
        Args:
            commune_id (str): Identifiant de la commune.
            date_id (int): Identifiant de la date/période.
            
        Returns:
            dict: Données extraites pour cette période.
        """
        # Requête pour obtenir les totaux d'entreprises
        total_query = """
            SELECT 
                SUM(nace.ms_num_entreprises) AS total_enterprises,
                SUM(nace.ms_num_starts) AS total_starts,
                SUM(nace.ms_num_stops) AS total_stops,
                SUM(nace.ms_net_creation) AS total_net_creation,
                COUNT(DISTINCT nace.cd_economic_activity) AS unique_nace_codes,
                d.cd_year
            FROM 
                dw.fact_vat_nace_employment nace
            JOIN
                dw.dim_date d ON nace.id_date = d.id_date
            WHERE 
                nace.id_geography = :commune_id
                AND nace.id_date = :date_id
                AND nace.cd_nace_level = 1
            GROUP BY
                d.cd_year
        """
        
        params = {
            'commune_id': commune_id,
            'date_id': date_id
        }
        
        try:
            total_result = self.execute_query(total_query, params)
            
            if not total_result or len(total_result) == 0:
                logger.warning(f"Aucune donnée d'activité économique trouvée pour la commune {commune_id} et la période {date_id}")
                return {}
            
            total_data = total_result[0]
            
            # Requête pour obtenir les données par secteur d'activité (niveau NACE 1)
            sectors_query = """
                SELECT 
                    nace.cd_economic_activity,
                    ea.tx_economic_activity_fr AS activity_description,
                    SUM(nace.ms_num_entreprises) AS sector_enterprises,
                    SUM(nace.ms_num_starts) AS sector_starts,
                    SUM(nace.ms_num_stops) AS sector_stops,
                    SUM(nace.ms_net_creation) AS sector_net_creation,
                    nace.cd_year
                FROM 
                    dw.fact_vat_nace_employment nace
                JOIN
                    dw.dim_economic_activity ea ON nace.cd_economic_activity = ea.cd_economic_activity
                WHERE 
                    nace.id_geography = :commune_id
                    AND nace.id_date = :date_id
                    AND nace.cd_nace_level = 1
                GROUP BY
                    nace.cd_economic_activity, ea.tx_economic_activity_fr, nace.cd_year
                ORDER BY
                    SUM(nace.ms_num_entreprises) DESC
            """
            
            sectors_result = self.execute_query(sectors_query, params)
            
            # Requête pour obtenir les données par taille d'entreprise
            size_query = """
                SELECT 
                    nace.cd_size_class,
                    es.tx_size_class_fr AS size_description,
                    SUM(nace.ms_num_entreprises) AS size_enterprises,
                    es.nb_min_employees,
                    es.nb_max_employees
                FROM 
                    dw.fact_vat_nace_employment nace
                JOIN
                    dw.dim_entreprise_size_employees es ON nace.cd_size_class = es.cd_size_class
                WHERE 
                    nace.id_geography = :commune_id
                    AND nace.id_date = :date_id
                GROUP BY
                    nace.cd_size_class, es.tx_size_class_fr, es.nb_min_employees, es.nb_max_employees
                ORDER BY
                    es.nb_min_employees
            """
            
            size_result = self.execute_query(size_query, params)
            
            # Requête pour obtenir les données sur les entreprises étrangères
            foreign_query = """
                SELECT 
                    SUM(nace.ms_num_entreprises) AS foreign_enterprises,
                    SUM(nace.ms_num_starts) AS foreign_starts,
                    SUM(nace.ms_num_stops) AS foreign_stops
                FROM 
                    dw.fact_vat_nace_employment nace
                WHERE 
                    nace.id_geography = :commune_id
                    AND nace.id_date = :date_id
                    AND nace.fl_foreign = TRUE
            """
            
            foreign_result = self.execute_query(foreign_query, params)
            
            # Organisation des données générales
            general_data = {
                'year': total_data['cd_year'],
                'total_enterprises': total_data['total_enterprises'],
                'total_starts': total_data['total_starts'],
                'total_stops': total_data['total_stops'],
                'net_creation': total_data['total_net_creation'],
                'creation_rate': (total_data['total_starts'] / total_data['total_enterprises']) * 100 if total_data['total_enterprises'] else 0,
                'closure_rate': (total_data['total_stops'] / total_data['total_enterprises']) * 100 if total_data['total_enterprises'] else 0
            }
            
            # Organisation des données par secteur
            sectors_data = {}
            for row in sectors_result:
                sector = row['cd_economic_activity']
                total_enterprises = total_data['total_enterprises'] or 1  # Éviter division par zéro
                
                sectors_data[sector] = {
                    'description': row['activity_description'],
                    'enterprises': row['sector_enterprises'],
                    'percentage': (row['sector_enterprises'] / total_enterprises) * 100,
                    'starts': row['sector_starts'],
                    'stops': row['sector_stops'],
                    'net_creation': row['sector_net_creation'],
                    'year': row['cd_year']
                }
            
            # Organisation des données par taille
            size_data = {}
            for row in size_result:
                size = row['cd_size_class']
                total_enterprises = total_data['total_enterprises'] or 1  # Éviter division par zéro
                
                size_data[size] = {
                    'description': row['size_description'],
                    'min_employees': row['nb_min_employees'],
                    'max_employees': row['nb_max_employees'],
                    'enterprises': row['size_enterprises'],
                    'percentage': (row['size_enterprises'] / total_enterprises) * 100
                }
            
            # Organisation des données sur les entreprises étrangères
            foreign_data = {}
            if foreign_result and len(foreign_result) > 0:
                foreign = foreign_result[0]
                total_enterprises = total_data['total_enterprises'] or 1  # Éviter division par zéro
                
                foreign_data = {
                    'enterprises': foreign['foreign_enterprises'],
                    'percentage': (foreign['foreign_enterprises'] / total_enterprises) * 100 if foreign['foreign_enterprises'] else 0,
                    'starts': foreign['foreign_starts'],
                    'stops': foreign['foreign_stops']
                }
            
            return {
                'general': general_data,
                'sectors': sectors_data,
                'by_size': size_data,
                'foreign': foreign_data
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données d'activité économique: {str(e)}")
            return {}
    
    def extract_data(self, commune_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Extrait toutes les données économiques pour une commune ou toutes les communes.
        
        Args:
            commune_id (str, optional): Identifiant de la commune. Si None, utilise la valeur définie dans l'instance.
            
        Returns:
            dict: Données extraites pour l'économie.
        """
        commune_id = commune_id or self.commune_id
        
        # Si commune_id est spécifié, extraire les données pour cette commune
        if commune_id:
            return {
                "tax_income": self.extract_tax_income(commune_id),
                "unemployment": self.extract_unemployment(commune_id),
                "business_activity": self.extract_business_activity(commune_id)
            }
        
        # Sinon, extraire les données pour toutes les communes de la province
        else:
            with self.get_db_session() as session:
                communes = self.get_communes(session)
                
            result = {}
            for commune in communes:
                commune_id = commune['commune_id']
                result[commune_id] = {
                    "tax_income": self.extract_tax_income(commune_id),
                    "unemployment": self.extract_unemployment(commune_id),
                    "business_activity": self.extract_business_activity(commune_id)
                }
            
            return result