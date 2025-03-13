"""
Processeur pour les données économiques.
Transforme les données extraites en format JSON selon la structure définie.
"""
import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict

from src.processors.base import BaseProcessor
from src.config.settings import THRESHOLDS

logger = logging.getLogger(__name__)

class EconomicsProcessor(BaseProcessor):
    """Processeur pour les données économiques."""
    
    def __init__(self):
        """Initialise le processeur des données économiques."""
        super().__init__()
        
        # Définition des catégories de revenus
        self.income_categories = {
            "low_income": 0.25,  # 25% les plus bas
            "middle_income": 0.65,  # 65% au milieu
            "high_income": 0.10  # 10% les plus hauts
        }
        
        # Liste des secteurs économiques principaux (codes NACE niveau 1)
        self.main_sectors = {
            "A": "agriculture",
            "C": "manufacturing",
            "F": "construction",
            "G": "retail",
            "H": "transportation",
            "I": "hospitality",
            "J": "it_communication",
            "K": "finance",
            "L": "real_estate",
            "M": "professional",
            "N": "administrative",
            "O": "public_admin",
            "P": "education",
            "Q": "health_social",
            "R": "arts_entertainment",
            "S": "other_services"
        }
        
    def process_income_tax(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données fiscales et de revenus.
        
        Args:
            data: Données brutes extraites.
            
        Returns:
            dict: Section income_tax du JSON.
        """
        if not data or 'current_data' not in data:
            logger.warning("Données fiscales manquantes ou incomplètes")
            return {}
            
        current_data = data.get('current_data', {})
        previous_year_data = data.get('previous_year_data', {})
        five_year_data = data.get('five_year_data', {})
        
        # Extraire les données de base
        year = current_data.get('year')
        total_population = current_data.get('total_population', 0)
        total_net_income = current_data.get('total_net_income', 0)
        total_taxable_income = current_data.get('total_taxable_income', 0)
        avg_net_income = current_data.get('average_net_income', 0)
        avg_taxable_income = current_data.get('average_taxable_income', 0)
        avg_tax_burden = current_data.get('average_tax_burden_percentage', 0)
        
        # Répartition des revenus (approximation selon les catégories définies)
        income_distribution = {
            "low_income_pct": self.income_categories['low_income'] * 100,
            "middle_income_pct": self.income_categories['middle_income'] * 100,
            "high_income_pct": self.income_categories['high_income'] * 100
        }
        
        # Extraire les données de sources de revenus
        income_sources = current_data.get('income_sources', {})
        
        professional_income_pct = income_sources.get('professional', {}).get('percentage', 0) if 'professional' in income_sources else 0
        real_estate_income_pct = income_sources.get('real_estate', {}).get('percentage', 0) if 'real_estate' in income_sources else 0
        movable_assets_income_pct = income_sources.get('movable_assets', {}).get('percentage', 0) if 'movable_assets' in income_sources else 0
        various_income_pct = income_sources.get('various', {}).get('percentage', 0) if 'various' in income_sources else 0
        
        # Extraire les données de taux d'imposition
        tax_types = current_data.get('tax_types', {})
        municipal_tax_rate = tax_types.get('municipal', {}).get('percentage', 0) if 'municipal' in tax_types else 0
        
        # Construire l'aperçu des revenus
        income_overview = {
            "average_income": avg_net_income,
            "median_income": avg_taxable_income,  # Approximation, idéalement utiliser la vraie médiane
            "income_distribution": income_distribution
        }
        
        # Construire les données de charge fiscale
        tax_burden = {
            "average_tax_rate": avg_tax_burden,
            "municipal_tax_rate": municipal_tax_rate
        }
        
        # Construire les données de sources de revenus
        income_sources_data = {
            "professional_income_pct": professional_income_pct,
            "real_estate_income_pct": real_estate_income_pct,
            "investment_income_pct": movable_assets_income_pct,
            "other_income_pct": various_income_pct
        }
        
        return {
            "income_overview": income_overview,
            "tax_burden": tax_burden,
            "income_sources": income_sources_data
        }
        
    def process_unemployment(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données de chômage.
        
        Args:
            data: Données brutes extraites.
            
        Returns:
            dict: Section unemployment du JSON.
        """
        if not data or 'current_data' not in data:
            logger.warning("Données de chômage manquantes ou incomplètes")
            return {}
            
        current_data = data.get('current_data', {})
        previous_year_data = data.get('previous_year_data', {})
        
        # Extraire le taux de chômage global
        overall_rate = current_data.get('overall_rate', 0)
        
        # Calculer la tendance par rapport à l'année précédente
        prev_overall_rate = previous_year_data.get('overall_rate', 0) if previous_year_data else 0
        trend_yoy = None
        if prev_overall_rate:
            trend_yoy = overall_rate - prev_overall_rate
            
        # Extraire les données par groupe d'âge
        by_age_group = current_data.get('by_age_group', {})
        prev_by_age_group = previous_year_data.get('by_age_group', {}) if previous_year_data else {}
        
        age_groups_data = {}
        
        # Mapping des codes de groupes d'âge vers notre structure
        age_mapping = {
            # Codes pour les jeunes (<25 ans) - à adapter aux codes réels
            "under_25": ["15-24", "15_24", "AGE_15_24", "YOUNG"],
            # Codes pour les 25-50 ans
            "25_to_50": ["25-49", "25_49", "AGE_25_49", "ADULT"],
            # Codes pour les >50 ans
            "over_50": ["50+", "50_PLUS", "AGE_50_PLUS", "SENIOR"]
        }
        
        # Traiter les données par groupe d'âge
        for age_key, possible_codes in age_mapping.items():
            # Chercher une correspondance parmi les codes possibles
            matching_code = None
            for code in possible_codes:
                if code in by_age_group:
                    matching_code = code
                    break
                    
            if matching_code:
                rate = by_age_group[matching_code].get('rate', 0)
                
                # Calculer la tendance si des données historiques existent
                trend = None
                if matching_code in prev_by_age_group:
                    prev_rate = prev_by_age_group[matching_code].get('rate', 0)
                    trend = rate - prev_rate
                    
                age_groups_data[age_key] = {
                    "rate": rate,
                    "trend": trend
                }
            else:
                # Si aucun code correspondant n'est trouvé, estimer à partir du taux global
                # Ceci est une approximation basée sur des facteurs typiques
                if age_key == "under_25":
                    est_rate = overall_rate * 1.5  # Les jeunes ont typiquement un taux plus élevé
                elif age_key == "over_50":
                    est_rate = overall_rate * 0.9  # Les seniors ont souvent un taux légèrement inférieur
                else:
                    est_rate = overall_rate * 0.95  # Groupe d'âge moyen, légèrement inférieur à la moyenne
                    
                age_groups_data[age_key] = {
                    "rate": est_rate,
                    "trend": trend_yoy if trend_yoy else None
                }
                
        return {
            "overall_rate": overall_rate,
            "trend_yoy": trend_yoy,
            "by_age_group": age_groups_data
        }
        
    def process_business_activity(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données d'activité économique des entreprises.
        
        Args:
            data: Données brutes extraites.
            
        Returns:
            dict: Section business_activity du JSON.
        """
        if not data or 'current_data' not in data:
            logger.warning("Données d'activité économique manquantes ou incomplètes")
            return {}
            
        current_data = data.get('current_data', {})
        previous_year_data = data.get('previous_year_data', {})
        
        # Extraire les données générales
        general_data = current_data.get('general', {})
        
        total_enterprises = general_data.get('total_enterprises', 0)
        total_starts = general_data.get('total_starts', 0)
        total_stops = general_data.get('total_stops', 0)
        net_creation = general_data.get('net_creation', 0)
        creation_rate = general_data.get('creation_rate', 0)
        closure_rate = general_data.get('closure_rate', 0)
        
        # Construire l'aperçu des entreprises
        enterprise_overview = {
            "total_enterprises": total_enterprises,
            "net_creation": net_creation,
            "creation_rate": creation_rate,
            "closure_rate": closure_rate
        }
        
        # Traiter les données par secteur économique
        sectors_data = current_data.get('sectors', {})
        sectors_result = {}
        
        # Extraire les données pour les secteurs principaux
        for nace_code, sector_key in self.main_sectors.items():
            if nace_code in sectors_data:
                sector_data = sectors_data[nace_code]
                enterprises = sector_data.get('enterprises', 0)
                
                # Calculer un indice d'emploi approximatif
                # Ceci est une simplification - idéalement basé sur les données réelles d'emploi
                employment_index = 1.0  # Valeur par défaut
                
                # Certains secteurs ont typiquement plus d'employés par entreprise
                if sector_key in ['manufacturing', 'public_admin', 'health_social']:
                    employment_index = 1.5
                elif sector_key in ['agriculture', 'retail', 'professional']:
                    employment_index = 0.8
                    
                # Calculer la tendance si des données historiques existent
                trend = None
                if previous_year_data and 'sectors' in previous_year_data:
                    prev_sectors = previous_year_data['sectors']
                    if nace_code in prev_sectors:
                        prev_enterprises = prev_sectors[nace_code].get('enterprises', 0)
                        if prev_enterprises > 0:
                            trend_value, trend_formatted = self.calculate_change(enterprises, prev_enterprises)
                            trend = trend_formatted
                            
                sectors_result[sector_key] = {
                    "enterprise_count": enterprises,
                    "trend": trend,
                    "employment_index": employment_index
                }
                
        # Extraire les données par taille d'entreprise
        size_data = current_data.get('by_size', {})
        enterprise_size = {}
        
        # Catégories de taille d'entreprise
        size_categories = {
            "micro_enterprises_pct": 0,  # 0-9 employés
            "small_enterprises_pct": 0,  # 10-49 employés
            "medium_enterprises_pct": 0,  # 50-249 employés
            "large_enterprises_pct": 0   # 250+ employés
        }
        
        for size_code, size_info in size_data.items():
            min_employees = size_info.get('min_employees', 0)
            max_employees = size_info.get('max_employees', float('inf'))
            percentage = size_info.get('percentage', 0)
            
            # Classer selon les catégories standard
            if max_employees <= 9:
                size_categories["micro_enterprises_pct"] += percentage
            elif max_employees <= 49:
                size_categories["small_enterprises_pct"] += percentage
            elif max_employees <= 249:
                size_categories["medium_enterprises_pct"] += percentage
            else:
                size_categories["large_enterprises_pct"] += percentage
                
        # Extraire les données sur les entreprises étrangères
        foreign_data = current_data.get('foreign', {})
        foreign_enterprises = {
            "foreign_enterprises_count": foreign_data.get('enterprises', 0),
            "foreign_investment_growth": foreign_data.get('starts', 0) - foreign_data.get('stops', 0)
        }
        
        # Convertir la croissance en pourcentage
        if foreign_enterprises["foreign_enterprises_count"] > 0:
            foreign_growth_pct = (foreign_enterprises["foreign_investment_growth"] / foreign_enterprises["foreign_enterprises_count"]) * 100
            foreign_enterprises["foreign_investment_growth"] = f"+{self.format_percentage(foreign_growth_pct)}" if foreign_growth_pct >= 0 else f"-{self.format_percentage(abs(foreign_growth_pct))}"
        else:
            foreign_enterprises["foreign_investment_growth"] = "+0.0%"
            
        return {
            "enterprise_overview": enterprise_overview,
            "sectors": sectors_result,
            "enterprise_size": size_categories,
            "foreign_investment": foreign_enterprises
        }
        
    def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données complètes économiques.
        
        Args:
            data: Données brutes extraites.
            
        Returns:
            dict: Section economic_indicators du JSON.
        """
        tax_income_data = data.get('tax_income', {})
        unemployment_data = data.get('unemployment', {})
        business_activity_data = data.get('business_activity', {})
        
        result = {
            "income_tax": self.process_income_tax(tax_income_data),
            "business_activity": self.process_business_activity(business_activity_data),
            "unemployment": self.process_unemployment(unemployment_data)
        }
        
        return result