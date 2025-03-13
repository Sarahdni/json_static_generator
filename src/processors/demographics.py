"""
Processeur pour les données démographiques.
Transforme les données extraites en format JSON selon la structure définie.
"""
import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict

from src.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class DemographicsProcessor(BaseProcessor):
    """Processeur pour les données démographiques."""
    
    def __init__(self):
        """Initialise le processeur des données démographiques."""
        super().__init__()
        
        # Mappings des groupes d'âge
        self.age_group_mapping = {
            # Moins de 18 ans
            "under_18": {
                "min": 0,
                "max": 17
            },
            # 18 à 35 ans
            "18_to_35": {
                "min": 18,
                "max": 35
            },
            # 36 à 65 ans
            "36_to_65": {
                "min": 36,
                "max": 65
            },
            # Plus de 65 ans
            "over_65": {
                "min": 66,
                "max": 120
            }
        }
        
        # Mappings des groupes de nationalité
        self.nationality_mapping = {
            "BE": "belgian",
            "EU": "eu_non_belgian",
            "OTHER": "non_eu"
        }
        
    def process_population_overview(self, data: Dict[str, Any], area_km2: Optional[float] = None) -> Dict[str, Any]:
        """
        Traite les données générales de la population.
        
        Args:
            data: Données brutes de la structure de population.
            area_km2: Superficie de la commune en km², si disponible.
            
        Returns:
            dict: Section population_overview du JSON.
        """
        if not data or 'current_data' not in data:
            logger.warning("Données de population manquantes ou incomplètes")
            return {}
            
        current_data = data['current_data']
        previous_year_data = data.get('previous_year_data', {})
        five_year_data = data.get('five_year_data', {})
        
        # Extraire la population totale actuelle
        total_population = current_data.get('total_population', 0)
        
        # Calculer la densité de population si la superficie est disponible
        population_density = None
        if area_km2 and area_km2 > 0:
            population_density = total_population / area_km2
            
        # Calculer les variations de population
        previous_population = previous_year_data.get('total_population', 0) if previous_year_data else 0
        five_year_population = five_year_data.get('total_population', 0) if five_year_data else 0
        
        one_year_growth, _ = self.calculate_change(total_population, previous_population)
        five_year_growth, _ = self.calculate_change(total_population, five_year_population)
        
        # Estimer la croissance future (simple projection linéaire)
        forecast_growth = None
        if five_year_growth is not None:
            # Taux de croissance annuel moyen sur 5 ans
            forecast_growth = five_year_growth / 5
            
        return {
            "total_population": total_population,
            "population_density": population_density,
            "population_trend": {
                "one_year_growth": one_year_growth,
                "five_year_growth": five_year_growth,
                "forecast_growth": forecast_growth
            }
        }
        
    def process_age_structure(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données de structure d'âge de la population en utilisant les âges exacts.
        
        Args:
            data: Données brutes de la structure de population.
            
        Returns:
            dict: Section age_structure du JSON.
        """
        if not data or 'current_data' not in data:
            logger.warning("Données de structure d'âge manquantes ou incomplètes")
            return {}
            
        current_data = data['current_data']
        previous_year_data = data.get('previous_year_data', {})
        
        # Extraire les données par âge exact
        ages_data_current = current_data.get('ages_data', {})
        total_population = current_data.get('total_population', 0) or 1  # Éviter division par zéro
        
        # Préparation des groupes d'âge agrégés selon notre mapping
        age_groups = {}
        for age_group_key, age_range in self.age_group_mapping.items():
            age_groups[age_group_key] = {
                "count": 0,
                "percentage": 0,
                "trend": None
            }
            
        # Agréger les données par nos groupes d'âge personnalisés
        for exact_age_str, age_data in ages_data_current.items():
            try:
                exact_age = int(exact_age_str)
                
                # Calculer la population totale pour cet âge (hommes + femmes)
                age_population = 0
                for sex_data in age_data.get('sexes', {}).values():
                    age_population += sex_data.get('population', 0)
                    
                # Déterminer à quel groupe d'âge cette population appartient
                for age_group_key, age_range in self.age_group_mapping.items():
                    if exact_age >= age_range['min'] and exact_age <= age_range['max']:
                        age_groups[age_group_key]["count"] += age_population
                        break
            except (ValueError, TypeError):
                continue
                
        # Calculer les pourcentages
        for age_group in age_groups.values():
            age_group["percentage"] = (age_group["count"] / total_population) * 100
            
        # Calculer les tendances avec l'année précédente
        if previous_year_data:
            ages_data_prev = previous_year_data.get('ages_data', {})
            prev_total_population = previous_year_data.get('total_population', 0) or 1
            
            # Préparation des groupes d'âge précédents
            prev_age_groups = {}
            for age_group_key in self.age_group_mapping.keys():
                prev_age_groups[age_group_key] = 0
                
            # Agréger les données précédentes par nos groupes d'âge
            for exact_age_str, age_data in ages_data_prev.items():
                try:
                    exact_age = int(exact_age_str)
                    
                    # Calculer la population totale pour cet âge (hommes + femmes)
                    age_population = 0
                    for sex_data in age_data.get('sexes', {}).values():
                        age_population += sex_data.get('population', 0)
                        
                    # Déterminer à quel groupe d'âge cette population appartient
                    for age_group_key, age_range in self.age_group_mapping.items():
                        if exact_age >= age_range['min'] and exact_age <= age_range['max']:
                            prev_age_groups[age_group_key] += age_population
                            break
                except (ValueError, TypeError):
                    continue
                    
            # Calculer les tendances (variation de pourcentage)
            for age_group_key, age_group in age_groups.items():
                prev_count = prev_age_groups[age_group_key]
                
                if prev_count > 0 and prev_total_population > 0:
                    prev_percentage = (prev_count / prev_total_population) * 100
                    
                    # Calculer la tendance avec une précision améliorée
                    change, change_formatted = self.calculate_change(age_group["percentage"], prev_percentage)
                    
                    # S'assurer qu'une petite tendance est toujours visible
                    if change and abs(change) < 0.1 and change != 0:
                        sign = "+" if change > 0 else "-"
                        change_formatted = f"{sign}0,1%"
                    
                    age_group["trend"] = change_formatted
                else:
                    age_group["trend"] = "N/A"
                    
        # Calculer le ratio de dépendance
        dependent_population = age_groups["under_18"]["count"] + age_groups["over_65"]["count"]
        working_age_population = age_groups["18_to_35"]["count"] + age_groups["36_to_65"]["count"]
        
        dependency_ratio = None
        if working_age_population > 0:
            dependency_ratio = dependent_population / working_age_population
        
        # Estimons aussi l'âge médian
        median_age = self.calculate_median_age(ages_data_current, total_population)
        
        return {
            "median_age": median_age,
            "age_groups": age_groups,
            "dependency_ratio": dependency_ratio
        }

    def calculate_median_age(self, ages_data: Dict[str, Any], total_population: int) -> Optional[float]:
        """
        Calcule l'âge médian à partir des données d'âge exact.
        """
        if not ages_data or total_population <= 0:
            return None
            
        try:
            # Créer une liste de tuples (âge, population)
            age_distribution = []
            for age_str, age_data in ages_data.items():
                try:
                    age = int(age_str)
                    population = sum(sex_data.get('population', 0) for sex_data in age_data.get('sexes', {}).values())
                    age_distribution.append((age, population))
                except (ValueError, TypeError):
                    continue
                    
            if not age_distribution:
                return None
                
            # Trier par âge
            age_distribution.sort(key=lambda x: x[0])
            
            # Calculer la population cumulée pour trouver l'âge médian
            cumulative_population = 0
            median_position = total_population / 2
            
            for age, population in age_distribution:
                cumulative_population += population
                if cumulative_population >= median_position:
                    return age
                    
            return None
        except Exception as e:
            logger.error(f"Erreur lors du calcul de l'âge médian: {str(e)}")
            return None

    def process_household_composition(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données de composition des ménages.
        
        Args:
            data: Données brutes de la composition des ménages.
            
        Returns:
            dict: Section household_composition du JSON.
        """
        if not data:
            logger.warning("Données de composition des ménages manquantes ou incomplètes")
            return {}
            
        # Extraire les totaux
        total_types = data.get('total_household_types', 0)
        total_individuals = data.get('total_individuals', 0)
        
        # Calculer la taille moyenne des ménages
        average_household_size = None
        if total_types > 0:
            average_household_size = total_individuals / total_types
            
        # Préparer la structure pour les types de ménages
        household_types = {
            "single_person": {
                "count": 0,
                "percentage": 0,
                "breakdown_by_age": {
                    "under_35": {
                        "count": 0,
                        "percentage": 0
                    },
                    "35_to_65": {
                        "count": 0,
                        "percentage": 0
                    },
                    "over_65": {
                        "count": 0,
                        "percentage": 0
                    }
                }
            },
            "couples_without_children": {
                "count": 0,
                "percentage": 0
            },
            "couples_with_children": {
                "count": 0,
                "percentage": 0,
                "breakdown_by_children": {
                    "with_1_child": {
                        "count": 0,
                        "percentage": 0
                    },
                    "with_2_children": {
                        "count": 0,
                        "percentage": 0
                    },
                    "with_3_plus_children": {
                        "count": 0,
                        "percentage": 0
                    }
                }
            },
            "single_parent": {
                "count": 0,
                "percentage": 0
            }
        }
        
        # Traiter les données brutes des types de cohabitation
        cohabitation_types = data.get('cohabitation_types', {})
        
        for cohab_type, cohab_data in cohabitation_types.items():
            count = cohab_data.get('total_count', 0)
            desc = cohab_data.get('description', '').lower()
            
            # Classifier selon notre structure
            if 'isolé' in desc or 'single' in desc:
                household_types["single_person"]["count"] += count
                
                # Répartir par âge si les données par âge sont disponibles
                age_groups = cohab_data.get('age_groups', {})
                for age_group, age_count in age_groups.items():
                    # Mapping des codes d'âge vers nos catégories
                    if age_group.startswith("0") or age_group.startswith("1") or age_group.startswith("2") or (age_group.startswith("3") and int(age_group[1:]) < 5):
                        household_types["single_person"]["breakdown_by_age"]["under_35"]["count"] += age_count
                    elif (age_group.startswith("3") and int(age_group[1:]) >= 5) or age_group.startswith("4") or age_group.startswith("5") or age_group.startswith("6"):
                        household_types["single_person"]["breakdown_by_age"]["35_to_65"]["count"] += age_count
                    else:
                        household_types["single_person"]["breakdown_by_age"]["over_65"]["count"] += age_count
                        
            elif ('couple' in desc and 'sans enfant' in desc) or ('couple' in desc and 'without children' in desc):
                household_types["couples_without_children"]["count"] += count
                
            elif ('couple' in desc and 'avec enfant' in desc) or ('couple' in desc and 'with children' in desc):
                household_types["couples_with_children"]["count"] += count
                
                # Si des données sur le nombre d'enfants sont disponibles
                # (à adapter selon la structure exacte des données)
                
            elif ('monoparental' in desc) or ('single parent' in desc):
                household_types["single_parent"]["count"] += count
                
        # Calculer les pourcentages
        total_count = sum(ht["count"] for ht in household_types.values())
        if total_count > 0:
            for household_type, data in household_types.items():
                data["percentage"] = (data["count"] / total_count) * 100
                
                # Calculer aussi les pourcentages des sous-catégories
                if household_type == "single_person":
                    total_singles = data["count"] or 1  # Éviter division par zéro
                    for age_category in data["breakdown_by_age"].values():
                        age_category["percentage"] = (age_category["count"] / total_singles) * 100
                        
                elif household_type == "couples_with_children":
                    total_families = data["count"] or 1  # Éviter division par zéro
                    for child_category in data["breakdown_by_children"].values():
                        child_category["percentage"] = (child_category["count"] / total_families) * 100
                        
        # Tendances d'évolution (nécessiteraient des données historiques)
        household_evolution = {
            "trend_single_person": None,
            "trend_family": None,
            "trend_average_size": None
        }
        
        return {
            "total_households": total_types,
            "average_household_size": average_household_size,
            "household_types": household_types,
            "household_evolution": household_evolution
        }
        
    def process_cultural_diversity(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données de diversité culturelle (nationalités).
        
        Args:
            data: Données brutes de la structure de population.
            
        Returns:
            dict: Section cultural_diversity du JSON.
        """
        if not data or 'current_data' not in data:
            logger.warning("Données de nationalités manquantes ou incomplètes")
            return {}
            
        current_data = data['current_data']
        
        # Extraire les données de nationalité
        nationalities_raw = current_data.get('nationalities', {})
        total_population = current_data.get('total_population', 0) or 1  # Éviter division par zéro
        
        # Agréger par groupe de nationalité
        nationality_groups = {
            "belgian": {
                "count": 0,
                "percentage": 0
            },
            "eu_non_belgian": {
                "count": 0,
                "percentage": 0
            },
            "non_eu": {
                "count": 0,
                "percentage": 0
            }
        }
        
        for nat_code, nat_data in nationalities_raw.items():
            count = nat_data.get('population', 0)
            group = nat_data.get('group', 'OTHER')
            
            # Mapper le groupe vers notre structure
            if nat_code == 'BE':
                nationality_groups["belgian"]["count"] += count
            elif group == 'EU':
                nationality_groups["eu_non_belgian"]["count"] += count
            else:
                nationality_groups["non_eu"]["count"] += count
                
        # Calculer les pourcentages
        for group in nationality_groups.values():
            group["percentage"] = (group["count"] / total_population) * 100
            
        return {
            "nationality_groups": nationality_groups
        }
        
    def process_vehicles(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données de véhicules des ménages.
        
        Args:
            data: Données brutes des véhicules des ménages.
            
        Returns:
            dict: Données pour la section mobility.vehicle_ownership du JSON.
        """
        if not data:
            logger.warning("Données de véhicules manquantes ou incomplètes")
            return {}
            
        commune_totals = data.get('commune_totals', {})
        sectors = data.get('sectors', {})
        
        # Extraire les totaux communaux
        total_households = commune_totals.get('total_households', 0) or 1  # Éviter division par zéro
        total_vehicles = commune_totals.get('total_vehicles', 0)
        avg_vehicles = commune_totals.get('avg_vehicles_per_household', 0)
        
        # Estimations pour les ménages sans véhicule et avec plusieurs véhicules
        # Ces estimations sont approximatives sans données exactes
        # Dans un cas réel, il faudrait les calculs exacts à partir des données complètes
        households_without_vehicle_est = 0
        households_with_multiple_est = 0
        
        # On peut estimer avec une distribution approximative
        if avg_vehicles and total_households:
            # Estimation des ménages sans véhicule (approximation)
            households_without_vehicle_est = total_households * (1 - min(1, avg_vehicles))
            
            # Estimation des ménages avec plusieurs véhicules (approximation)
            if avg_vehicles > 1:
                households_with_multiple_est = total_households * ((avg_vehicles - 1) / avg_vehicles)
            else:
                households_with_multiple_est = total_households * 0.1  # Estimation minimale
                
        # Calculer les pourcentages
        households_without_vehicle_pct = (households_without_vehicle_est / total_households) * 100
        households_with_multiple_pct = (households_with_multiple_est / total_households) * 100
        
        # Préparation des données par secteur
        sectors_data = []
        
        for sector_id, sector_data in sectors.items():
            sector_name = sector_data.get('sector_name', f"Sector-{sector_id}")
            vehicles_per_household = sector_data.get('vehicles_per_household', 0)
            
            # Calculer l'écart par rapport à la moyenne
            comparison_to_average = None
            if avg_vehicles > 0:
                comparison_to_average = ((vehicles_per_household - avg_vehicles) / avg_vehicles) * 100
                
            sectors_data.append({
                "sector_id": sector_id,
                "sector_name": sector_name,
                "vehicles_per_household": vehicles_per_household,
                "comparison_to_average": comparison_to_average
            })
            
        # Trier par écart à la moyenne
        sectors_data.sort(key=lambda x: x.get('comparison_to_average', 0) if x.get('comparison_to_average') is not None else 0, reverse=True)
        
        return {
            "vehicle_ownership": {
                "average_vehicles_per_household": avg_vehicles,
                "households_with_no_vehicle_pct": households_without_vehicle_pct,
                "households_with_multiple_vehicles_pct": households_with_multiple_pct
            },
            "vehicle_distribution": {
                "by_sector": sectors_data
            }
        }
        
    def process_data(self, data: Dict[str, Any], area_km2: Optional[float] = None) -> Dict[str, Any]:
        """
        Traite les données complètes de démographie.
        
        Args:
            data: Données brutes extraites.
            area_km2: Superficie de la commune en km², si disponible.
            
        Returns:
            dict: Sections demographics et geographical_context.mobility du JSON.
        """
        population_data = data.get('population_structure', {})
        household_data = data.get('household_composition', {})
        vehicles_data = data.get('household_vehicles', {})
        
        # Traiter les données démographiques
        demographics_result = {
            "population_overview": self.process_population_overview(population_data, area_km2),
            "age_structure": self.process_age_structure(population_data),
            "household_composition": self.process_household_composition(household_data),
            "cultural_diversity": self.process_cultural_diversity(population_data)
        }
        
        # Traiter les données de mobilité pour geographical_context
        mobility_result = self.process_vehicles(vehicles_data)
        
        return {
            "demographics": demographics_result,
            "geographical_context": {
                "mobility": mobility_result
            }
        }