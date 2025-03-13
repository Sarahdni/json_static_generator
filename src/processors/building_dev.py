"""
Processeur pour les données de développement immobilier.
Transforme les données extraites en format JSON selon la structure définie.
"""
import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict

from src.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class BuildingDevProcessor(BaseProcessor):
    """Processeur pour les données de développement immobilier."""
    
    def __init__(self):
        """Initialise le processeur des données de développement immobilier."""
        super().__init__()
        
    def process_permits(self, counts_data: Dict[str, Any], surface_data: Dict[str, Any], volume_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données sur les permis de construire.
        
        Args:
            counts_data: Données brutes du nombre de permis.
            surface_data: Données brutes de surface des permis résidentiels.
            volume_data: Données brutes de volume des permis non résidentiels.
            
        Returns:
            dict: Section permits du JSON.
        """
        if not counts_data or 'current_data' not in counts_data:
            logger.warning("Données de permis manquantes ou incomplètes")
            return {}
            
        current_counts = counts_data.get('current_data', {})
        previous_year_counts = counts_data.get('previous_year_data', {})
        
        current_surface = surface_data.get('current_data', {}) if surface_data else {}
        previous_year_surface = surface_data.get('previous_year_data', {}) if surface_data else {}
        
        current_volume = volume_data.get('current_data', {}) if volume_data else {}
        previous_year_volume = volume_data.get('previous_year_data', {}) if volume_data else {}
        
        # Traiter les données de comptage des permis
        summary = self.process_permits_summary(current_counts, previous_year_counts)
        counts = self.process_permits_counts(current_counts)
        surface = self.process_permits_surface(current_surface, previous_year_surface, current_counts)
        volume = self.process_permits_volume(current_volume, previous_year_volume)
        
        return {
            "summary": summary,
            "counts": counts,
            "surface": surface,
            "volume": volume
        }
        
    def process_permits_summary(self, current_data: Dict[str, Any], previous_year_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données de résumé des permis de construire.
        
        Args:
            current_data: Données actuelles de comptage des permis.
            previous_year_data: Données de l'année précédente pour les comparaisons.
            
        Returns:
            dict: Section summary des permis.
        """
        # Extraire les totaux actuels
        total_buildings = current_data.get('total', {}).get('buildings', 0)
        
        # Calculer le nombre total de permis de l'année en cours (YTD)
        total_permits_ytd = total_buildings
        
        # Extraire les totaux de l'année précédente pour calculer la tendance
        prev_total_buildings = previous_year_data.get('total', {}).get('buildings', 0) if previous_year_data else 0
        
        # Calculer la tendance par rapport à l'année précédente
        trend_yoy_value, trend_yoy = self.calculate_change(total_buildings, prev_total_buildings)
        
        # Calculer le ratio résidentiel/non résidentiel
        residential_buildings = current_data.get('residential', {}).get('new_construction', {}).get('buildings', 0)
        residential_buildings += current_data.get('residential', {}).get('renovation', {}).get('buildings', 0)
        
        non_residential_buildings = current_data.get('non_residential', {}).get('new_construction', {}).get('buildings', 0)
        non_residential_buildings += current_data.get('non_residential', {}).get('renovation', {}).get('buildings', 0)
        
        residential_ratio = 0
        if total_buildings > 0:
            residential_ratio = residential_buildings / total_buildings
            
        return {
            "total_permits_ytd": total_permits_ytd,
            "trend_yoy": trend_yoy_value,
            "residential_ratio": residential_ratio
        }
        
    def process_permits_counts(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données détaillées de comptage des permis.
        
        Args:
            data: Données de comptage des permis.
            
        Returns:
            dict: Section counts des permis.
        """
        # Extraire les données résidentielles
        residential_new = data.get('residential', {}).get('new_construction', {})
        residential_renovation = data.get('residential', {}).get('renovation', {})
        
        # Extraire les données non résidentielles
        non_residential_new = data.get('non_residential', {}).get('new_construction', {})
        non_residential_renovation = data.get('non_residential', {}).get('renovation', {})
        
        return {
            "residential": {
                "new_construction": {
                    "buildings": residential_new.get('buildings', 0),
                    "dwellings": residential_new.get('dwellings', 0),
                    "houses": residential_new.get('houses', 0),
                    "apartments": residential_new.get('apartments', 0)
                },
                "renovation": {
                    "buildings": residential_renovation.get('buildings', 0),
                    "dwellings": residential_renovation.get('dwellings', 0)
                }
            },
            "non_residential": {
                "new_construction": {
                    "buildings": non_residential_new.get('buildings', 0)
                },
                "renovation": {
                    "buildings": non_residential_renovation.get('buildings', 0)
                }
            }
        }
        
    def process_permits_surface(self, current_data: Dict[str, Any], previous_year_data: Dict[str, Any], counts_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données de surface des permis résidentiels.
        
        Args:
            current_data: Données actuelles de surface des permis.
            previous_year_data: Données de l'année précédente pour les comparaisons.
            counts_data: Données de comptage pour les calculs complémentaires.
            
        Returns:
            dict: Section surface des permis.
        """
        # Extraire les surfaces
        total_surface_sqm = current_data.get('total_surface_m2', 0)
        avg_dwelling_size_sqm = current_data.get('avg_surface_per_dwelling_m2', 0)
        
        # Calculer la tendance par rapport à l'année précédente
        prev_surface = previous_year_data.get('total_surface_m2', 0) if previous_year_data else 0
        trend_yoy_value, trend_yoy = self.calculate_change(total_surface_sqm, prev_surface)
        
        # Si la taille moyenne n'est pas disponible, la calculer à partir des données de comptage
        if not avg_dwelling_size_sqm:
            dwellings_count = counts_data.get('residential', {}).get('new_construction', {}).get('dwellings', 0)
            if dwellings_count > 0:
                avg_dwelling_size_sqm = total_surface_sqm / dwellings_count
                
        return {
            "residential_new_construction_sqm": total_surface_sqm,
            "avg_dwelling_size_sqm": avg_dwelling_size_sqm,
            "trend_yoy": trend_yoy_value
        }
        
    def process_permits_volume(self, current_data: Dict[str, Any], previous_year_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données de volume des permis non résidentiels.
        
        Args:
            current_data: Données actuelles de volume des permis.
            previous_year_data: Données de l'année précédente pour les comparaisons.
            
        Returns:
            dict: Section volume des permis.
        """
        # Extraire les volumes
        total_volume_cubic_m = current_data.get('total_volume_m3', 0)
        
        # Calculer la tendance par rapport à l'année précédente
        prev_volume = previous_year_data.get('total_volume_m3', 0) if previous_year_data else 0
        trend_yoy_value, trend_yoy = self.calculate_change(total_volume_cubic_m, prev_volume)
        
        return {
            "non_residential_new_construction_cubic_m": total_volume_cubic_m,
            "trend_yoy": trend_yoy_value
        }
        
    def process_construction_activity(self, permits_data: Dict[str, Any], real_estate_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Traite les données d'activité de construction pour générer des indicateurs analytiques.
        
        Args:
            permits_data: Données des permis de construire.
            real_estate_data: Données du marché immobilier pour le contexte.
            
        Returns:
            dict: Section construction_activity du JSON.
        """
        # Calculer un indice d'intensité de construction
        # Cet indice est un indicateur composite basé sur plusieurs facteurs
        
        construction_intensity_index = 0.5  # Valeur par défaut moyenne
        
        if permits_data:
            # Récupérer les données clés des permis
            total_permits = permits_data.get('summary', {}).get('total_permits_ytd', 0)
            trend_yoy = permits_data.get('summary', {}).get('trend_yoy', 0)
            new_residential_buildings = permits_data.get('counts', {}).get('residential', {}).get('new_construction', {}).get('buildings', 0)
            new_residential_dwellings = permits_data.get('counts', {}).get('residential', {}).get('new_construction', {}).get('dwellings', 0)
            
            # Facteurs d'ajustement de l'indice
            if trend_yoy is not None and trend_yoy > 20:
                construction_intensity_index += 0.2
            elif trend_yoy > 10:
                construction_intensity_index += 0.1
            elif trend_yoy < -10:
                construction_intensity_index -= 0.1
            elif trend_yoy < -20:
                construction_intensity_index -= 0.2
                
            # Ajustement basé sur le volume de permis résidentiels
            if new_residential_dwellings > 100:
                construction_intensity_index += 0.1
            elif new_residential_dwellings > 50:
                construction_intensity_index += 0.05
                
        # Déterminer la phase de développement
        development_phase = "Stable"
        if construction_intensity_index > 0.8:
            development_phase = "Strong Growth"
        elif construction_intensity_index > 0.6:
            development_phase = "Moderate Growth"
        elif construction_intensity_index < 0.4:
            development_phase = "Slowdown"
        elif construction_intensity_index < 0.2:
            development_phase = "Stagnation"
            
        # Estimer le pipeline d'offre future
        supply_pipeline = {
            "residential_units_coming": new_residential_dwellings * 1.5 if 'new_residential_dwellings' in locals() else 0,
            "estimated_completion_timeframe": "18 months",
            "impact_on_supply": "Medium"
        }
        
        # Ajuster l'impact sur l'offre
        if supply_pipeline["residential_units_coming"] > 200:
            supply_pipeline["impact_on_supply"] = "High"
        elif supply_pipeline["residential_units_coming"] < 50:
            supply_pipeline["impact_on_supply"] = "Low"
            
        return {
            "construction_intensity_index": construction_intensity_index,
            "development_phase": development_phase,
            "supply_pipeline": supply_pipeline
        }
        
    def process_data(self, data: Dict[str, Any], real_estate_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Traite les données complètes de développement immobilier.
        
        Args:
            data: Données brutes extraites.
            real_estate_data: Données du marché immobilier pour le contexte.
            
        Returns:
            dict: Section building_development du JSON.
        """
        counts_data = data.get('permits_counts', {})
        surface_data = data.get('permits_surface', {})
        volume_data = data.get('permits_volume', {})
        
        # Traiter les données de permis
        permits_result = self.process_permits(counts_data, surface_data, volume_data)
        
        # Traiter les données d'activité de construction
        construction_activity_result = self.process_construction_activity(permits_result, real_estate_data)
        
        result = {
            "permits": permits_result,
            "construction_activity": construction_activity_result
        }
        
        return result