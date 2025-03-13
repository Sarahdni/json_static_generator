"""
Processeur pour les données du marché immobilier.
Transforme les données extraites en format JSON selon la structure définie.
"""
import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict

from src.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class RealEstateProcessor(BaseProcessor):
    """Processeur pour les données du marché immobilier."""
    
    def __init__(self):
        """Initialise le processeur du marché immobilier."""
        super().__init__()
        
        # Correspondance entre les codes de types de bâtiments et les noms pour le JSON
        self.building_type_mapping = {
            # Maisons
            "200": "houses",
            # Appartements
            "537": "apartments",
            # Studios
            "534": "studios",
            # Chambres
            "533": "rooms",
            # Fermes
            "240": "farms",
            # Maisons de commerce
            "407": "commercial_houses",
            # Autres types résidentiels
            "OTHER": "other_residential"
        }
        
    def process_municipality_overview(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données du marché immobilier au niveau municipal.
        
        Args:
            data: Données brutes extraites.
            
        Returns:
            dict: Section municipality_overview du JSON.
        """
        # Vérifier si les données nécessaires sont présentes
        if not data or 'current_data' not in data:
            logger.warning("Données municipales manquantes ou incomplètes")
            return {}
            
        current_data = data['current_data']
        previous_year_data = data.get('previous_year_data', {})
        five_year_data = data.get('five_year_data', {})
        
        # Agréger les transactions totales toutes catégories confondues
        total_transactions = 0
        total_price = 0
        total_surface = 0
        
        for building_type, building_data in current_data.items():
            if building_data.get('ms_total_transactions'):
                total_transactions += building_data['ms_total_transactions']
            if building_data.get('ms_total_price'):
                total_price += building_data['ms_total_price']
            if building_data.get('ms_total_surface'):
                total_surface += building_data['ms_total_surface']
                
        # Calculer les moyennes
        avg_price = self.calculate_avg(total_price, total_transactions)
        avg_surface = self.calculate_avg(total_surface, total_transactions)
        
        # Obtenir les données pour les comparaisons
        prev_year_transactions = 0
        prev_year_total_price = 0
        for _, building_data in previous_year_data.items():
            if building_data.get('ms_total_transactions'):
                prev_year_transactions += building_data['ms_total_transactions']
            if building_data.get('ms_total_price'):
                prev_year_total_price += building_data['ms_total_price']
                
        five_year_transactions = 0
        five_year_total_price = 0
        for _, building_data in five_year_data.items():
            if building_data.get('ms_total_transactions'):
                five_year_transactions += building_data['ms_total_transactions']
            if building_data.get('ms_total_price'):
                five_year_total_price += building_data['ms_total_price']
                
        five_year_avg_price = self.calculate_avg(five_year_total_price, five_year_transactions)
        prev_year_avg_price = self.calculate_avg(prev_year_total_price, prev_year_transactions)
        
        # Calculer les pourcentages de variation
        transaction_change_1y, transaction_change_1y_formatted = self.calculate_change(
            total_transactions, prev_year_transactions)
            
        price_change_1y, price_change_1y_formatted = self.calculate_change(
            avg_price, prev_year_avg_price)
            
        transaction_change_5y, transaction_change_5y_formatted = self.calculate_change(
            total_transactions, five_year_transactions)
            
        price_change_5y, price_change_5y_formatted = self.calculate_change(
            avg_price, five_year_avg_price)
        
        # Construction du résultat selon la structure JSON définie
        result = {
            "last_period": {
                "total_transactions": total_transactions,
                "price_trends": {
                    "mean_price": avg_price,
                    "median_price": None,  # À calculer à partir des données détaillées
                    "price_p10": None,     # À calculer à partir des données détaillées
                    "price_p25": None,     # À calculer à partir des données détaillées
                    "price_p75": None,     # À calculer à partir des données détaillées
                    "price_p90": None      # À calculer à partir des données détaillées
                },
                "total_surface_sqm": total_surface,
                "avg_surface_sqm": avg_surface
            },
            "historical_trends": {
                "year_over_year": {
                    "transaction_change_pct": transaction_change_1y,
                    "price_change_pct": price_change_1y
                },
                "five_year": {
                    "transaction_change_pct": transaction_change_5y,
                    "price_change_pct": price_change_5y
                }
            }
        }
        
        # Ajouter les percentiles de prix si disponibles (pour le type de bien le plus courant)
        most_common_type = None
        max_transactions = 0
        
        for building_type, building_data in current_data.items():
            transactions = building_data.get('ms_total_transactions', 0)
            if transactions > max_transactions:
                max_transactions = transactions
                most_common_type = building_type
                
        if most_common_type and most_common_type in current_data:
            data = current_data[most_common_type]
            result["last_period"]["price_trends"].update({
                "median_price": data.get('ms_price_p50'),
                "price_p10": data.get('ms_price_p10'),
                "price_p25": data.get('ms_price_p25'),
                "price_p75": data.get('ms_price_p75'),
                "price_p90": data.get('ms_price_p90')
            })
            
        return result
    
    def process_property_types(self, data: Dict[str, Any], building_stock: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données par type de propriété.
        
        Args:
            data: Données brutes du marché immobilier.
            building_stock: Données brutes du stock de bâtiments.
            
        Returns:
            dict: Section by_property_type du JSON.
        """
        if not data or 'current_data' not in data:
            logger.warning("Données par type de propriété manquantes ou incomplètes")
            return {}
            
        current_data = data.get('current_data', {})
        previous_year_data = data.get('previous_year_data', {})
        five_year_data = data.get('five_year_data', {})
        
        building_stock_data = {}
        if building_stock and 'current_data' in building_stock:
            building_stock_data = building_stock['current_data']
        
        # Déterminer le type de bien le plus courant
        most_common_type = None
        max_transactions = 0
        
        # Déterminer le segment avec la plus forte croissance
        fastest_growing_segment = None
        highest_growth_rate = -100
        
        # Déterminer le segment le plus cher
        most_valuable_segment = None
        highest_price = 0
        
        # Préparer les données détaillées par type de bien
        detailed_types = {}
        
        for code, building_data in current_data.items():
            # Skip if not relevant building type
            if code not in self.building_type_mapping:
                continue
                
            type_key = self.building_type_mapping[code]
            transactions = building_data.get('ms_total_transactions', 0)
            
            # Check for most common type
            if self.is_numeric_and_greater_than(transactions, max_transactions):
                max_transactions = transactions
                most_common_type = type_key
                
            # Get price data
            mean_price = building_data.get('ms_mean_price')
            
            # Check for most valuable segment
            if self.is_numeric_and_greater_than(mean_price, highest_price):
                highest_price = mean_price
                most_valuable_segment = type_key
                
            # Get historical data for growth calculation
            prev_year_price = None
            five_year_price = None
            
            if code in previous_year_data:
                prev_year_price = previous_year_data[code].get('ms_mean_price')
                
            if code in five_year_data:
                five_year_price = five_year_data[code].get('ms_mean_price')
                
            # Calculate growth rates
            price_change_1y, price_change_1y_formatted = self.calculate_change(
                mean_price, prev_year_price)
                
            price_change_5y, price_change_5y_formatted = self.calculate_change(
                mean_price, five_year_price)
                
            # Check for fastest growing segment
            if self.is_numeric_and_greater_than(price_change_1y, highest_growth_rate):
                highest_growth_rate = price_change_1y
                fastest_growing_segment = type_key
                
            # Get building stock data
            inventory_count = 0
            percentage_of_stock = 0
            
            # Try to map the code to the building stock data
            stock_code = code
            if stock_code in building_stock_data:
                stock_data = building_stock_data[stock_code]
                for stat_type, stat_data in stock_data.get('statistics', {}).items():
                    if stat_type == 'TOTAL':
                        inventory_count = stat_data.get('count', 0)
                        
            # Calculate price per sqm if surface data available
            price_per_sqm = None
            if building_data.get('ms_total_surface') and building_data.get('ms_total_price'):
                total_surface = self.safe_numeric_value(building_data['ms_total_surface'])
                total_price = self.safe_numeric_value(building_data['ms_total_price'])
                if self.is_numeric_and_greater_than(total_surface, 0):
                    price_per_sqm = total_price / total_surface
                    
            # Create detailed entry for this type
            detailed_types[type_key] = {
                "code": code,
                "transaction_count": transactions,
                "inventory_count": inventory_count,
                "percentage_of_stock": percentage_of_stock,
                "price_data": {
                    "mean_price": mean_price,
                    "median_price": building_data.get('ms_price_p50'),
                    "price_per_sqm": price_per_sqm,
                    "price_evolution_1y": price_change_1y_formatted,
                    "price_evolution_5y": price_change_5y_formatted
                }
            }
            
        # Calculate percentage of stock now that we have total
        total_stock = sum(t["inventory_count"] for t in detailed_types.values())
        for type_key, type_data in detailed_types.items():
            if self.is_numeric_and_greater_than(total_stock, 0):
                type_data["percentage_of_stock"] = (type_data["inventory_count"] / total_stock) * 100
                
        # Build the summary
        summary = {
            "most_common_type": most_common_type,
            "fastest_growing_segment": fastest_growing_segment,
            "most_valuable_segment": most_valuable_segment
        }
        
        return {
            "summary": summary,
            "detailed_types": detailed_types
        }
    
    def process_sector_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données du marché immobilier par secteur statistique.
        
        Args:
            data: Données brutes extraites.
            
        Returns:
            dict: Section sector_analysis du JSON.
        """
        if not data:
            logger.warning("Données par secteur manquantes ou incomplètes")
            return {}
            
        # Préparer les données des secteurs
        sectors = []
        all_prices = []
        
        # Identifier le secteur le plus cher et le plus abordable
        hottest_sector = None
        highest_median_price = 0
        most_affordable_sector = None
        lowest_median_price = float('inf')
        
        for sector_id, sector_data in data.items():
            sector_name = sector_data.get('sector_name', f"Sector-{sector_id}")
            
            # Agréger les données de tous les types résidentiels
            total_transactions = 0
            all_sector_prices = []
            
            for res_type, type_data in sector_data.get('residential_types', {}).items():
                transactions = type_data.get('nb_transactions', 0)
                total_transactions += transactions
                
                median_price = type_data.get('ms_price_p50')
                if median_price is not None:
                    all_sector_prices.append(median_price)
                    all_prices.append(median_price)
                    
            if not all_sector_prices:
                continue
                
            # Calculer le prix médian pour ce secteur (médiane des médianes)
            all_sector_prices.sort()
            sector_median_price = all_sector_prices[len(all_sector_prices) // 2] if all_sector_prices else None
            
            # Vérifier si c'est le secteur le plus cher ou le plus abordable
            if sector_median_price:
                if self.is_numeric_and_greater_than(sector_median_price, highest_median_price):
                    highest_median_price = sector_median_price
                    hottest_sector = sector_name
                if self.is_numeric_and_less_than(sector_median_price, lowest_median_price):
                    lowest_median_price = sector_median_price
                    most_affordable_sector = sector_name
                    
            # Créer l'entrée pour ce secteur
            sectors.append({
                "sector_id": sector_id,
                "sector_name": sector_name,
                "transaction_count": total_transactions,
                "median_price": sector_median_price,
                "price_trend": None,  # Nécessite des données historiques
                "transaction_trend": None  # Nécessite des données historiques
            })
            
        # Calculer l'indice de disparité des prix (écart max / min)
        price_disparity_index = None
        if all_prices and self.is_numeric_and_greater_than(lowest_median_price, 0):
            price_disparity_index = (highest_median_price - lowest_median_price) / lowest_median_price
            
        # Créer le résumé
        summary = {
            "sectors_count": len(sectors),
            "price_disparity_index": price_disparity_index,
            "hottest_sector": hottest_sector,
            "most_affordable_sector": most_affordable_sector
        }
        
        return {
            "summary": summary,
            "sectors": sectors
        }
    
    def process_building_stock(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données du stock de bâtiments.
        
        Args:
            data: Données brutes extraites.
            
        Returns:
            dict: Section building_stock du JSON.
        """
        if not data or 'current_data' not in data:
            logger.warning("Données de stock de bâtiments manquantes ou incomplètes")
            return {}
            
        current_data = data['current_data']
        five_year_data = data.get('five_year_data', {})
        
        # Calculer le nombre total de bâtiments
        total_buildings = 0
        building_types = {}
        building_ages = {}
        
        # Calculer le nombre total d'unités de logement
        total_housing_units = 0
        
        for code, building_data in current_data.items():
            building_desc = building_data.get('description', 'Unknown')
            
            for stat_type, stat_data in building_data.get('statistics', {}).items():
                count = stat_data.get('count', 0)
                stat_desc = stat_data.get('description', 'Unknown')
                
                # Traiter les totaux
                if stat_type == 'TOTAL':
                    total_buildings += count
                    
                    # Ajouter au dictionnaire des types
                    key = code
                    if key == '200':  # Maisons unifamiliales
                        building_types['single_family_houses'] = {
                            "count": count,
                            "percentage": 0,  # À calculer plus tard
                            "evolution_5y": None  # À calculer plus tard
                        }
                    elif key == '401':  # Bâtiments à appartements
                        building_types['apartment_buildings'] = {
                            "count": count,
                            "percentage": 0,
                            "evolution_5y": None
                        }
                    elif key == '407':  # Bâtiments à usage mixte
                        building_types['mixed_use_buildings'] = {
                            "count": count,
                            "percentage": 0,
                            "evolution_5y": None
                        }
                    elif key in ['410', '411', '412', '420']:  # Bâtiments commerciaux
                        if 'commercial_buildings' not in building_types:
                            building_types['commercial_buildings'] = {
                                "count": 0,
                                "percentage": 0,
                                "evolution_5y": None
                            }
                        building_types['commercial_buildings']['count'] += count
                    else:
                        if 'other_buildings' not in building_types:
                            building_types['other_buildings'] = {
                                "count": 0,
                                "percentage": 0,
                                "evolution_5y": None
                            }
                        building_types['other_buildings']['count'] += count
                
                # Traiter les âges des bâtiments
                elif stat_type.startswith('AGE_'):
                    if 'pre_1945' in stat_desc.lower():
                        building_ages['pre_1945'] = {
                            "count": count,
                            "percentage": 0  # À calculer plus tard
                        }
                    elif '1945_1970' in stat_desc.lower() or ('1945' in stat_desc.lower() and '1970' in stat_desc.lower()):
                        building_ages['1945_1970'] = {
                            "count": count,
                            "percentage": 0
                        }
                    elif '1971_2000' in stat_desc.lower() or ('1971' in stat_desc.lower() and '2000' in stat_desc.lower()):
                        building_ages['1971_2000'] = {
                            "count": count,
                            "percentage": 0
                        }
                    elif 'post_2000' in stat_desc.lower() or '2000' in stat_desc.lower():
                        building_ages['post_2000'] = {
                            "count": count,
                            "percentage": 0
                        }
                
                # Compter les unités de logement
                elif stat_type == 'HOUSING_UNITS':
                    total_housing_units += count
                
        # Calculer les pourcentages pour les types de bâtiments
        for building_type in building_types.values():
            building_type["percentage"] = (building_type["count"] / total_buildings) * 100 if total_buildings else 0
            
        # Calculer les pourcentages pour les âges des bâtiments
        for age_group in building_ages.values():
            age_group["percentage"] = (age_group["count"] / total_buildings) * 100 if total_buildings else 0
            
        # Calculer l'évolution sur 5 ans
        if five_year_data:
            for code, building_data in five_year_data.items():
                for stat_type, stat_data in building_data.get('statistics', {}).items():
                    if stat_type == 'TOTAL':
                        old_count = stat_data.get('count', 0)
                        
                        # Trouver le type correspondant
                        key = code
                        target_type = None
                        
                        if key == '200':
                            target_type = 'single_family_houses'
                        elif key == '401':
                            target_type = 'apartment_buildings'
                        elif key == '407':
                            target_type = 'mixed_use_buildings'
                        elif key in ['410', '411', '412', '420']:
                            target_type = 'commercial_buildings'
                        else:
                            target_type = 'other_buildings'
                            
                        if target_type in building_types:
                            current_count = building_types[target_type]['count']
                            change, change_formatted = self.calculate_change(current_count, old_count)
                            building_types[target_type]['evolution_5y'] = change_formatted
        
        # Calculer les unités par bâtiment
        units_per_building = self.calculate_avg(total_housing_units, total_buildings)
        
        # Construire le résultat
        return {
            "total_buildings": total_buildings,
            "by_type": building_types,
            "by_age": building_ages,
            "housing_units": {
                "total_count": total_housing_units,
                "units_per_building_avg": units_per_building,
                "units_per_capita": None  # Nécessite des données démographiques
            }
        }
        
    def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traite les données complètes du marché immobilier.
        """
        try:
            municipality_data = data.get('municipality_data', {})
            sector_data = data.get('sector_data', {})
            building_stock_data = data.get('building_stock', {})
            
            result = {
                "municipality_overview": self.process_municipality_overview(municipality_data),
                "by_property_type": self.process_property_types(municipality_data, building_stock_data),
                "sector_analysis": self.process_sector_analysis(sector_data),
                "building_stock": self.process_building_stock(building_stock_data)
            }
            
            return result
        except Exception as e:
            logger.error(f"Erreur dans RealEstateProcessor.process_data: {str(e)}")
            return {
                "municipality_overview": {},
                "by_property_type": {},
                "sector_analysis": {},
                "building_stock": {}
            }