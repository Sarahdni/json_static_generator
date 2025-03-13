"""
Processeur pour les analyses d'investissement immobilier.
Génère des indicateurs et métriques pour l'évaluation des opportunités d'investissement.
"""
import logging
from typing import Dict, List, Any, Optional

from src.processors.base import BaseProcessor
from src.config.settings import THRESHOLDS

logger = logging.getLogger(__name__)

class InvestmentProcessor(BaseProcessor):
    """Processeur pour les analyses d'investissement immobilier."""
    
    def __init__(self):
        """Initialise le processeur d'analyses d'investissement."""
        super().__init__()
        
        # Paramètres de calcul de rendement locatif
        self.rental_yield_params = {
            "default_gross_yield": 0.042,  # 4.2% rendement brut par défaut
            "annual_costs_percentage": 0.25,  # 25% des revenus bruts pour les charges
            "vacancy_rate": 0.05,  # 5% de taux de vacance
        }
        
        # Paramètres de calcul d'accessibilité
        self.affordability_params = {
            "mortgage_rate": 0.025,  # 2.5% taux d'emprunt
            "mortgage_term_years": 25,  # Durée emprunt
            "down_payment_percentage": 0.2,  # 20% apport personnel
            "debt_service_ratio_max": 0.33,  # Ratio maximum de service de la dette
            "monthly_expenses_ratio": 0.30,  # 30% des revenus pour les charges mensuelles
        }
        
    def process_affordability_metrics(self, real_estate_data: Dict[str, Any], economic_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcule les métriques d'accessibilité au marché immobilier.
        
        Args:
            real_estate_data: Données du marché immobilier.
            economic_data: Données économiques.
            
        Returns:
            dict: Section affordability_metrics du JSON.
        """
        # Extraire les prix immobiliers
        median_price = 0
        if real_estate_data and 'municipality_overview' in real_estate_data:
            price_trends = real_estate_data['municipality_overview'].get('last_period', {}).get('price_trends', {})
            median_price = price_trends.get('median_price', 0)
            
        # Extraire les revenus
        avg_income = 0
        if economic_data and 'income_tax' in economic_data:
            income_overview = economic_data['income_tax'].get('income_overview', {})
            avg_income = income_overview.get('average_income', 0)
            
        # Calculer le ratio prix/revenu
        price_to_income_ratio = None
        if avg_income > 0:
            price_to_income_ratio = median_price / avg_income
            
        # Calculer l'indice d'accessibilité hypothécaire
        mortgage_affordability_index = None
        if avg_income > 0 and median_price > 0:
            # Calcul simplifié basé sur la mensualité d'un prêt immobilier
            down_payment = median_price * self.affordability_params['down_payment_percentage']
            loan_amount = median_price - down_payment
            monthly_income = avg_income / 12
            
            # Calculer le paiement mensuel approximatif (formule simplifiée)
            r = self.affordability_params['mortgage_rate'] / 12
            n = self.affordability_params['mortgage_term_years'] * 12
            monthly_payment = loan_amount * (r * (1 + r) ** n) / ((1 + r) ** n - 1)
            
            # Calculer le ratio de service de la dette
            debt_service_ratio = monthly_payment / monthly_income
            
            # Calculer l'indice d'accessibilité (1 = parfaitement abordable, 0 = inaccessible)
            mortgage_affordability_index = 1 - (debt_service_ratio / self.affordability_params['debt_service_ratio_max'])
            mortgage_affordability_index = max(0, min(1, mortgage_affordability_index))
            
        # Calculer un score d'accessibilité à la propriété
        ownership_accessibility_score = None
        if price_to_income_ratio and mortgage_affordability_index:
            # Score sur 100, combinant le ratio prix/revenu et l'indice d'accessibilité hypothécaire
            pti_component = max(0, 100 - (price_to_income_ratio - 3) * 15)  # Moins élevé est meilleur
            mai_component = mortgage_affordability_index * 100
            
            ownership_accessibility_score = (pti_component * 0.6) + (mai_component * 0.4)
            ownership_accessibility_score = max(0, min(100, ownership_accessibility_score))
            
        return {
            "price_to_income_ratio": price_to_income_ratio,
            "mortgage_affordability_index": mortgage_affordability_index,
            "ownership_accessibility_score": ownership_accessibility_score
        }
        
    def process_rental_market_potential(self, real_estate_data: Dict[str, Any], economic_data: Dict[str, Any], demographic_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Évalue le potentiel du marché locatif.
        
        Args:
            real_estate_data: Données du marché immobilier.
            economic_data: Données économiques.
            demographic_data: Données démographiques.
            
        Returns:
            dict: Section rental_market_potential du JSON.
        """
        # Extraire les prix immobiliers
        median_price = 0
        if real_estate_data and 'municipality_overview' in real_estate_data:
            price_trends = real_estate_data['municipality_overview'].get('last_period', {}).get('price_trends', {})
            median_price = price_trends.get('median_price', 0)
            
        # Estimer les loyers basés sur le rendement brut par défaut
        estimated_rental_yield = self.rental_yield_params['default_gross_yield']
        
        # Ajuster le rendement en fonction des facteurs économiques et démographiques
        if economic_data and 'unemployment' in economic_data:
            unemployment_rate = economic_data['unemployment'].get('overall_rate', 0)
            
            # Ajustement basé sur le chômage (plus de chômage = rendement potentiellement plus bas)
            if unemployment_rate > THRESHOLDS['high_unemployment']:
                estimated_rental_yield -= 0.005
            elif unemployment_rate < THRESHOLDS['high_unemployment'] / 2:
                estimated_rental_yield += 0.003
                
        # Évaluer l'indice de demande locative
        rental_demand_index = 0.5  # Valeur par défaut
        
        if demographic_data and 'population_overview' in demographic_data:
            population_trend = demographic_data['population_overview'].get('population_trend', {})
            five_year_growth = population_trend.get('five_year_growth', 0)
            
            # Ajustement basé sur la croissance démographique
            if five_year_growth > 5:
                rental_demand_index += 0.2
                estimated_rental_yield += 0.003
            elif five_year_growth > 2:
                rental_demand_index += 0.1
                estimated_rental_yield += 0.001
            elif five_year_growth < 0:
                rental_demand_index -= 0.1
                estimated_rental_yield -= 0.001
                
        # Déterminer le potentiel de croissance des loyers
        rental_growth_potential = "Moderate"
        
        if rental_demand_index > 0.7:
            rental_growth_potential = "Strong"
        elif rental_demand_index > 0.6:
            rental_growth_potential = "Good"
        elif rental_demand_index < 0.4:
            rental_growth_potential = "Weak"
        elif rental_demand_index < 0.3:
            rental_growth_potential = "Very weak"
            
        return {
            "estimated_rental_yield": estimated_rental_yield,
            "rental_demand_index": rental_demand_index,
            "rental_growth_potential": rental_growth_potential
        }
        
    def process_target_demographic_analysis(self, demographic_data: Dict[str, Any], real_estate_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyse les segments démographiques cibles pour l'investissement immobilier.
        
        Args:
            demographic_data: Données démographiques.
            real_estate_data: Données du marché immobilier.
            
        Returns:
            dict: Section target_demographic_analysis du JSON.
        """
        growing_segments = []
        declining_segments = []
        
        if not demographic_data:
            logger.warning("Données démographiques manquantes pour l'analyse des segments")
            
            # Fournir des données par défaut
            growing_segments = [
                {
                    "segment": "Seniors (65+)",
                    "growth_rate": 2.4,
                    "housing_preferences": "Smaller, accessible units close to amenities"
                },
                {
                    "segment": "Young professionals",
                    "growth_rate": 1.2,
                    "housing_preferences": "Modern apartments with good connectivity"
                }
            ]
            
            declining_segments = [
                {
                    "segment": "Families with 3+ children",
                    "decline_rate": -0.8,
                    "housing_impact": "Reduced demand for large houses"
                }
            ]
        else:
            # Analyser les données démographiques pour identifier les segments en croissance/déclin
            age_structure = demographic_data.get('age_structure', {})
            age_groups = age_structure.get('age_groups', {})
            
            for age_key, age_data in age_groups.items():
                trend = age_data.get('trend')
                if not trend:
                    continue
                    
                # Convertir la tendance formatée en valeur numérique
                trend_value = None
                try:
                    if trend.startswith('+'):
                        trend_value = float(trend[1:-1])  # Enlever le + et le %
                    else:
                        trend_value = float(trend[:-1])  # Enlever le %
                except:
                    continue
                    
                # Déterminer les préférences de logement par segment
                housing_preferences = ""
                if age_key == "under_18":
                    continue  # Les enfants ne sont pas des acheteurs/locataires directs
                elif age_key == "18_to_35":
                    housing_preferences = "Modern apartments or starter homes, proximity to employment centers and transportation"
                elif age_key == "36_to_65":
                    housing_preferences = "Family homes with space, good school districts, established neighborhoods"
                elif age_key == "over_65":
                    housing_preferences = "Single-level homes, smaller maintenance-free units, access to healthcare and amenities"
                    
                # Classer comme segment en croissance ou en déclin
                if trend_value >= 1.0:
                    growing_segments.append({
                        "segment": self.get_age_segment_name(age_key),
                        "growth_rate": trend_value,
                        "housing_preferences": housing_preferences
                    })
                elif trend_value <= -1.0:
                    declining_segments.append({
                        "segment": self.get_age_segment_name(age_key),
                        "decline_rate": trend_value,
                        "housing_impact": self.get_housing_impact(age_key, trend_value)
                    })
                    
            # Si aucun segment n'a été identifié, ajouter des données par défaut basées sur les tendances nationales
            if not growing_segments:
                growing_segments.append({
                    "segment": "Seniors (65+)",
                    "growth_rate": 2.0,
                    "housing_preferences": "Smaller, accessible units close to amenities"
                })
                
        return {
            "growing_segments": growing_segments,
            "declining_segments": declining_segments
        }
        
    def get_age_segment_name(self, age_key: str) -> str:
        """
        Convertit une clé de groupe d'âge en nom de segment plus lisible.
        
        Args:
            age_key: Clé du groupe d'âge.
            
        Returns:
            str: Nom du segment.
        """
        if age_key == "under_18":
            return "Children & Teenagers"
        elif age_key == "18_to_35":
            return "Young adults & Early professionals"
        elif age_key == "36_to_65":
            return "Middle-aged families & Established professionals"
        elif age_key == "over_65":
            return "Seniors (65+)"
        else:
            return age_key
            
    def get_housing_impact(self, age_key: str, trend_value: float) -> str:
        """
        Détermine l'impact sur le marché immobilier d'un déclin de segment.
        
        Args:
            age_key: Clé du groupe d'âge.
            trend_value: Valeur de la tendance.
            
        Returns:
            str: Description de l'impact.
        """
        if age_key == "under_18":
            return "Future reduction in demand for family homes"
        elif age_key == "18_to_35":
            return "Decreasing demand for starter homes and rental properties"
        elif age_key == "36_to_65":
            return "Reduced demand for larger family homes and mid-range properties"
        elif age_key == "over_65":
            return "Potential oversupply of retirement/senior-focused properties"
        else:
            return "Shifting housing demand patterns"
            
    def process_market_dynamics(self, real_estate_data: Dict[str, Any], building_dev_data: Dict[str, Any], demographic_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyse les dynamiques du marché immobilier.
        
        Args:
            real_estate_data: Données du marché immobilier.
            building_dev_data: Données du développement immobilier.
            demographic_data: Données démographiques.
            
        Returns:
            dict: Section market_dynamics du JSON.
        """
        # Évaluer l'équilibre offre/demande
        supply_demand_balance = "Balanced"
        
        # Données sur les permis de construire (offre future)
        new_residential_units = 0
        if building_dev_data and 'permits' in building_dev_data:
            permits_counts = building_dev_data['permits'].get('counts', {})
            new_residential_units = permits_counts.get('residential', {}).get('new_construction', {}).get('dwellings', 0)
            
        # Données sur la croissance démographique (demande)
        population_growth_rate = 0
        if demographic_data:
            population_trend = demographic_data.get('population_overview', {}).get('population_trend', {})
            population_growth_rate = population_trend.get('one_year_growth', 0)
            
        # Calculer le taux d'absorption vs construction
        construction_vs_absorption_rate = 1.0  # Valeur par défaut: équilibre
        
        # Estimer l'absorption annuelle basée sur la croissance démographique
        # (simplification: chaque 2.5 nouveaux habitants = 1 nouveau ménage)
        population = demographic_data.get('population_overview', {}).get('total_population', 0) if demographic_data else 0
        estimated_new_households = 0
        
        if population > 0 and population_growth_rate:
            new_population = population * population_growth_rate / 100
            estimated_new_households = new_population / 2.5
            
            if estimated_new_households > 0:
                construction_vs_absorption_rate = new_residential_units / estimated_new_households
                
        # Déterminer l'équilibre offre/demande
        if construction_vs_absorption_rate > 1.25:
            supply_demand_balance = "Oversupply"
        elif construction_vs_absorption_rate > 1.1:
            supply_demand_balance = "Slight oversupply"
        elif construction_vs_absorption_rate < 0.75:
            supply_demand_balance = "Undersupply"
        elif construction_vs_absorption_rate < 0.9:
            supply_demand_balance = "Slight undersupply"
            
        # Déterminer la position dans le cycle du marché
        market_cycle_position = "Mid-growth phase"
        
        # Analyser les prix et les transactions pour déterminer la phase du cycle
        if real_estate_data and 'municipality_overview' in real_estate_data:
            trends = real_estate_data['municipality_overview'].get('historical_trends', {})
            price_change = trends.get('year_over_year', {}).get('price_change_pct', 0)
            transaction_change = trends.get('year_over_year', {}).get('transaction_change_pct', 0)
            
            if price_change > 10 and transaction_change > 5:
                market_cycle_position = "Strong growth phase"
            elif price_change > 5 and transaction_change > 0:
                market_cycle_position = "Growth phase"
            elif price_change < -5 and transaction_change < -5:
                market_cycle_position = "Correction phase"
            elif price_change < 0 and transaction_change < 0:
                market_cycle_position = "Slowdown phase"
            elif price_change > 0 and transaction_change < 0:
                market_cycle_position = "Late growth phase (pre-correction)"
            elif abs(price_change) < 2 and abs(transaction_change) < 3:
                market_cycle_position = "Stabilization phase"
                
        return {
            "supply_demand_balance": supply_demand_balance,
            "construction_vs_absorption_rate": construction_vs_absorption_rate,
            "market_cycle_position": market_cycle_position
        }
        
    def process_risk_assessment(self, real_estate_data: Dict[str, Any], economic_data: Dict[str, Any], building_dev_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Évalue les risques liés à l'investissement immobilier.
        
        Args:
            real_estate_data: Données du marché immobilier.
            economic_data: Données économiques.
            building_dev_data: Données du développement immobilier.
            
        Returns:
            dict: Section risk_assessment du JSON.
        """
        # Évaluer la volatilité des prix
        price_volatility = "Medium"
        
        if real_estate_data and 'municipality_overview' in real_estate_data:
            trends = real_estate_data['municipality_overview'].get('historical_trends', {})
            price_change_1y = trends.get('year_over_year', {}).get('price_change_pct', 0)
            price_change_5y = trends.get('five_year', {}).get('price_change_pct', 0)
            
            # Calculer la volatilité comme la différence entre les tendances à court et long terme
            volatility_indicator = abs(price_change_1y - (price_change_5y / 5))
            
            if volatility_indicator < 3:
                price_volatility = "Low"
            elif volatility_indicator > 7:
                price_volatility = "High"
                
        # Évaluer le score de liquidité
        liquidity_score = 50  # Valeur par défaut
        
        if real_estate_data and 'municipality_overview' in real_estate_data:
            last_period = real_estate_data['municipality_overview'].get('last_period', {})
            transactions = last_period.get('total_transactions', 0)
            
            # Ajuster le score de liquidité selon le volume de transactions
            if transactions > 100:
                liquidity_score += 20
            elif transactions > 50:
                liquidity_score += 10
            elif transactions < 20:
                liquidity_score -= 10
            elif transactions < 10:
                liquidity_score -= 20
                
            # Ajuster en fonction de l'évolution des transactions
            trends = real_estate_data['municipality_overview'].get('historical_trends', {})
            transaction_change = trends.get('year_over_year', {}).get('transaction_change_pct', 0)
            
            if transaction_change > 10:
                liquidity_score += 10
            elif transaction_change > 5:
                liquidity_score += 5
            elif transaction_change < -10:
                liquidity_score -= 10
            elif transaction_change < -5:
                liquidity_score -= 5
                
        # Borner le score entre 0 et 100
        liquidity_score = max(0, min(100, liquidity_score))
        
        # Évaluer le risque de suroffre
        oversupply_risk = "Medium"
        
        if building_dev_data and 'construction_activity' in building_dev_data:
            supply_pipeline = building_dev_data['construction_activity'].get('supply_pipeline', {})
            impact = supply_pipeline.get('impact_on_supply', 'Medium')
            
            if impact == "High":
                oversupply_risk = "High"
            elif impact == "Low":
                oversupply_risk = "Low"
                
        # Évaluer le risque de dépendance économique
        economic_dependency_risk = "Medium"
        
        if economic_data and 'business_activity' in economic_data:
            sectors = economic_data['business_activity'].get('sectors', {})
            
            # Calculer la concentration par secteur
            largest_sector_pct = 0
            for sector_data in sectors.values():
                enterprise_count = sector_data.get('enterprise_count', 0)
                total_enterprises = economic_data['business_activity'].get('enterprise_overview', {}).get('total_enterprises', 0)
                if total_enterprises > 0:
                    sector_pct = (enterprise_count / total_enterprises) * 100
                    largest_sector_pct = max(largest_sector_pct, sector_pct)
                    
            # Évaluer le risque selon la concentration
            if largest_sector_pct > 40:
                economic_dependency_risk = "High"
            elif largest_sector_pct < 25:
                economic_dependency_risk = "Low"
                
        return {
            "price_volatility": price_volatility,
            "liquidity_score": liquidity_score,
            "oversupply_risk": oversupply_risk,
            "economic_dependency_risk": economic_dependency_risk
        }
        
    def process_comparative_ranking(self, real_estate_data: Dict[str, Any], economic_data: Dict[str, Any], demographic_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Génère des classements comparatifs pour l'investissement immobilier.
        
        Args:
            real_estate_data: Données du marché immobilier.
            economic_data: Données économiques.
            demographic_data: Données démographiques.
            
        Returns:
            dict: Section comparative_ranking du JSON.
        """
        # Notes: Dans une implémentation complète, ces classements seraient basés sur la comparaison
        # avec d'autres communes. Ici, nous utilisons des scores calculés sur une échelle de 1 à 100
        # qui pourraient être convertis en rangs relatifs dans une phase ultérieure.
        
        # Calculer le score de rapport qualité/prix
        value_for_money_score = 50  # Valeur par défaut
        
        if real_estate_data and economic_data:
            # Extraire le prix médian
            median_price = real_estate_data.get('municipality_overview', {}).get('last_period', {}).get('price_trends', {}).get('median_price', 0)
            
            # Extraire le revenu moyen
            avg_income = economic_data.get('income_tax', {}).get('income_overview', {}).get('average_income', 0)
            
            if median_price > 0 and avg_income > 0:
                # Calculer le ratio prix/revenu et ajuster le score
                price_to_income_ratio = median_price / avg_income
                
                if price_to_income_ratio < 4:
                    value_for_money_score += 30
                elif price_to_income_ratio < 5:
                    value_for_money_score += 20
                elif price_to_income_ratio < 6:
                    value_for_money_score += 10
                elif price_to_income_ratio > 8:
                    value_for_money_score -= 20
                elif price_to_income_ratio > 7:
                    value_for_money_score -= 10
                    
        # Borner le score entre 1 et 100
        value_for_money_score = max(1, min(100, value_for_money_score))
        
        # Convertir en rang (ici simulé)
        value_for_money_rank = max(1, 101 - value_for_money_score)
        
        # Calculer le score de potentiel de croissance
        growth_potential_score = 50  # Valeur par défaut
        
        if real_estate_data and demographic_data:
            # Facteurs de croissance des prix
            price_change = real_estate_data.get('municipality_overview', {}).get('historical_trends', {}).get('year_over_year', {}).get('price_change_pct', 0)
            
            # Facteurs de croissance démographique
            population_growth = demographic_data.get('population_overview', {}).get('population_trend', {}).get('five_year_growth', 0)
            
            # Ajuster le score selon les facteurs de croissance
            if price_change > 5:
                growth_potential_score += 15
            elif price_change > 3:
                growth_potential_score += 10
            elif price_change < 0:
                growth_potential_score -= 10
                
            if population_growth > 5:
                growth_potential_score += 15
            elif population_growth > 2:
                growth_potential_score += 10
            elif population_growth < 0:
                growth_potential_score -= 10
                
        # Borner le score entre 1 et 100
        growth_potential_score = max(1, min(100, growth_potential_score))
        
        # Convertir en rang (ici simulé)
        growth_potential_rank = max(1, 101 - growth_potential_score)
        
        # Calculer le score global d'investissement
        overall_investment_score = (value_for_money_score * 0.4) + (growth_potential_score * 0.6)
        
        return {
            "value_for_money_rank": value_for_money_rank,
            "growth_potential_rank": growth_potential_rank,
            "overall_investment_score": overall_investment_score
        }
        
    def process_data(self, real_estate_data: Dict[str, Any], economic_data: Dict[str, Any], 
                   demographic_data: Optional[Dict[str, Any]] = None, 
                   building_dev_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Traite les données complètes pour l'analyse d'investissement.
        
        Args:
            real_estate_data: Données du marché immobilier.
            economic_data: Données économiques.
            demographic_data: Données démographiques.
            building_dev_data: Données du développement immobilier.
            
        Returns:
            dict: Section investment_analysis du JSON.
        """
        result = {
            "affordability_metrics": self.process_affordability_metrics(real_estate_data, economic_data),
            "rental_market_potential": self.process_rental_market_potential(real_estate_data, economic_data, demographic_data),
            "target_demographic_analysis": self.process_target_demographic_analysis(demographic_data, real_estate_data),
            "market_dynamics": self.process_market_dynamics(real_estate_data, building_dev_data or {}, demographic_data),
            "risk_assessment": self.process_risk_assessment(real_estate_data, economic_data, building_dev_data),
            "comparative_ranking": self.process_comparative_ranking(real_estate_data, economic_data, demographic_data)
        }
        
        return result 

        
        
   
        
    
 