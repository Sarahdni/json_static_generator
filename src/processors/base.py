"""
Classe de base pour tous les processeurs de données.
Fournit les fonctionnalités communes comme le formatage des nombres et le calcul des variations.
"""
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime

from src.config.settings import NUMBER_FORMAT, THRESHOLDS

# Configuration du logger
logger = logging.getLogger(__name__)

class BaseProcessor:
    """Classe de base pour tous les processeurs de données."""
    
    def __init__(self):
        """Initialise le processeur avec les paramètres par défaut."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    def format_number(self, value: Union[int, float], decimal_places: Optional[int] = None) -> str:
        """
        Formate un nombre selon les paramètres définis.
        
        Args:
            value: Valeur à formater.
            decimal_places: Nombre de décimales (si None, utilise la valeur par défaut).
            
        Returns:
            str: Nombre formaté.
        """
        if value is None:
            return "N/A"
            
        # Déterminer le nombre de décimales
        if decimal_places is None:
            decimal_places = NUMBER_FORMAT['decimal_places']
            
        # Formater le nombre
        formatted = f"{value:,.{decimal_places}f}"
        
        # Remplacer les séparateurs selon les paramètres
        if NUMBER_FORMAT['thousands_separator'] != ',':
            formatted = formatted.replace(',', NUMBER_FORMAT['thousands_separator'])
        if NUMBER_FORMAT['decimal_separator'] != '.':
            formatted = formatted.replace('.', NUMBER_FORMAT['decimal_separator'])
            
        return formatted
        
    def format_price(self, value: Union[int, float]) -> str:
        """
        Formate un prix selon les paramètres définis.
        
        Args:
            value: Prix à formater.
            
        Returns:
            str: Prix formaté.
        """
        if value is None:
            return "N/A"
            
        return self.format_number(value, NUMBER_FORMAT['price_decimal_places'])
        
    def format_percentage(self, value: Union[int, float], include_sign: bool = True) -> str:
        """
        Formate un pourcentage selon les paramètres définis.
        
        Args:
            value: Pourcentage à formater.
            include_sign: Si True, inclut le signe % dans le résultat.
            
        Returns:
            str: Pourcentage formaté.
        """
        if value is None:
            return "N/A"
            
        formatted = self.format_number(value, NUMBER_FORMAT['percentage_decimal_places'])
        
        if include_sign:
            formatted += "%"
            
        return formatted
        
    def calculate_change(self, current_value: Union[int, float], 
                         previous_value: Union[int, float]) -> Tuple[float, str]:
        """
        Calcule la variation entre deux valeurs.
        
        Args:
            current_value: Valeur actuelle.
            previous_value: Valeur précédente.
            
        Returns:
            tuple: (pourcentage de variation, variation formatée avec signe + ou -)
        """
        if current_value is None or previous_value is None or previous_value == 0:
            return None, "N/A"
            
        change = ((current_value - previous_value) / previous_value) * 100
        sign = "+" if change >= 0 else ""
        
        return change, f"{sign}{self.format_percentage(change)}"
        
    def calculate_avg(self, total: Union[int, float], count: int) -> Optional[float]:
        """
        Calcule une moyenne.
        
        Args:
            total: Somme des valeurs.
            count: Nombre de valeurs.
            
        Returns:
            float: Moyenne calculée.
        """
        if total is None or count is None or count == 0:
            return None
            
        return total / count
        
    def classify_trend(self, change_percentage: float, 
                      threshold_positive: float = 5.0, 
                      threshold_negative: float = -5.0) -> str:
        """
        Classifie une tendance en fonction de son pourcentage de variation.
        
        Args:
            change_percentage: Pourcentage de variation.
            threshold_positive: Seuil pour considérer une forte hausse.
            threshold_negative: Seuil pour considérer une forte baisse.
            
        Returns:
            str: Classification de la tendance ('strong_increase', 'increase', 'stable', 'decrease', 'strong_decrease').
        """
        if change_percentage is None:
            return "unknown"
            
        if change_percentage >= threshold_positive:
            return "strong_increase"
        elif change_percentage > 0:
            return "increase"
        elif change_percentage >= threshold_negative:
            return "stable"
        elif change_percentage >= threshold_negative * 2:
            return "decrease"
        else:
            return "strong_decrease"
        
    def classify_market_trend(self, price_change: float, transaction_change: float) -> str:
        """
        Classifie une tendance de marché en fonction des variations de prix et de transactions.
        
        Args:
            price_change: Variation des prix en pourcentage.
            transaction_change: Variation du nombre de transactions en pourcentage.
            
        Returns:
            str: Classification du marché ('hot', 'warm', 'balanced', 'cooling', 'cold', 'volatile', 'unknown').
        """
        if price_change is None or transaction_change is None:
            return "unknown"
            
        # Marché en hausse (prix et transactions augmentent)
        if self.is_numeric_and_greater_than(price_change, THRESHOLDS['high_price_growth']) and self.is_numeric_and_greater_than(transaction_change, 0):
            return "hot"
        elif self.is_numeric_and_greater_than(price_change, 0) and self.is_numeric_and_greater_than(transaction_change, 0):
            return "warm"
            
        # Marché équilibré (prix stables)
        elif abs(self.safe_numeric_value(price_change)) <= 2 and abs(self.safe_numeric_value(transaction_change)) <= 5:
            return "balanced"
            
        # Marché en baisse (prix baissent)
        elif self.is_numeric_and_less_than(price_change, THRESHOLDS['low_price_growth']) and self.is_numeric_and_less_than(transaction_change, 0):
            return "cold"
        elif self.is_numeric_and_less_than(price_change, 0) and self.is_numeric_and_less_than(transaction_change, 0):
            return "cooling"
            
        # Marché instable (tendances contradictoires)
        elif ((self.is_numeric_and_greater_than(price_change, 0) and self.is_numeric_and_less_than(transaction_change, 0)) or
            (self.is_numeric_and_less_than(price_change, 0) and self.is_numeric_and_greater_than(transaction_change, 0))):
            return "volatile"
            
        # Autres cas
        else:
            return "unknown"
            
    def get_current_date(self) -> str:
        """
        Retourne la date actuelle au format YYYY-MM-DD.
        
        Returns:
            str: Date actuelle formatée.
        """
        return datetime.now().strftime("%Y-%m-%d")
        
    def create_metadata(self, commune_data: Dict[str, Any], data_periods: Dict[str, str]) -> Dict[str, Any]:
        """
        Crée la section de métadonnées du JSON.
        
        Args:
            commune_data: Données de la commune.
            data_periods: Périodes de données par type.
            
        Returns:
            dict: Section metadata du JSON.
        """
        return {
            "commune_id": commune_data.get('commune_id'),
            "commune_name": commune_data.get('commune_name'),
            "postal_code": commune_data.get('postal_code'),
            "province": commune_data.get('province'),
            "region": commune_data.get('region'),
            "version": "1.0",
            "generated_date": self.get_current_date(),
            "data_period": data_periods
        }
        
    def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Méthode principale pour traiter les données.
        À implémenter dans les classes enfants.
        
        Args:
            data: Données brutes à traiter.
            
        Returns:
            dict: Données traitées au format JSON.
        """
        raise NotImplementedError("La méthode process_data doit être implémentée dans les classes enfants.")