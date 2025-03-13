"""
Utilitaires pour la manipulation des fichiers JSON.
Fournit des fonctions pour valider, formater et écrire des fichiers JSON.
"""
import json
import os
import logging
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
import jsonschema
from datetime import datetime

from src.config.settings import JSON_FORMAT

logger = logging.getLogger(__name__)

def format_json(data: Dict[str, Any]) -> str:
    """
    Formate les données en JSON selon les paramètres définis.
    
    Args:
        data: Données à formatter.
        
    Returns:
        str: Chaîne JSON formatée.
    """
    return json.dumps(
        data,
        indent=JSON_FORMAT.get('indent', 2),
        ensure_ascii=JSON_FORMAT.get('ensure_ascii', False),
        sort_keys=JSON_FORMAT.get('sort_keys', False)
    )

def save_json(data: Dict[str, Any], output_path: str) -> bool:
    """
    Sauvegarde les données JSON dans un fichier.
    
    Args:
        data: Données à sauvegarder.
        output_path: Chemin du fichier de sortie.
        
    Returns:
        bool: True si l'opération a réussi, False sinon.
    """
    try:
        # Créer le répertoire parent s'il n'existe pas
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Formater et sauvegarder le JSON
        json_string = format_json(data)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(json_string)
            
        logger.info(f"Fichier JSON sauvegardé avec succès: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde du fichier JSON {output_path}: {str(e)}")
        return False

def load_json(file_path: str) -> Dict[str, Any]:
    """
    Charge un fichier JSON.
    
    Args:
        file_path: Chemin du fichier JSON à charger.
        
    Returns:
        dict: Données chargées du fichier JSON.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Erreur lors du chargement du fichier JSON {file_path}: {str(e)}")
        return {}

def validate_json_structure(data: Dict[str, Any], schema_path: Optional[str] = None) -> bool:
    """
    Valide la structure d'un JSON par rapport à un schéma.
    
    Args:
        data: Données JSON à valider.
        schema_path: Chemin vers le fichier de schéma JSON. Si None, utilise un schéma par défaut.
        
    Returns:
        bool: True si la validation réussit, False sinon.
    """
    try:
        # Si aucun schéma n'est fourni, utiliser un schéma minimal
        if not schema_path:
            schema = {
                "type": "object",
                "required": ["metadata", "real_estate_market", "demographics", "economic_indicators"],
                "properties": {
                    "metadata": {
                        "type": "object",
                        "required": ["commune_id", "commune_name", "generated_date"]
                    }
                }
            }
        else:
            # Charger le schéma à partir du fichier
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
        
        # Valider le JSON
        jsonschema.validate(data, schema)
        return True
        
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Erreur de validation JSON: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Erreur lors de la validation du JSON: {str(e)}")
        return False

def get_files_by_province(output_dir: str) -> Dict[str, List[str]]:
    """
    Récupère les fichiers JSON générés, organisés par province.
    
    Args:
        output_dir: Répertoire de base contenant les sous-répertoires par province.
        
    Returns:
        dict: Dictionnaire avec les provinces comme clés et les listes de fichiers comme valeurs.
    """
    result = {}
    
    try:
        # Parcourir les répertoires de provinces
        for province_dir in os.listdir(output_dir):
            province_path = os.path.join(output_dir, province_dir)
            
            # Vérifier si c'est un répertoire
            if os.path.isdir(province_path):
                # Récupérer les fichiers JSON
                json_files = [f for f in os.listdir(province_path) if f.endswith('.json')]
                
                # Ajouter à notre résultat
                result[province_dir] = json_files
                
        return result
        
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des fichiers par province: {str(e)}")
        return {}

def merge_json_files(files: List[str], output_file: str) -> bool:
    """
    Fusionne plusieurs fichiers JSON en un seul.
    
    Args:
        files: Liste des chemins de fichiers JSON à fusionner.
        output_file: Chemin du fichier de sortie.
        
    Returns:
        bool: True si l'opération a réussi, False sinon.
    """
    try:
        merged_data = []
        
        # Charger et fusionner les données
        for file_path in files:
            data = load_json(file_path)
            if data:
                merged_data.append(data)
                
        # Sauvegarder le résultat
        if merged_data:
            result = {
                "metadata": {
                    "description": "Fusion de plusieurs rapports communaux",
                    "generated_date": datetime.now().strftime("%Y-%m-%d"),
                    "source_files": len(merged_data)
                },
                "communes": merged_data
            }
            
            return save_json(result, output_file)
            
        return False
        
    except Exception as e:
        logger.error(f"Erreur lors de la fusion des fichiers JSON: {str(e)}")
        return False

def extract_section(file_path: str, section: str) -> Dict[str, Any]:
    """
    Extrait une section spécifique d'un fichier JSON.
    
    Args:
        file_path: Chemin du fichier JSON.
        section: Nom de la section à extraire.
        
    Returns:
        dict: Données de la section extraite.
    """
    try:
        data = load_json(file_path)
        if section in data:
            return data[section]
        else:
            logger.warning(f"Section '{section}' non trouvée dans le fichier {file_path}")
            return {}
            
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction de la section {section} du fichier {file_path}: {str(e)}")
        return {}

def find_extreme_values(files: List[str], metrics: List[str]) -> Dict[str, Dict[str, Dict[str, Union[float, str]]]]:
    """
    Trouve les valeurs extrêmes (min/max) pour certaines métriques dans un ensemble de fichiers JSON.
    
    Args:
        files: Liste des chemins de fichiers JSON à analyser.
        metrics: Liste des chemins de métriques à extraire (format: "section.subsection.metric").
        
    Returns:
        dict: Dictionnaire avec les métriques et leurs valeurs extrêmes.
    """
    try:
        result = {}
        
        # Initialiser le résultat
        for metric_path in metrics:
            result[metric_path] = {
                "min": {"value": float('inf'), "commune": ""},
                "max": {"value": float('-inf'), "commune": ""}
            }
            
        # Analyser chaque fichier
        for file_path in files:
            try:
                data = load_json(file_path)
                commune_name = data.get("metadata", {}).get("commune_name", "Inconnu")
                
                # Extraire chaque métrique
                for metric_path in metrics:
                    # Diviser le chemin en sections
                    sections = metric_path.split('.')
                    
                    # Naviguer dans le JSON
                    curr = data
                    valid = True
                    
                    for section in sections:
                        if section in curr:
                            curr = curr[section]
                        else:
                            valid = False
                            break
                            
                    # Si la métrique est valide et numérique
                    if valid and isinstance(curr, (int, float)):
                        # Vérifier les min/max
                        if curr < result[metric_path]["min"]["value"]:
                            result[metric_path]["min"]["value"] = curr
                            result[metric_path]["min"]["commune"] = commune_name
                            
                        if curr > result[metric_path]["max"]["value"]:
                            result[metric_path]["max"]["value"] = curr
                            result[metric_path]["max"]["commune"] = commune_name
                            
            except Exception as inner_e:
                logger.warning(f"Erreur lors de l'analyse du fichier {file_path}: {str(inner_e)}")
                continue
                
        # Finaliser le résultat
        for metric_path in metrics:
            # Si aucune valeur n'a été trouvée
            if result[metric_path]["min"]["value"] == float('inf'):
                result[metric_path]["min"]["value"] = None
                
            if result[metric_path]["max"]["value"] == float('-inf'):
                result[metric_path]["max"]["value"] = None
                
        return result
        
    except Exception as e:
        logger.error(f"Erreur lors de la recherche des valeurs extrêmes: {str(e)}")
        return {}