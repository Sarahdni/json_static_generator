"""
Classe de base pour tous les extracteurs de données.
Fournit les fonctionnalités communes comme la connexion à la base de données et la gestion des erreurs.
"""
import logging
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from src.config.database import SessionLocal, execute_raw_query
from src.config.settings import LOG_LEVEL, LOG_FORMAT

# Configuration du logger
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class BaseExtractor:
    """Classe de base pour tous les extracteurs de données."""
    
    def __init__(self, commune_id=None, province=None, period=None):
        """
        Initialise l'extracteur avec les paramètres de base.
        
        Args:
            commune_id (str, optional): Identifiant de la commune. Si None, extrait pour toutes les communes.
            province (str, optional): Province à extraire. Si None, extrait pour toutes les provinces.
            period (dict, optional): Périodes d'extraction pour différents types de données.
        """
        self.commune_id = commune_id
        self.province = province
        self.period = period or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    @contextmanager
    def get_db_session(self):
        """
        Crée une session de base de données et la ferme automatiquement après utilisation.
        
        Yields:
            Session: Session SQLAlchemy.
        """
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Erreur lors de l'interaction avec la base de données: {str(e)}")
            raise
        finally:
            session.close()
            
    def execute_query(self, query, params=None):
        """
        Exécute une requête SQL brute et gère les erreurs.
        
        Args:
            query (str): Requête SQL à exécuter.
            params (dict, optional): Paramètres à injecter dans la requête.
            
        Returns:
            list: Liste de dictionnaires représentant les résultats.
        """
        try:
            # Convertir la requête en objet text() SQL
            from sqlalchemy import text
            sql = text(query)
            
            # Utiliser engine directement pour exécuter
            with self.get_db_session() as session:
                result = session.execute(sql, params or {})
                return [dict(zip(result.keys(), row)) for row in result]
        except Exception as e:
            self.logger.error(f"Erreur lors de l'exécution de la requête: {str(e)}")
            self.logger.debug(f"Requête: {query}")
            self.logger.debug(f"Paramètres: {params}")
            raise
            
    def get_communes(self, session):
        """
        Récupère la liste des communes à traiter.
        
        Args:
            session (Session): Session SQLAlchemy.
            
        Returns:
            list: Liste de dictionnaires contenant les informations des communes.
        """
        try:
            # Construire une requête simple
            query = """
                SELECT 
                    id_geography AS commune_id,
                    tx_name_fr AS commune_name,
                    cd_lau AS postal_code,
                    'Région wallonne' AS region,
                    'Non spécifiée' AS province
                FROM dw.dim_geography
                WHERE cd_level = 4
                    AND fl_current = true
            """
            
            # Ajouter le filtre pour la commune spécifique si nécessaire
            if self.commune_id:
                query += f" AND id_geography = {self.commune_id}"
            
            # Exécuter la requête en mode texte brut
            from sqlalchemy import text
            result = session.execute(text(query))
            
            # Convertir les résultats en liste de dictionnaires
            communes = []
            for row in result:
                commune = {
                    'commune_id': row.commune_id,
                    'commune_name': row.commune_name,
                    'postal_code': row.postal_code,
                    'region': row.region,
                    'province': row.province
                }
                communes.append(commune)
            
            return communes
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des communes: {str(e)}")
            raise
            
    def get_date_id(self, session, period, type_data):
        """
        Récupère l'ID de date correspondant à une période spécifiée.
        
        Args:
            session (Session): Session SQLAlchemy.
            period (str): Période (format: YYYY ou YYYY-QN).
            type_data (str): Type de données ('year', 'quarter', etc.).
            
        Returns:
            int: ID de la date.
        """
        try:
            # Pour les périodes annuelles (ex: '2023')
            if len(period) == 4 and period.isdigit():
                query = """
                    SELECT id_date
                    FROM dw.dim_date
                    WHERE cd_year = :year
                    AND cd_quarter IS NULL
                    AND cd_month IS NULL
                """
                params = {'year': int(period)}
            
            # Pour les périodes trimestrielles (ex: '2023-Q2')
            elif len(period) == 7 and period[4:5] == '-' and period[5:6] == 'Q' and period[6:7].isdigit():
                year = period[:4]
                quarter = period[6:7]
                query = """
                    SELECT id_date
                    FROM dw.dim_date
                    WHERE cd_year = :year
                    AND cd_quarter = :quarter
                    AND cd_month IS NULL
                """
                params = {'year': int(year), 'quarter': int(quarter)}
            
            # Format non reconnu
            else:
                self.logger.error(f"Format de période non reconnu: {period}")
                return None
                
            result = self.execute_query(query, params)
            if result and len(result) > 0:
                return result[0]['id_date']
            else:
                self.logger.warning(f"Aucune date trouvée pour la période {period}")
                return None
                
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération de l'ID de date: {str(e)}")
            raise
            
    def extract_data(self):
        """
        Méthode principale pour extraire les données.
        À implémenter dans les classes enfants.
        
        Returns:
            dict: Données extraites.
        """
        raise NotImplementedError("La méthode extract_data doit être implémentée dans les classes enfants.")
        
    def log_extraction_start(self, data_type):
        """
        Enregistre le début d'une extraction.
        
        Args:
            data_type (str): Type de données en cours d'extraction.
        """
        self.logger.info(f"Début de l'extraction des données {data_type}" + 
                        (f" pour la commune {self.commune_id}" if self.commune_id else "") +
                        (f" dans la province {self.province}" if self.province else ""))
        
    def log_extraction_end(self, data_type, count):
        """
        Enregistre la fin d'une extraction.
        
        Args:
            data_type (str): Type de données extraites.
            count (int): Nombre d'éléments extraits.
        """
        self.logger.info(f"Extraction terminée: {count} entrées de {data_type} extraites" + 
                        (f" pour la commune {self.commune_id}" if self.commune_id else "") +
                        (f" dans la province {self.province}" if self.province else ""))