"""
Configuration de la connexion à la base de données PostgreSQL.
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Configuration de la connexion à PostgreSQL
DB_USERNAME = "sarahdinari"
DB_PASSWORD = "hellodata"
DB_NAME = "belgian_data"
DB_HOST = "localhost"
DB_PORT = "5432"
DEFAULT_SCHEMA = "dw"  # Schéma data warehouse

# Création de l'URL de connexion
DATABASE_URL = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Création du moteur SQLAlchemy
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Vérifie que la connexion est active avant utilisation
    connect_args={"options": f"-c search_path={DEFAULT_SCHEMA}"}  # Définit le schéma par défaut
)

# Création d'une session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base pour les modèles déclaratifs
Base = declarative_base()

def get_db():
    """
    Génère une session de base de données à utiliser dans les extracteurs.
    Gère automatiquement la fermeture de la session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def execute_raw_query(query, params=None):
    """
    Exécute une requête SQL brute et retourne les résultats.
    
    Args:
        query (str): Requête SQL à exécuter
        params (dict, optional): Paramètres à injecter dans la requête
        
    Returns:
        list: Liste de dictionnaires représentant les résultats
    """
    with engine.connect() as connection:
        result = connection.execute(query, params or {})
        return [dict(row) for row in result]