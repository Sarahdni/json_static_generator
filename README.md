# IMMO SCORE - Analyseur de Données Immobilières

## Description

IMMO SCORE est une plateforme qui permet de générer des rapports d'analyse détaillée du marché immobilier organisés par commune. Les rapports aident les investisseurs à comprendre le marché grâce aux données, rendant le processus d'investissement immobilier plus analytique et moins basé sur des suppositions.

Ce projet génère des fichiers JSON statiques par commune, qui pourront être transformés en rapports visuels ou servir de backend pour une API complète dans le futur.

## Fonctionnalités

- Extraction de données depuis une base PostgreSQL
- Analyse multiparamétrique du marché immobilier
- Génération de rapports au format JSON par commune
- Métriques d'investissement et indicateurs de performance
- Statistiques comparatives entre communes
- Analyse démographique et économique intégrée

## Structure du Projet

```
immo-score/
├── data/
│   └── output/                # JSON finaux par commune
│       ├── brabant_wallon/
│       ├── liege/
│       ├── hainaut/
│       ├── namur/
│       └── luxembourg/
├── src/
│   ├── config/                # Configuration
│   │   ├── __init__.py
│   │   ├── database.py        # Configuration PostgreSQL
│   │   └── settings.py        # Paramètres généraux
│   ├── extractors/            # Extraction de données
│   │   ├── __init__.py
│   │   ├── base.py            # Classe de base pour extraction
│   │   ├── real_estate.py     # Extraction marché immobilier
│   │   ├── building.py        # Extraction développement immobilier
│   │   ├── demographics.py    # Extraction démographie
│   │   └── economics.py       # Extraction économie
│   ├── processors/            # Traitement des données
│   │   ├── __init__.py
│   │   ├── base.py            # Classe de base pour traitement
│   │   ├── real_estate.py     # Traitement marché immobilier
│   │   ├── building_dev.py    # Traitement développement immobilier
│   │   ├── demographics.py    # Traitement démographie
│   │   ├── economics.py       # Traitement économie locale
│   │   └── investment.py      # Analyse d'investissement
│   ├── generators/            # Génération des rapports
│   │   ├── __init__.py
│   │   └── municipality.py    # Générateur de rapport par commune
│   └── utils/                 # Utilitaires
│       ├── __init__.py
│       ├── db_utils.py        # Utilitaires base de données
│       └── json_utils.py      # Utilitaires JSON
├── logs/                      # Logs d'exécution
├── main.py                    # Point d'entrée principal
├── requirements.txt           # Dépendances
└── README.md                  # Documentation
```

## Installation

1. Cloner le dépôt :
```bash
git clone https://github.com/votre-utilisateur/immo-score.git
cd immo-score
```

2. Créer un environnement virtuel Python :
```bash
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate
```

3. Installer les dépendances :
```bash
pip install -r requirements.txt
```

4. Configurer la base de données dans `src/config/database.py`.

## Utilisation

### Générer un rapport pour une commune spécifique

```bash
python main.py --commune [ID_COMMUNE]
```

### Générer des rapports pour toutes les communes d'une province

```bash
python main.py --province [NOM_PROVINCE]
```

### Paramètres avancés

```bash
# Spécifier des périodes personnalisées
python main.py --commune [ID_COMMUNE] --real-estate-period 2024-Q1 --economic-period 2023

# Générer des statistiques comparatives
python main.py --stats --province liege

# Fusionner tous les rapports d'une province
python main.py --merge-province [NOM_PROVINCE]

# Mode debug (logs détaillés)
python main.py --commune [ID_COMMUNE] --debug
```

## Structure des Données JSON

Chaque rapport JSON contient les sections suivantes :

- **metadata** : Informations sur la commune et les périodes de données
- **real_estate_market** : Analyse du marché immobilier (prix, transactions, types de biens)
- **building_development** : Analyse des permis de construire et du développement immobilier
- **demographics** : Analyse démographique (population, ménages, nationalités)
- **economic_indicators** : Indicateurs économiques (revenus, chômage, activité entrepreneuriale)
- **geographical_context** : Contexte géographique et mobilité
- **investment_analysis** : Métriques d'investissement et potentiel du marché

## Dépendances

- Python 3.8+
- SQLAlchemy
- psycopg2
- pandas
- numpy
- jsonschema

## Développement Futur

- Intégration d'une API REST pour accéder aux données
- Interface web pour visualiser les rapports
- Modèles prédictifs pour l'évolution des prix
- Comparaisons automatisées entre communes
- Assistant IA pour l'analyse des rapports

## Licence

Ce projet est sous licence [MIT](LICENSE).

## Auteurs

- Votre Nom - Développeur Principal - [votre-email@example.com](mailto:votre-email@example.com)

## Remerciements

- À toutes les sources de données qui rendent ce projet possible
- Aux contributeurs et testeurs du projet