import psycopg2
from src.config.database import DATABASE_URL

def check_database_schema():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Vérifier les tables essentielles
    tables = [
        "dim_geography", "dim_age", "dim_sex", "dim_nationality", 
        "fact_population_structure", "fact_real_estate_municipality", "dim_age_group"
    ]
    
    print("=== Diagnostic de la base de données ===")
    
    for table in tables:
        print(f"\nTable: {table}")
        try:
            # Vérifier si la table existe
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' AND table_schema = 'dw')")
            exists = cursor.fetchone()[0]
            
            if not exists:
                print(f"  ERREUR: Table {table} n'existe pas!")
                continue
                
            # Lister les colonnes
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = 'dw'")
            columns = cursor.fetchall()
            
            print("  Colonnes:")
            for col_name, col_type in columns:
                print(f"    - {col_name} ({col_type})")
                
            # Compter les enregistrements
            cursor.execute(f"SELECT COUNT(*) FROM dw.{table}")
            count = cursor.fetchone()[0]
            print(f"  Nombre d'enregistrements: {count}")
            
        except Exception as e:
            print(f"  ERREUR: {str(e)}")
    
    conn.close()

if __name__ == "__main__":
    check_database_schema()