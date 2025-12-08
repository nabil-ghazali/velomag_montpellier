# from database.schemas import Database
# import os
# from dotenv import load_dotenv

# load_dotenv()

# USER = os.getenv("user")
# PASSWORD = os.getenv("password")
# HOST = os.getenv("host")
# PORT = os.getenv("port")
# DBNAME = os.getenv("dbname")

# DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

# db = Database(DATABASE_URL)
# velo_clean = db.pull_data("velo_clean")
# meteo_clean = db.pull_data("meteo_clean")

# import pandas as pd
# from database.schemas import Database
# import os
# from dotenv import load_dotenv

# # 1. Charger la configuration
# load_dotenv()

# USER = os.getenv("user")
# PASSWORD = os.getenv("password")
# HOST = os.getenv("host")
# PORT = os.getenv("port")
# DBNAME = os.getenv("dbname")

# DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

# # 2. Connexion
# print(" Connexion à la base de données...")
# db = Database(DATABASE_URL)

# # 3. Téléchargement et Sauvegarde
# print(" Téléchargement des données vélos...")
# df_velo = db.pull_data("velo_clean")
# df_velo.to_csv("data_files/viaBDD/mes_donnees_velo.csv", index=False, sep=";") # Séparateur ; pour Excel en France
# print(f" 'data_files/viaBDD/mes_donnees_velo.csv' créé ({len(df_velo)} lignes)")

# print(" Téléchargement de la météo...")
# df_meteo = db.pull_data("meteo_clean")
# df_meteo.to_csv("data_files/viaBDD/mes_donnees_meteo.csv", index=False, sep=";")
# print(f" 'data_files/viaBDD/mes_donnees_meteo.csv' créé ({len(df_meteo)} lignes)")

import os
import pandas as pd
from dotenv import load_dotenv
from database.schemas import Database

load_dotenv()

class FeatureEngineering:
    def __init__(self):
        # Connexion à la base de données
        self.user = os.getenv("user")
        self.password = os.getenv("password")
        self.host = os.getenv("host")
        self.port = os.getenv("port")
        self.dbname = os.getenv("dbname")
        
        # URL de connexion
        self.database_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}?sslmode=require"
        self.db = Database(self.database_url)

    def create_dataset(self) -> pd.DataFrame:
        print(" Chargement des données depuis Supabase...")
        
        # 1. Récupération des tables
        df_velo = self.db.pull_data("velo_clean")
        df_meteo = self.db.pull_data("meteo_clean")

        # 2. Conversion des dates
        df_velo['datetime'] = pd.to_datetime(df_velo['datetime'])
        df_meteo['datetime'] = pd.to_datetime(df_meteo['datetime'])

        # 3. Création d'une clé commune (Date sans l'heure) pour la fusion
        df_velo['date_only'] = df_velo['datetime'].dt.normalize()
        df_meteo['date_only'] = df_meteo['datetime'].dt.normalize()

        # 4. Fusion (Merge) : On ajoute la météo en face de chaque heure de vélo
        print(" Fusion des données Vélos et Météo...")
        df_final = pd.merge(
            df_velo,
            df_meteo,
            on='date_only',
            how='left', # On garde tous les vélos, même si la météo manque
            suffixes=('', '_meteo')
        )

        # 5. Nettoyage final
        # On enlève les lignes où il manque la température (si historique météo incomplet)
        df_final = df_final.dropna(subset=['temperature_2m_max'])
        
        # On retire les colonnes techniques inutiles pour l'IA
        cols_to_drop = ['date_only', 'id', 'id_meteo', 'datetime_meteo']
        df_final = df_final.drop(columns=[c for c in cols_to_drop if c in df_final.columns], errors='ignore')

        print(f"✅ Dataset prêt : {df_final.shape[0]} lignes générées.")
        return df_final