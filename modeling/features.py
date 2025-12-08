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

import pandas as pd
from database.schemas import Database
import os
from dotenv import load_dotenv

# 1. Charger la configuration
load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

# 2. Connexion
print(" Connexion à la base de données...")
db = Database(DATABASE_URL)

# 3. Téléchargement et Sauvegarde
print(" Téléchargement des données vélos...")
df_velo = db.pull_data("velo_clean")
df_velo.to_csv("data_files/viaBDD/mes_donnees_velo.csv", index=False, sep=";") # Séparateur ; pour Excel en France
print(f" 'data_files/viaBDD/mes_donnees_velo.csv' créé ({len(df_velo)} lignes)")

print(" Téléchargement de la météo...")
df_meteo = db.pull_data("meteo_clean")
df_meteo.to_csv("data_files/viaBDD/mes_donnees_meteo.csv", index=False, sep=";")
print(f" 'data_files/viaBDD/mes_donnees_meteo.csv' créé ({len(df_meteo)} lignes)")