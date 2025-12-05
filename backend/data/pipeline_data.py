from database.schemas import Database
from data.fetch_data import FetchAPI
from data.clean_data import DataCleaning
import os
from datetime import datetime
from dotenv import load_dotenv
import typer


app = typer.Typer(help="P")
# Load environment variables from .env
load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"
OPEN_API_URL = os.getenv("OPEN_API_URL")

start_date="2024-11-30"
end_date = datetime.now().strftime("%Y-%m-%d")
latitude=43.6119
longitude=3.8772

db = Database(DATABASE_URL)
fetch = FetchAPI(OPEN_API_URL) #le url sera passé en argument de la classe
clean = DataCleaning() #le dataframe sera passé en argument des fonctions

@app.command()
def delete_tables():
    """Supprime les tables existantes dans la base de données."""
    db.drop_tables()
    print("Tables supprimées avec succès.")

@app.command()
def create_tables():
    """Crée les tables dans la base de données."""
    db.create_tables()
    print("Tables créées avec succès.")

@app.command()
def push_velo():
    """Récupère les données depuis l'API et les charge dans la base de données."""

    data_velo = fetch.fetch_all_data_velo()
    df_clean_velo = clean.clean_data_velo(data_velo)
    df_clean_velo = clean._standardize_delete_timezone(df_clean_velo)
    db.push_data(data_velo, "velo_raw")
    db.push_data(df_clean_velo, "velo_clean")

@app.command()
def push_meteo():
    """Récupère les données météo depuis l'API et les charge dans la base de données."""

    data_meteo = fetch.fetch_meteo(start_date, end_date, latitude, longitude)
    df_clean_meteo = clean._standardize_to_UTC(data_meteo)
    db.push_data(data_meteo, "meteo_raw")
    db.push_data(df_clean_meteo, "meteo_clean")

@app.command()
def push_db():
    """Récupère les données depuis l'API et les charge dans la base de données."""

    data_velo = fetch.fetch_all_data_velo()
    data_meteo = fetch.fetch_meteo(start_date, end_date, latitude, longitude)
    df_clean_velo = clean.clean_data_velo(data_velo)
    df_clean_velo = clean._standardize_delete_timezone(df_clean_velo)
    df_clean_meteo = clean._standardize_to_UTC(data_meteo)
    db.push_data(data_velo, "velo_raw")
    db.push_data(df_clean_velo, "velo_clean")
    db.push_data(data_meteo, "meteo_raw")
    db.push_data(df_clean_meteo, "meteo_clean")
    print("Données récupérées et chargées avec succès.")


@app.command()
def pull_db():
    """Charge les données nettoyées dans la base de données."""
    print("Fonction de chargement des données dans la base de données.")
    df_velo_clean = db.pull_data("velo_clean")
    df_meteo_clean = db.pull_data("meteo_clean")
    print(f"Données Vélo nettoyées : {len(df_velo_clean)} enregistrements.")
    print(f"Données Météo nettoyées : {len(df_meteo_clean)} enregistrements.")