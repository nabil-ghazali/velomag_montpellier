from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import pandas as pd
from dotenv import load_dotenv
from data.schemas import Database

# 1. Configuration
load_dotenv()
app = FastAPI(title="VéloMag API")

# Autoriser Streamlit (qui tourne sur un autre port) à parler à l'API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Connexion Base de Données
try:
    USER = os.getenv("user")
    PASSWORD = os.getenv("password")
    HOST = os.getenv("host")
    PORT = os.getenv("port")
    DBNAME = os.getenv("dbname")
    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"
    db = Database(DATABASE_URL)
except Exception as e:
    print(f" Erreur DB connection: {e}")

# --- ROUTES ---

@app.get("/")
def root():
    return {"message": "API VéloMag est en ligne ! 🚲"}

@app.get("/counters")
def get_list_counters():
    """Retourne la liste unique des compteurs disponibles."""
    try:
        # On lit la table model_data car c'est elle qui contient les prédictions prêtes
        df = db.pull_data("model_data")
        if df.empty:
            return {"counters": []}
        
        unique_ids = df['counter_id'].unique().tolist()
        return {"counters": unique_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{counter_id}")
def get_history(counter_id: str):
    """Retourne l'historique réel pour un compteur."""
    try:
        df = db.pull_data("velo_clean")
        # Filtrage
        df = df[df['counter_id'] == counter_id]
        # Optimisation : on ne renvoie que les colonnes utiles
        df = df[['datetime', 'intensity']]
        df = df.rename(columns={'intensity': 'count'})
        # Conversion JSON
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/prediction/{counter_id}")
def get_prediction(counter_id: str):
    """Retourne les prédictions (J+1) pour un compteur."""
    try:
        df = db.pull_data("model_data")
        df = df[df['counter_id'] == counter_id]
        df = df[['datetime', 'predicted_values']]
        df = df.rename(columns={'predicted_values': 'count'})
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    # --- AJOUTER À LA FIN DE app/api.py ---

@app.get("/map-data")
def get_map_data():
    """
    Route spéciale pour la carte : 
    Récupère toutes les prédictions et joint les coordonnées GPS.
    """
    try:
        # 1. Récupérer toutes les prédictions
        df_preds = db.pull_data("model_data")
        
        # 2. Récupérer les positions GPS (depuis velo_clean ou une table counters)
        # On fait une requête SQL pour avoir une seule ligne par compteur avec lat/lon
        query = "SELECT DISTINCT ON (counter_id) counter_id, lat, lon FROM velo_clean"
        with db.engine.connect() as conn:
            df_locs = pd.read_sql(query, conn)
            
        # 3. Fusionner (Join)
        # On ajoute lat/lon aux prédictions
        df_merged = pd.merge(df_preds, df_locs, on="counter_id", how="left")
        
        # 4. Nettoyage pour le frontend
        # On renomme pour coller à votre code frontend
        df_merged = df_merged.rename(columns={
            'predicted_values': 'predicted_intensity',
            'datetime': 'date'
        })
        
        # Astuce : Comme on n'a pas stocké la température dans model_data, 
        # on met une valeur par défaut pour ne pas faire planter votre interface.
        df_merged['temperature_2m'] = 15.0 
        
        return df_merged.to_dict(orient="records")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))