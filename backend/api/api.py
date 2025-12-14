from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import pandas as pd
from dotenv import load_dotenv
from backend.data.schemas import Database

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
    
# Cette version est trop lourde :C'est très clair. Oublions le monitoring.

# Voici l'explication technique : le problème principal est la méthode "Tout télécharger, puis filtrer". Actuellement, quand un utilisateur demande l'historique d'un seul compteur, voici ce qui se passe :

# - Python appelle la Base de Données (PostgreSQL) et PostgreSQL envoie TOUTE la table velo_clean (800 000+ lignes, peut-être 100 Mo ou plus) via le réseau vers Azure Web App.

# - Pandas charge ces 100 Mo dans la RAM, ensuite Pandas filtre pour ne garder que les 5 000 lignes du compteur demandé.

# - Python jette les 795 000 autres lignes à la poubelle.

# @app.get("/history/{counter_id}")
# def get_history(counter_id: str):
#     """Retourne l'historique réel pour un compteur."""
#     try:
#         df = db.pull_data("velo_clean")
#         # Filtrage
#         df = df[df['counter_id'] == counter_id]
#         df = df[['datetime', 'intensity']]         # on ne renvoie que les colonnes utiles
#         df = df.rename(columns={'intensity': 'count'})
#         # Conversion JSON
#         return df.to_dict(orient="records")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{counter_id}")
def get_history(counter_id: str):
    # 1. On écrit une requête SQL ciblée
    # On ne demande QUE les colonnes utiles et QUE les lignes du compteur
    query = f"""
        SELECT datetime, intensity 
        FROM velo_clean 
        WHERE counter_id = '{counter_id}'
    """
    
    # 2. On exécute via le moteur SQLAlchemy déjà présent dans ton objet db
    with db.engine.connect() as conn:
        df = pd.read_sql(query, conn)
        
    # 3. Plus besoin de filtrer, c'est déjà fait !
    df = df.rename(columns={'intensity': 'count'})
    
    # Petite astuce : convertir les dates en string pour éviter les erreurs JSON
    df['datetime'] = df['datetime'].astype(str)
    
    return df.to_dict(orient="records")

# # Version lourde
# @app.get("/prediction/{counter_id}")
# def get_prediction(counter_id: str):
#     """Retourne les prédictions (J+1) pour un compteur."""
#     try:
#         df = db.pull_data("model_data")
#         df = df[df['counter_id'] == counter_id]
#         df = df[['datetime', 'predicted_values']]
#         df = df.rename(columns={'predicted_values': 'count'})
#         return df.to_dict(orient="records")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@app.get("/prediction/{counter_id}")
def get_prediction(counter_id: str):
    """Retourne les prédictions (J+1) pour un compteur de façon optimisée."""
    try:
        # OPTIMISATION SQL :
        # 1. On filtre sur l'ID directement dans la base.
        # 2. On filtre sur la date (>= aujourd'hui) pour ne pas renvoyer de vieilles prédictions.
        query = f"""
            SELECT datetime, predicted_values 
            FROM model_data 
            WHERE counter_id = '{counter_id}'
            AND datetime >= CURRENT_DATE
            ORDER BY datetime ASC
        """
        
        with db.engine.connect() as conn:
            df = pd.read_sql(query, conn)
            
        # Si aucune prédiction n'est trouvée (ex: mauvais ID)
        if df.empty:
            return []

        # Nettoyage et Renommage
        df = df.rename(columns={'predicted_values': 'count'})
        
        # Conversion Date pour JSON
        df['datetime'] = df['datetime'].astype(str)
        
        return df.to_dict(orient="records")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Cette version est lourde aussi : on récupères aussi tout model_data (lourd) et tout velo_clean pour avoir les positions GPS.
# @app.get("/map-data")
# def get_map_data():
#     """
#     Route spéciale pour la carte : 
#     Récupère toutes les prédictions et joint les coordonnées GPS.
#     """
#     try:
#         # 1. Récupérer toutes les prédictions
#         df_preds = db.pull_data("model_data")
        
#         # 2. Récupérer les positions GPS (depuis velo_clean ou une table counters)
#         # On fait une requête SQL pour avoir une seule ligne par compteur avec lat/lon
#         query = "SELECT DISTINCT ON (counter_id) counter_id, lat, lon FROM velo_clean"
#         with db.engine.connect() as conn:
#             df_locs = pd.read_sql(query, conn)
            
#         # 3. Fusionner (Join)
#         # On ajoute lat/lon aux prédictions
#         df_merged = pd.merge(df_preds, df_locs, on="counter_id", how="left")
        
#         # 4. Nettoyage pour le frontend
#         # On renomme pour coller à votre code frontend
#         df_merged = df_merged.rename(columns={
#             'predicted_values': 'predicted_intensity',
#             'datetime': 'date'
#         })
        
#         # Astuce : Comme on n'a pas stocké la température dans model_data, 
#         # on met une valeur par défaut pour ne pas faire planter votre interface.
#         df_merged['temperature_2m'] = 15.0 
        
#         return df_merged.to_dict(orient="records")
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


#première version optimisé
@app.get("/map-data")
def get_map_data():
    try:
        # 1. On récupère les prédictions (J+1 seulement)
        # Supposons que tu veux les prédictions de demain.
        # Si model_data ne contient QUE les prédictions futures, pull_data est OK.
        # Sinon, il faut filtrer par date en SQL.
        df_preds = db.pull_data("model_data")

        # 2. Pour les positions GPS, on fait une requête SQL ultra-légère
        # DISTINCT ON : pour n'avoir qu'une seule ligne par compteur
        query_loc = "SELECT DISTINCT ON (counter_id) counter_id, lat, lon FROM velo_clean"
        
        with db.engine.connect() as conn:
            df_locs = pd.read_sql(query_loc, conn)
            
        # 3. Fusion (Join) en mémoire (rapide car les DataFrames sont maintenant petits)
        df_merged = pd.merge(df_preds, df_locs, on="counter_id", how="left")
        
        # Nettoyage
        df_merged = df_merged.rename(columns={
            'predicted_values': 'predicted_intensity',
            'datetime': 'date'
        })
        df_merged['temperature_2m'] = 15.0 
        
        # Conversion date pour JSON
        if 'date' in df_merged.columns:
            df_merged['date'] = df_merged['date'].astype(str)

        return df_merged.to_dict(orient="records")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/map-data")
def get_map_data():
    try:
        # 1. DÉFINITION DE LA FENÊTRE DE TEMPS
        # On regarde : Hier (pour le contexte), Aujourd'hui et Demain (J+1)
        # Cela permet de voir où s'arrêtent les données réelles.
        
        # --- REQUÊTE A : DONNÉES RÉELLES (La Vérité) ---
        query_real = """
            SELECT counter_id, datetime, intensity as real_count
            FROM velo_clean
            WHERE datetime >= CURRENT_DATE - INTERVAL '1 day'
        """
        
        # --- REQUÊTE B : PRÉDICTIONS (Le Futur) ---
        query_pred = """
            SELECT counter_id, datetime, predicted_values as pred_count
            FROM model_data
            WHERE datetime >= CURRENT_DATE - INTERVAL '1 day' 
            AND datetime < CURRENT_DATE + INTERVAL '2 day'
        """

        # --- REQUÊTE C : COORDONNÉES (Pour la carte) ---
        query_loc = "SELECT DISTINCT ON (counter_id) counter_id, lat, lon FROM velo_clean"

        # Exécution optimisée (3 requêtes légères)
        with db.engine.connect() as conn:
            df_real = pd.read_sql(query_real, conn)
            df_pred = pd.read_sql(query_pred, conn)
            df_locs = pd.read_sql(query_loc, conn)

        # 2. STANDARDISATION DES DATES
        # Pour que la fusion fonctionne, il faut être sûr que les formats soient identiques
        df_real['datetime'] = pd.to_datetime(df_real['datetime'])
        df_pred['datetime'] = pd.to_datetime(df_pred['datetime'])

        # 3. FUSION INTELLIGENTE (Outer Join)
        # On assemble Réel et Prédictions sur (counter_id + datetime)
        df_merged = pd.merge(df_pred, df_real, on=['counter_id', 'datetime'], how='outer')

        # 4. LA LOGIQUE "COMBLE LE RESTE" (Coalesce)
        # C'est ici que la magie opère :
        # "Prends la valeur réelle. Si elle est vide (NaN), prends la prédiction."
        df_merged['final_count'] = df_merged['real_count'].fillna(df_merged['pred_count'])
        
        # On remplit les derniers trous (cas où ni réel ni prédiction n'existent) par 0
        df_merged['final_count'] = df_merged['final_count'].fillna(0)

        # On ajoute un flag pour le frontend (pour savoir si c'est du réel ou du prédictif)
        # Si real_count n'est pas nul, c'est "History", sinon "Prediction"
        df_merged['status'] = df_merged['real_count'].notna().map({True: 'Reel', False: 'Prediction'})

        # 5. AJOUT DES COORDONNÉES
        df_final = pd.merge(df_merged, df_locs, on="counter_id", how="left")

        # 6. NETTOYAGE FINAL
        # On ne garde que les colonnes utiles
        df_final = df_final[['counter_id', 'datetime', 'final_count', 'status', 'lat', 'lon', 'real_count']]
        
        df_final = df_final.rename(columns={
            'final_count': 'predicted_intensity', # On garde ce nom pour compatibilité frontend
            'datetime': 'date'
        })
        
        # Ajout météo par défaut
        df_final['temperature_2m'] = 15.0
        # Conversion date pour JSON
        df_final['date'] = df_final['date'].astype(str)
        
        # Tri par date pour avoir une belle courbe
        df_final = df_final.sort_values(by=['counter_id', 'date'])

        return df_final.to_dict(orient="records")

    except Exception as e:
        print(f"❌ Erreur map-data: {e}")
        raise HTTPException(status_code=500, detail=str(e))