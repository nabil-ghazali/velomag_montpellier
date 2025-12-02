# backend/app/main.py








# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from app.routes import velo_routes

# # ---------------------------------------------------------
# # Création de l'application FastAPI
# # ---------------------------------------------------------
# app = FastAPI(
#     title="Velomag Montpellier API",
#     description="API pour les données vélos Montpellier et prédictions",
#     version="1.0.0"
# )

# # ---------------------------------------------------------
# # Middleware CORS (Front to Backend)
# # ---------------------------------------------------------
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],          # Ou mettre l'URL du front
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # ---------------------------------------------------------
# # Inclusion des routes
# # ---------------------------------------------------------
# app.include_router(velo_routes.router, prefix="/velo", tags=["Vélo"])

# # ---------------------------------------------------------
# # Endpoint racine simple
# # ---------------------------------------------------------
# @app.get("/")
# def read_root():
#     return {"message": "Bienvenue sur l'API Velomag Montpellier "}

# # ---------------------------------------------------------
# # Exécution (si run direct)
# # # ---------------------------------------------------------
# # if __name__ == "__main__":
# #     import uvicorn
# #     uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)

























# from app.fetch_velo_api import fetch_all_counters, fetch_counter_description
# from app.utils.loader import load_existing_csv, update_csv
# from app.services.velo_service import add_time_features, compute_rolling_lags, categorize_counters
# import pandas as pd

# CSV_PATH = "../data/raw/ecocounters_full_complet.csv"

# # 1️ Charger ancien CSV
# df_old = load_existing_csv(CSV_PATH)

# # 2️ Déterminer la période de mise à jour
# from_date = (df_old["datetime"].max() + pd.Timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S") if not df_old.empty else "2025-03-10T00:00:00"
# to_date = pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S")

# # 3️ Récupérer les compteurs (liste initiale)
# counters = [
#     "urn:ngsi-ld:EcoCounter:X2H25023006",
#     "urn:ngsi-ld:EcoCounter:ZLT25011699",
#     "urn:ngsi-ld:EcoCounter:COM24010120",
#     # ... ajoute tous tes compteurs
# ]

# df_new = fetch_all_counters(counters, from_date, to_date)

# # 4️ Mise à jour du CSV
# update_csv(df_old, df_new, CSV_PATH)

# # 5️ Charger CSV complet mis à jour
# df_all = pd.read_csv(CSV_PATH)
# df_all["datetime"] = pd.to_datetime(df_all["datetime"])

# # 6️ Ajouter features temporelles
# df_all = add_time_features(df_all)

# # 7️ Ajouter rolling et lags
# df_all = compute_rolling_lags(df_all)

# # 8️ Catégoriser les compteurs
# duration = categorize_counters(df_all)

# # 9️ On peut maintenant filtrer df_162, df_200, df_100 selon duration
# counters_200 = duration[duration["category"] == "plus de 200 jours"].index.tolist()
# counters_162 = duration[duration["category"] == "162 jours ou plus"].index.tolist()
# counters_100 = duration[duration["category"] == "moins de 100 jours"].index.tolist()

# df_162 = df_all[df_all["counter_id"].isin(counters_162)].copy()
# df_200 = df_all[df_all["counter_id"].isin(counters_200)].copy()
# df_100 = df_all[df_all["counter_id"].isin(counters_100)].copy()

# # 10️ Sauvegarde datasets filtrés
# df_162.to_csv("../data/processed/df_162_clean.csv", index=False)
# df_200.to_csv("../data/processed/df_200_clean.csv", index=False)
# df_100.to_csv("../data/processed/df_100_clean.csv", index=False)
