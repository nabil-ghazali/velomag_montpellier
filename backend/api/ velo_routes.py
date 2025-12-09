# backend/app/routes/velo_routes.py

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd

from app.services.velo_service import prepare_velo_pipeline
from backend.modeling.velo_model import Predictor
from app.fetch_velo_api import fetch_all_data

router = APIRouter(
    prefix="/velo",
    tags=["velo"]
)

# Instancier le Predictior (vide pour l'instant)
predictor = Predictor()


# ---------------------------------------------------------
# Endpoint : récupérer les données clean
# ---------------------------------------------------------
@router.get("/data")
def get_velo_data():
    try:
        # Récupérer les données depuis l'API Montpellier3M
        df_raw = fetch_all_data()
        # Pipeline nettoyage + features
        result = prepare_velo_pipeline(df_raw)
        df_clean = result['df_clean']
        # Retour en JSON
        return JSONResponse(content=df_clean.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------
# Endpoint : prédictions
# ---------------------------------------------------------
@router.get("/predict")
def get_predictions():
    try:
        # Récupérer les données nettoyées
        df_raw = fetch_all_data()
        result = prepare_velo_pipeline(df_raw)
        df_clean = result['df_clean']
        
        # Faire les prédictions
        df_pred = predictor.predict(df_clean)
        
        # Retour en JSON
        return JSONResponse(content=df_pred.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
