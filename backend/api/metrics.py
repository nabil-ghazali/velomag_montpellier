from fastapi import APIRouter
from prometheus_client import Gauge

router = APIRouter()

# --- DÉFINITION DES JAUGES ---
# MAE : L'erreur moyenne en valeur absolue (ex: 12 vélos)
MAE_METRIC = Gauge('velomag_model_mae', 'Mean Absolute Error (Erreur Moyenne)')

# RMSE : L'erreur qui pénalise les gros écarts
RMSE_METRIC = Gauge('velomag_model_rmse', 'Root Mean Squared Error')

# R2 : La "note" du modèle (1 = Parfait, 0 = Nul)
R2_METRIC = Gauge('velomag_model_r2', 'R2 Score (Coefficient de determination)')

@router.post("/metrics/update-scores")
def update_scores():
    """
    Met à jour les scores de performance (Simulé pour l'instant).
    """
    try:
        # Valeurs fictives
        fake_mae = 12.5
        fake_rmse = 18.2
        fake_r2 = 0.85  # <-- Un score de 0.85, c'est un bon modèle !
        
        # Mise à jour des jauges Prometheus
        MAE_METRIC.set(fake_mae)
        RMSE_METRIC.set(fake_rmse)
        R2_METRIC.set(fake_r2) # <-- On remplit la nouvelle jauge
        
        return {
            "status": "success", 
            "metrics": {
                "mae": fake_mae, 
                "rmse": fake_rmse,
                "r2": fake_r2
            }
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}