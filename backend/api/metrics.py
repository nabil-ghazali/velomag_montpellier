# backend/api/metrics.py
from fastapi import APIRouter
from prometheus_client import Gauge, start_http_server
from threading import Thread
import time

from backend.modeling.evaluate import get_mae, get_r2

router = APIRouter()

#  Création des métriques Prometheus
mae_gauge = Gauge("velo_mae", "MAE du modèle de prédiction vélo")
r2_gauge = Gauge("velo_r2", "R² du modèle de prédiction vélo")

def update_metrics(interval=60):
    """
    Met à jour les métriques toutes les 'interval' secondes.
    """
    while True:
        try:
            mae_gauge.set(get_mae())
            r2_gauge.set(get_r2())
        except Exception as e:
            print(f"Erreur mise à jour métriques : {e}")
        time.sleep(interval)

#  Lancement du thread de mise à jour
thread = Thread(target=update_metrics, daemon=True)
thread.start()

@router.get("/metrics")
def metrics():
    """
    Retourne un texte lisible par Prometheus.
    """
    return {
        "MAE": mae_gauge._value.get(),
        "R2": r2_gauge._value.get()
    }
def add_metrics_route(app):
    app.include_router(router)