# backend/api/metrics.py
from fastapi import APIRouter
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import PlainTextResponse
from threading import Thread
import time
from modeling.evaluate import get_mae, get_r2

router = APIRouter()

# Création des métriques Prometheus
mae_gauge = Gauge("velo_mae", "MAE du modèle de prédiction vélo")
r2_gauge = Gauge("velo_r2", "R² du modèle de prédiction vélo")

def update_metrics(interval=60):
    """
    Met à jour les métriques toutes les 'interval' secondes.
    """
    while True:
        try:
            mae_val = get_mae()
            r2_val = get_r2()
            if mae_val is not None:
                mae_gauge.set(mae_val)
            if r2_val is not None:
                r2_gauge.set(r2_val)
        except Exception as e:
            print(f"Erreur mise à jour métriques : {e}")
        time.sleep(interval)

# Calcul initial pour que les métriques soient disponibles immédiatement
try:
    initial_mae = get_mae()
    initial_r2 = get_r2()
    if initial_mae is not None:
        mae_gauge.set(initial_mae)
    if initial_r2 is not None:
        r2_gauge.set(initial_r2)
except Exception as e:
    print(f"Erreur calcul initial métriques : {e}")

# Lancement du thread de mise à jour
thread = Thread(target=update_metrics, daemon=True)
thread.start()

@router.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

def add_metrics_route(app):
    app.include_router(router)

# from fastapi import APIRouter
# from prometheus_client import Gauge, start_http_server, generate_latest, CONTENT_TYPE_LATEST
# from fastapi.responses import PlainTextResponse
# from threading import Thread
# import time

# #from backend.modeling.evaluate import get_mae, get_r2
# from modeling.evaluate import get_mae, get_r2


# router = APIRouter()

# #  Création des métriques Prometheus
# mae_gauge = Gauge("velo_mae", "MAE du modèle de prédiction vélo")
# r2_gauge = Gauge("velo_r2", "R² du modèle de prédiction vélo")

# def update_metrics(interval=60):
#     """
#     Met à jour les métriques toutes les 'interval' secondes.
#     """
#     while True:
#         try:
#             mae_val = get_mae()
#             r2_val = get_r2()
#             if mae_val is not None:
#                 mae_gauge.set(mae_val)
#             if r2_val is not None:
#                 r2_gauge.set(r2_val)
#         except Exception as e:
#             print(f"Erreur mise à jour métriques : {e}")
#         time.sleep(interval)

# #  Lancement du thread de mise à jour
# thread = Thread(target=update_metrics, daemon=True)
# thread.start()

# @router.get("/metrics")
# def metrics():
#     return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# def add_metrics_route(app):
#     app.include_router(router)
    
    
# @router.get("/metrics")
# def metrics():
#     """
#     Retourne un texte lisible par Prometheus.
#     """
#     return {
#         "MAE": mae_gauge._value.get(),
#         "R2": r2_gauge._value.get()
#     }
# def add_metrics_route(app):
#     app.include_router(router)