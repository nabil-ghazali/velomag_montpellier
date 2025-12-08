from fastapi import FastAPI
from datetime import date, timedelta
import numpy as np
import pandas as pd

app = FastAPI()

# --- BASE DE DONNÉES SIMULÉE (Compteurs Statiques) ---
# Ces infos ne changent pas, c'est pourquoi on fait une route dédiée
COUNTERS_DB = [
    {"id": "Albert 1er", "lat": 43.6162, "lon": 3.8744},
    {"id": "Comédie", "lat": 43.6084, "lon": 3.8814},
    {"id": "Beracasa", "lat": 43.6038, "lon": 3.8876},
    {"id": "Celleneuve", "lat": 43.6146, "lon": 3.8343},
    {"id": "Corum", "lat": 43.6139, "lon": 3.8825},
    {"id": "Polygone", "lat": 43.6071, "lon": 3.8858},
]

@app.get("/counters")
def get_counters():
    """Renvoie la liste des compteurs et leurs coordonnées."""
    return COUNTERS_DB

@app.get("/predict")
def predict_mock():
    """Génère les prédictions sur 3 jours pour les compteurs existants."""
    
    # On génère J, J+1, J+2
    days_to_predict = [date.today() + timedelta(days=i) for i in range(3)]
    hours = range(24)
    data = []

    for current_date in days_to_predict:
        
        # Logique Week-end
        is_weekend = current_date.weekday() >= 5
        weekend_factor = 0.6 if is_weekend else 1.0

        for h in hours:
            # Météo simulée (identique pour toute la ville à cette heure)
            temp_simulated = 12 + 4 * np.sin(2 * np.pi * (h - 9) / 24)
            
            for counter in COUNTERS_DB:
                
                # Simulation de l'intensité
                # Profil sinusoïdal avec pics à 8h et 18h
                base_val = np.sin(h/24 * np.pi * 2)**2 * 100 
                if 7 <= h <= 9 or 17 <= h <= 19:
                    base_val *= 1.5 # Boost heures de pointe
                
                # Ajout d'aléa et facteur weekend
                intensity = int((base_val * weekend_factor) + np.random.randint(0, 15))
                intensity = max(0, intensity)

                data.append({
                    "date": str(current_date),
                    "hour": h,
                    "counter_id_encoded": counter['id'],
                    "predicted_intensity": intensity,
                    "lat": counter['lat'],
                    "lon": counter['lon'],
                    "temperature_2m": round(temp_simulated, 1)
                })
                
    return data