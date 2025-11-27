import requests
import pandas as pd
from tqdm import tqdm  # optionnel, juste pour le suivi

BASE_URL = "https://portail-api-data.montpellier3m.fr"

def fetch_counters():
    url = f"{BASE_URL}/ecocounter"
    resp = requests.get(url, headers={"Accept":"application/json"})
    resp.raise_for_status()
    return resp.json()

def fetch_timeseries(counter_id):
    url = f"{BASE_URL}/ecocounter_timeseries/{counter_id}/attrs/intensity"
    resp = requests.get(url, headers={"Accept":"application/json"})
    resp.raise_for_status()
    return resp.json()

# 1. récupérer tous les compteurs
counters = fetch_counters()
df_counters = pd.json_normalize(counters)

# Extraire les ids
ids = df_counters["id"].apply(lambda x: x.split(":")[-1]).unique().tolist()

# 2. pour chaque compteur, récupérer les mesures
all_data = []
for cid in tqdm(ids):
    try:
        ts = fetch_timeseries(cid)
        for item in ts:
            all_data.append({
                "counter_id": cid,
                "timestamp": item.get("intensity", {}).get("metadata", {}).get("TimeInstant", {}).get("value"),
                "intensity": item.get("intensity", {}).get("value")
            })
    except Exception as e:
        print("Erreur pour", cid, e)

df_all = pd.DataFrame(all_data)

# 3. Nettoyage
df_all["timestamp"] = pd.to_datetime(df_all["timestamp"], errors="coerce")
df_all["intensity"] = pd.to_numeric(df_all["intensity"], errors="coerce")
df_all = df_all.dropna(subset=["counter_id", "timestamp"])

# 4. Sauvegarde
df_counters.to_csv("data/raw/counters_meta.csv", index=False)
df_all.to_csv("data/raw/traffic_history.csv", index=False)

print("Ingestion terminée – fichiers raw créés")
