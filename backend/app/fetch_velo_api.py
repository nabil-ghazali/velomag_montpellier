import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_counter_timeseries(counter_id: str, from_date: str, to_date: str) -> pd.DataFrame:
    """Récupère la série temporelle d'un compteur"""
    url_series = f"https://portail-api-data.montpellier3m.fr/ecocounter_timeseries/{counter_id}/attrs/intensity"
    params = {"fromDate": from_date, "toDate": to_date}
    response = requests.get(url_series, params=params)
    
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame({
            "datetime": data["index"],
            "intensity": data["values"],
            "counter_id": counter_id
        })
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df
    else:
        print(f"Erreur récupération séries pour {counter_id} : {response.status_code}")
        return pd.DataFrame()

def fetch_counter_description(counter_id: str) -> dict:
    """Récupère les informations statiques d’un compteur"""
    url_desc = f"https://portail-api-data.montpellier3m.fr/ecocounter/{counter_id}"
    response = requests.get(url_desc)
    if response.status_code == 200:
        data = response.json()
        lat, lon = data["location"]["value"]["coordinates"]
        laneId = data.get("laneId", {}).get("value")
        vehicleType = data.get("vehicleType", {}).get("value")
        return {
            "counter_id": counter_id,
            "lat": lat,
            "lon": lon,
            "laneId": laneId,
            "vehicleType": vehicleType
        }
    else:
        print(f"Erreur récupération description pour {counter_id}")
        return {
            "counter_id": counter_id,
            "lat": None,
            "lon": None,
            "laneId": None,
            "vehicleType": None
        }

def fetch_all_counters(counters: list, from_date: str, to_date: str) -> pd.DataFrame:
    """Récupère toutes les séries temporelles pour une liste de compteurs"""
    all_data = []
    for c in counters:
        df = fetch_counter_timeseries(c, from_date, to_date)
        if not df.empty:
            all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()