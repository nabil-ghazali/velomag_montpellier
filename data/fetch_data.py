import requests
import pandas as pd
from datetime import datetime

class FetchAPI:

    def __init__(self, url):
        self.session = requests.Session()
        self.url = url

    # ---------------------------------------------------------
    # 1️ Récupérer la liste des compteurs
    # ---------------------------------------------------------
    def fetch_all_counters(self) -> list:
        response = self.session.get(self.url)

        if response.status_code != 200:
            print("Erreur récupération liste des compteurs :", response.status_code)
            return []

        data = response.json()

        # Certains endpoints renvoient {"data": [...]}, d'autres une liste brute
        if isinstance(data, dict) and "data" in data:
            counters = data["data"]
        else:
            counters = data

        ids = [c["id"] for c in counters if "id" in c]
        print(f"Nombre de compteurs trouvés : {len(ids)}")
        return ids

    # ---------------------------------------------------------
    # 2️ Récupérer les séries temporelles d’un compteur
    # ---------------------------------------------------------
    def fetch_counter_timeseries(self, counter_id: str, from_date: str, to_date: str) -> pd.DataFrame:
        url_timeseries = f"https://portail-api-data.montpellier3m.fr/ecocounter_timeseries/{counter_id}/attrs/intensity"

        params = {"fromDate": from_date, "toDate": to_date}

        response = self.session.get(url_timeseries, params=params)

        if response.status_code != 200:
            print(f"Erreur séries temporelles pour {counter_id} : {response.status_code}")
            return pd.DataFrame()

        data = response.json()

        if "index" not in data or "values" not in data:
            print(f"⚠ Pas de série temporelle pour {counter_id}")
            return pd.DataFrame()

        df = pd.DataFrame({
            "datetime": data["index"],
            "intensity": data["values"]
        })

        if df.empty:
            print(f"⚠ Série vide pour {counter_id}")
            return df

        df["datetime"] = pd.to_datetime(df["datetime"])
        df["intensity"] = df["intensity"].astype(int)
        df["counter_id"] = counter_id

        return df

    # ---------------------------------------------------------
    # 3️ Récupérer la description d’un compteur
    # ---------------------------------------------------------
    def fetch_counter_description(self, counter_id: str) -> dict:
        url_desc = f"{self.url}{counter_id}"
        response = self.session.get(url_desc)

        if response.status_code != 200:
            print(f"Erreur description pour {counter_id} : {response.status_code}")
            return {"lat": None, "lon": None, "laneId": None, "vehicleType": None}

        data = response.json()

        try:
            lat, lon = data["location"]["value"]["coordinates"]
        except:
            lat = lon = None

        laneId = data.get("laneId", {}).get("value", None)
        vehicleType = data.get("vehicleType", {}).get("value", None)

        return {
            "lat": lat,
            "lon": lon,
            "laneId": laneId,
            "vehicleType": vehicleType
        }
    
    def fetch_meteo(self, start_date, end_date, latitude:str, longitude:str) -> pd.DataFrame:
        
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        print(f" Chargement de la météo HORAIRE pour {latitude}, {longitude}...")
        
        # URL modifiée pour récupérer l'heure par heure (hourly)
        url_meteo = (
            f"https://archive-api.open-meteo.com/v1/era5?"
            f"latitude={latitude}&longitude={longitude}"
            f"&start_date={start_date}&end_date={end_date}"
            f"&hourly=temperature_2m,wind_speed_10m,precipitation" # <-- CHANGEMENT ICI
        )

        try:
            response = requests.get(url_meteo, timeout=10)
            response.raise_for_status()

            data = response.json()
            
            # CHANGEMENT ICI : On cherche la clé 'hourly' et plus 'daily'
            if "hourly" not in data:
                print(" Erreur : clé 'hourly' manquante dans le JSON")
                return None

            # Création du DataFrame
            self.meteo_df = pd.DataFrame(data["hourly"])
            
            # La colonne s'appelle souvent 'time' dans l'API, on la renomme 'datetime'
            self.meteo_df = self.meteo_df.rename(columns={'time': 'datetime'})
            
            # Conversion en format date
            self.meteo_df['datetime'] = pd.to_datetime(self.meteo_df['datetime'])

            print(f" Météo chargée : {len(self.meteo_df)} heures récupérées.")
            return self.meteo_df

        except requests.exceptions.RequestException as e:
            print(f" Erreur de connexion API Météo : {e}")
            return None
        

    def fetch_all_data_velo(self, start_date="2024-11-30T00:00:00", end_date = None) -> pd.DataFrame:
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%dT23:59:59")
        counters = self.fetch_all_counters()
        data = []

        for counter_id in counters:
            print(f"\n➡ Récupération pour : {counter_id}")

            # --- Timeseries ---
            df_ts = self.fetch_counter_timeseries(counter_id, start_date, end_date)
            if df_ts.empty:
                continue

            # --- Description ---
            desc = self.fetch_counter_description(counter_id)

            df_ts["lat"] = desc["lat"]
            df_ts["lon"] = desc["lon"]
            df_ts["laneId"] = desc["laneId"]
            df_ts["vehicleType"] = desc["vehicleType"]

            data.append(df_ts)
            print(f" {len(df_ts)} lignes ajoutées")

        if not data:
            print("Aucune donnée récupérée")
            return pd.DataFrame()

        df_final = pd.concat(data, ignore_index=True)
        return df_final