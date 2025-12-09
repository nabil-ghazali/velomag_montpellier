# fetch_velo_api.py

import requests
import pandas as pd
from datetime import datetime, timedelta

class VelomagAPI:
    BASE_URL = "https://portail-api-data.montpellier3m.fr/ecocounter/"

    def __init__(self):
        self.session = requests.Session()

    # ---------------------------------------------------------
    # 1️ Récupérer la liste des compteurs
    # ---------------------------------------------------------
    def fetch_all_counters(self) -> list:
        url = self.BASE_URL
        response = self.session.get(url)

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
        url = f"https://portail-api-data.montpellier3m.fr/ecocounter_timeseries/{counter_id}/attrs/intensity"

        params = {"fromDate": from_date, "toDate": to_date}

        response = self.session.get(url, params=params)

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
        url = f"{self.BASE_URL}{counter_id}"
        response = self.session.get(url)

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

    # ---------------------------------------------------------
    # 4️ Récupérer TOUTES les données (loop complète)
    # ---------------------------------------------------------
    def fetch_all_data(self, start_date="2024-11-30T00:00:00") -> pd.DataFrame:
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
# ---------------------------------------------------------
# Bloc de test
# ---------------------------------------------------------
if __name__ == "__main__":
    import os
    # Créer une instance de l'API
    api = VelomagAPI()

    # 1️ Récupérer la liste des compteurs
    counters = api.fetch_all_counters()
    print("Liste des compteurs (5 premiers) :", counters[:5])  # affiche seulement les 5 premiers

    # 2️ Tester la récupération d'une série temporelle pour le premier compteur
    if counters:
        counter_id = counters[0]
        df_ts = api.fetch_counter_timeseries(counter_id, "2024-11-30T00:00:00", "2024-12-01T23:59:59")
        print(f"\nPremières lignes de la série temporelle pour {counter_id} :")
        print(df_ts.head())

        # 3️ Tester la récupération de la description
        desc = api.fetch_counter_description(counter_id)
        print(f"\nDescription du compteur {counter_id} :")
        print(desc)

    # 4️ Récupérer toutes les données
    df_all = api.fetch_all_data("2024-11-30T00:00:00")
    print(f"\nNombre total de lignes récupérées : {len(df_all)}")

    # 5️ Sauvegarder les données dans un CSV pour le pipeline
    if not df_all.empty:
        os.makedirs("../data/raw", exist_ok=True)  # créer le dossier si nécessaire
        path_csv = "../data/raw/velo_data.csv"
        df_all.to_csv(path_csv, index=False)
        print(f"Données sauvegardées dans {path_csv}")







# import requests
# import pandas as pd
# from datetime import datetime, timedelta

# BASE_URL = "https://portail-api-data.montpellier3m.fr/ecocounter/"

# # ---------------------------------------------------------
# # 1️ Récupérer la liste des compteurs
# # ---------------------------------------------------------
# def fetch_all_counters() -> list:
#     url = BASE_URL
#     response = requests.get(url)

#     if response.status_code != 200:
#         print("Erreur récupération liste des compteurs :", response.status_code)
#         return []

#     data = response.json()

#     # Certains endpoints renvoient {"data": [...]}, d'autres une liste brute
#     if isinstance(data, dict) and "data" in data:
#         counters = data["data"]
#     else:
#         counters = data

#     ids = [c["id"] for c in counters if "id" in c]
#     print(f"Nombre de compteurs trouvés : {len(ids)}")
#     return ids


# # ---------------------------------------------------------
# # 2️ Récupérer les séries temporelles d’un compteur
# # ---------------------------------------------------------
# def fetch_counter_timeseries(counter_id: str, from_date: str, to_date: str) -> pd.DataFrame:
#     url = f"https://portail-api-data.montpellier3m.fr/ecocounter_timeseries/{counter_id}/attrs/intensity"
#     params = {"fromDate": from_date, "toDate": to_date}

#     response = requests.get(url, params=params)

#     if response.status_code != 200:
#         print(f"Erreur séries temporelles pour {counter_id} : {response.status_code}")
#         return pd.DataFrame()

#     data = response.json()

#     if "index" not in data or "values" not in data:
#         print(f"⚠ Pas de série temporelle pour {counter_id}")
#         return pd.DataFrame()

#     df = pd.DataFrame({
#         "datetime": data["index"],
#         "intensity": data["values"]
#     })

#     if df.empty:
#         print(f"⚠ Série vide pour {counter_id}")
#         return df

#     df["datetime"] = pd.to_datetime(df["datetime"])
#     df["intensity"] = df["intensity"].astype(int)
#     df["counter_id"] = counter_id

#     return df

# # ---------------------------------------------------------
# # 3️ Récupérer la description d’un compteur
# # ---------------------------------------------------------
# def fetch_counter_description(counter_id: str) -> dict:
#     url = f"{BASE_URL}{counter_id}"
#     response = requests.get(url)

#     if response.status_code != 200:
#         print(f"⚠ Erreur description pour {counter_id} : {response.status_code}")
#         return {"lat": None, "lon": None, "laneId": None, "vehicleType": None}

#     data = response.json()

#     # Sécurisé si location manquant
#     try:
#         lat, lon = data["location"]["value"]["coordinates"]
#     except:
#         lat = lon = None

#     laneId = data.get("laneId", {}).get("value", None)
#     vehicleType = data.get("vehicleType", {}).get("value", None)

#     return {
#         "lat": lat,
#         "lon": lon,
#         "laneId": laneId,
#         "vehicleType": vehicleType
#     }


# # ---------------------------------------------------------
# # 4️ Récupérer TOUTES les données (loop complète)
# # ---------------------------------------------------------
# def fetch_all_data(start_date="2024-11-30T00:00:00") -> pd.DataFrame:
#     # Date d’aujourd’hui automatique (jusqu’à minuit)
#     end_date = datetime.now().strftime("%Y-%m-%dT23:59:59")

#     counters = fetch_all_counters()
#     data = []

#     for counter_id in counters:
#         print(f"\n➡ Récupération pour : {counter_id}")

#         # --- Timeseries ---
#         df_ts = fetch_counter_timeseries(counter_id, start_date, end_date)
#         if df_ts.empty:
#             continue

#         # --- Description ---
#         desc = fetch_counter_description(counter_id)

#         df_ts["lat"] = desc["lat"]
#         df_ts["lon"] = desc["lon"]
#         df_ts["laneId"] = desc["laneId"]
#         df_ts["vehicleType"] = desc["vehicleType"]

#         data.append(df_ts)
#         print(f" {len(df_ts)} lignes ajoutées")

#     if not data:
#         print("⚠ Aucune donnée récupérée")
#         return pd.DataFrame()

#     df_final = pd.concat(data, ignore_index=True)
#     return df_final