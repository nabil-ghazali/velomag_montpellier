import pandas as pd
import requests
import json
import os 


class DataLoader:
    def __init__(self):
        # C'est bien de prévoir des attributs, même vides au début
        self.meteo_df = None
        self.feries_df = None

    def load_meteo(self, start_date='2024-11-30', end_date='2025-12-01', latitude=43.6109, longitude=3.8763):
        """
        Charge les données météo historiques depuis Open-Meteo ERA5.
        Retourne un DataFrame Pandas nettoyé.
        """
        print(f" Chargement de la météo pour {latitude}, {longitude}...")
        
        url = (
            f"https://archive-api.open-meteo.com/v1/era5?"
            f"latitude={latitude}&longitude={longitude}"
            f"&start_date={start_date}&end_date={end_date}"
            f"&daily=temperature_2m_max,temperature_2m_min,shortwave_radiation_sum"
            f"&timezone=Europe/Paris"
        )

        try:
            response = requests.get(url, timeout=10) # Toujours mettre un timeout !
            response.raise_for_status() # Lève une erreur si code != 200

            data = response.json()
            
            if "daily" not in data:
                print(" Erreur : clé 'daily' manquante dans le JSON")
                return None

            # Conversion en DataFrame
            self.meteo_df = pd.DataFrame(data["daily"])
            
            # Nettoyage des types
            self.meteo_df['time'] = pd.to_datetime(self.meteo_df['time'])
            
            # Renommage pour cohérence (optionnel mais conseillé)
            self.meteo_df = self.meteo_df.rename(columns={'time': 'date'})

            print(f" Météo chargée : {len(self.meteo_df)} jours récupérés.")
            return self.meteo_df

        except requests.exceptions.RequestException as e:
            print(f" Erreur de connexion API Météo : {e}")
            return None

    def load_jours_feries(self, annee: int):
        """
        Charge les jours fériés pour une année donnée via api.gouv.fr.
        Retourne un DataFrame avec colonnes ['date', 'nom_ferie'].
        """
        print(f" Chargement des jours fériés pour {annee}...")
        
        url = f"https://calendrier.api.gouv.fr/jours-feries/metropole/{annee}.json"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data_dict = response.json() 
            # data_dict ressemble à : {"2024-01-01": "1er janvier", ...}

            # --- TRANSFORMATION EN DATAFRAME ---
            # C'est l'étape clé pour pouvoir merger avec la météo plus tard !
            df = pd.DataFrame(list(data_dict.items()), columns=['date', 'nom_ferie'])
            
            # Conversion de la date en datetime
            df['date'] = pd.to_datetime(df['date'])
            
            self.feries_df = df
            
            print(f" {len(df)} jours fériés chargés.")
            return df

        except requests.exceptions.RequestException as e:
            print(f" Erreur API Jours Fériés : {e}")
            return None
        

    def export_data(self, df, path_export):
        """
        Sauvegarde un DataFrame en CSV et CRÉE LES DOSSIERS MANQUANTS.
        """
        print(f" Tentative de sauvegarde vers : {path_export}")

        # 1. Gestion automatique des dossiers
        # On récupère le chemin du dossier parent (ex: "data/jour_feries_2024")
        dossier = os.path.dirname(path_export)
        
        # Si le dossier n'est pas vide et n'existe pas, on le crée
        if dossier and not os.path.exists(dossier):
            print(f" Le dossier '{dossier}' n'existe pas. Création en cours...")
            try:
                os.makedirs(dossier, exist_ok=True) # exist_ok=True évite les erreurs si créé entre temps
            except OSError as e:
                print(f" Impossible de créer le dossier : {e}")
                return

        # 2. Sauvegarde
        try:
            df.to_csv(
                path_export,
                sep=';',             # Standard Excel FR
                decimal=',',         # Virgule décimale
                encoding='utf-8-sig',# Gestion des accents
                index=True           # On garde les dates
            )
            print(" Export réussi !")
        except Exception as e:
            print(f" Erreur lors de l'écriture du fichier : {e}")


# --- EXÉCUTION ---

# 1. Instanciation
loader = DataLoader()

# 2. Chargement Jours Fériés (Test)
# Attention : la méthode a besoin de 'self' implicitement, donc on l'appelle sur l'instance 'loader'
# df_feries_2024 = loader.load_jours_feries(2025)

# print("\n--- Aperçu Jours Fériés ---")
# if df_feries_2024 is not None:
#     print(df_feries_2024.head())

# loader.export_data(df_feries_2024, "data/jours_feries_2025.csv")
# 3. Chargement Météo (Test)
df_meteo = loader.load_meteo()
print(df_meteo.head(10))
loader.export_data(df_meteo, "data/meteo_mtp.csv")
