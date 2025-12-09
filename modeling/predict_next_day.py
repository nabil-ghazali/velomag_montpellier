import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import requests
import holidays
from datetime import timedelta
from modeling.features import FeatureEngineering

class Predictor:
    def __init__(self):
        self.model_path = 'modeling/model_velo.pkl'
        
    def get_weather_forecast(self, date_target) -> pd.DataFrame:
        """
        Récupère la météo prévue pour demain via l'API Open-Meteo.
        """
        print(f"☁️  Récupération météo pour le {date_target.date()}...")
        url = "https://api.open-meteo.com/v1/forecast"
        
        params = {
            "latitude": 43.6108,
            "longitude": 3.8767,
            "hourly": "temperature_2m,wind_speed_10m,precipitation",
            "timezone": "UTC", # On reste cohérent avec l'entraînement
            "start_date": date_target.strftime("%Y-%m-%d"),
            "end_date": date_target.strftime("%Y-%m-%d")
        }
        
        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            
            df_weather = pd.DataFrame(data['hourly'])
            df_weather['time'] = pd.to_datetime(df_weather['time'])
            # On renomme pour que ça matche avec notre pipeline
            df_weather = df_weather.rename(columns={'time': 'ds'})
            return df_weather
            
        except Exception as e:
            print(f"⚠️ Erreur Météo : {e}")
            # Fallback : Si l'API plante, on crée une journée vide pour ne pas crasher
            dates = pd.date_range(start=date_target, periods=24, freq='h')
            return pd.DataFrame({'ds': dates, 'temperature_2m': 15, 'wind_speed_10m': 10, 'precipitation': 0})

    def run_prediction(self):
        print("🔮 Lancement du script de prédiction J+1...")

        # --- 1. CHARGEMENT & PIPELINE (VOTRE DEMANDE) ---
        # On utilise exactement la même classe que pour l'entraînement
        fe = FeatureEngineering()
        df_history = fe.create_dataset()
        
        # Tri indispensable pour que les lags fonctionnent
        df_history = df_history.sort_values(['counter_id', 'ds'])

        # --- 2. DÉFINITION DE "DEMAIN" ---
        last_date_in_db = df_history['ds'].max()
        target_date = last_date_in_db + timedelta(days=1)
        # On remet à minuit pile pour avoir toute la journée
        target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        print(f" Dernière donnée connue : {last_date_in_db}")
        print(f" Objectif : Prédire la journée du {target_date.date()}")

        # --- 3. PRÉPARATION DES DONNÉES FUTURES ---
        # A. On récupère la météo du futur
        df_forecast = self.get_weather_forecast(target_date)
        
        # B. On crée la structure (Produit Cartésien : Heures x Compteurs)
        counters = df_history['counter_id'].unique()
        unique_encoded = df_history[['counter_id', 'counter_id_encoded']].drop_duplicates()
        
        future_data = []

        # Pour optimiser, on indexe l'historique par (counter_id, ds)
        # Cela permet de trouver la valeur d'hier en une fraction de seconde
        history_lookup = df_history.set_index(['counter_id', 'ds'])['count']

        print("  Construction des features (Lags, Cycles)...")
        for counter in counters:
            # On récupère l'encodage numérique de ce compteur
            encoded_id = unique_encoded[unique_encoded['counter_id'] == counter]['counter_id_encoded'].values[0]
            
            # On prend la météo et on lui colle l'info du compteur
            temp_df = df_forecast.copy()
            temp_df['counter_id'] = counter
            temp_df['counter_id_encoded'] = encoded_id
            
            # --- CALCUL DES LAGS (Cœur du système) ---
            # Le Lag 24h de "Demain 8h", c'est la réalité d' "Aujourd'hui 8h"
            
            def get_lag(row, hours_back):
                target_time = row['ds'] - timedelta(hours=hours_back)
                try:
                    return history_lookup.loc[(counter, target_time)]
                except KeyError:
                    # Si on n'a pas la donnée (ex: trou dans l'historique), on renvoie 0
                    return 0

            temp_df['lag_24h'] = temp_df.apply(lambda row: get_lag(row, 24), axis=1)
            temp_df['lag_48h'] = temp_df.apply(lambda row: get_lag(row, 48), axis=1)
            temp_df['lag_168h'] = temp_df.apply(lambda row: get_lag(row, 168), axis=1) # 1 semaine
            
            # Approximation pour la moyenne glissante (on fait avec ce qu'on a)
            temp_df['mean_last_4_days'] = (temp_df['lag_24h'] + temp_df['lag_48h']) / 2

            future_data.append(temp_df)

        # On rassemble tout dans un seul DataFrame
        df_final = pd.concat(future_data, ignore_index=True)

        # --- 4. FEATURE ENGINEERING (Comme dans train.py) ---
        df_final['hour'] = df_final['ds'].dt.hour
        df_final['day_of_week'] = df_final['ds'].dt.dayofweek
        df_final['month'] = df_final['ds'].dt.month
        df_final['is_weekend'] = df_final['day_of_week'].isin([5, 6]).astype(int)
        
        fr_holidays = holidays.France()
        df_final['is_holiday'] = df_final['ds'].apply(lambda x: 1 if x in fr_holidays else 0)
        
        # Cycles (Sin/Cos)
        df_final['hour_sin'] = np.sin(2 * np.pi * df_final['hour'] / 24)
        df_final['hour_cos'] = np.cos(2 * np.pi * df_final['hour'] / 24)
        df_final['month_sin'] = np.sin(2 * np.pi * df_final['month'] / 12)
        df_final['month_cos'] = np.cos(2 * np.pi * df_final['month'] / 12)
        df_final['dow_sin'] = np.sin(2 * np.pi * df_final['day_of_week'] / 7)
        df_final['dow_cos'] = np.cos(2 * np.pi * df_final['day_of_week'] / 7)

        # --- 5. CHARGEMENT MODÈLE & PRÉDICTION ---
        print("🧠 Chargement du modèle...")
        try:
            model = joblib.load(self.model_path)
        except FileNotFoundError:
            print("❌ Erreur : Modèle non trouvé. Lancez 'python -m modeling.train' d'abord.")
            return

        # On aligne les colonnes exactement comme le modèle les attend
        model_cols = model.get_booster().feature_names
        
        # Sécurité : Si une colonne manque, on met 0
        for col in model_cols:
            if col not in df_final.columns:
                df_final[col] = 0
        
        # On garde uniquement les colonnes utiles dans le bon ordre
        X = df_final[model_cols]
        
        print("⚡ Calcul des prédictions...")
        preds = model.predict(X)
        preds = [max(0, int(x)) for x in preds] # Pas de négatifs, converti en entier

        # --- 6. SAUVEGARDE EN BASE ---
        print("📤 Sauvegarde dans 'model_data'...")
        df_export = pd.DataFrame({
            'datetime': df_final['ds'],
            'counter_id': df_final['counter_id'],
            'predicted_values': preds
        })

        try:
            # On supprime d'abord les potentielles anciennes prédictions pour cette date 
            # (Note: Database n'a pas de delete_where simple, donc on insert juste en plus pour l'instant)
            fe.db.push_data(df_export, "model_data")
            print(f"✅ Succès ! {len(df_export)} lignes insérées pour le {target_date.date()}.")
        except Exception as e:
            print(f"❌ Erreur lors de l'insertion : {e}")

if __name__ == "__main__":
    p = Predictor()
    p.run_prediction()