import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import requests
import holidays
from datetime import datetime, timedelta
from backend.data.schemas import Database
from backend.modeling.features import FeatureEngineering

class Predictor:
    def __init__(self):
        self.model_path = 'backend/model/model_velo.pkl'
        # Jusqu'à quand prédire ? (Demain réel)
        self.real_tomorrow = datetime.now().date() + timedelta(days=1)
        
    def get_weather_data(self, date_target) -> pd.DataFrame:
        """
        Récupère météo Archive (Passé) ou Forecast (Futur)
        """
        is_past = date_target.date() < datetime.now().date()
        
        url = "https://archive-api.open-meteo.com/v1/era5" if is_past else "https://api.open-meteo.com/v1/forecast"
        
        params = {
            "latitude": 43.6108, "longitude": 3.8767,
            "hourly": "temperature_2m,wind_speed_10m,precipitation",
            "start_date": date_target.strftime("%Y-%m-%d"),
            "end_date": date_target.strftime("%Y-%m-%d")
        }
        if not is_past: params["timezone"] = "UTC"

        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            df = pd.DataFrame(r.json()['hourly'])
            df['ds'] = pd.to_datetime(df['time'])
            return df
        except:
            # Fallback
            dates = pd.date_range(start=date_target, periods=24, freq='h')
            return pd.DataFrame({'ds': dates, 'temperature_2m': 12, 'wind_speed_10m': 10, 'precipitation': 0})

    def run_recursive_prediction(self):
        print(f" Démarrage du mode RÉCURSIF HYBRIDE (J-1 simulé + J-14 réel)...")
        print(f" Objectif : Atteindre le {self.real_tomorrow}")

        # 1. Chargement
        fe = FeatureEngineering()
        df_history = fe.create_dataset()
        df_history = df_history.sort_values(['counter_id', 'ds'])

        # Mémoire (Historique complet)
        memory = df_history.set_index(['counter_id', 'ds'])['count'].to_dict()

        # 2. Initialisation de la boucle
        last_known_date = df_history['ds'].max()
        current_target_date = last_known_date + timedelta(days=1)
        current_target_date = current_target_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # 3. Chargement Modèle
        try:
            model = joblib.load(self.model_path)
            model_cols = model.get_booster().feature_names
        except:
            print(" Modèle introuvable. Lancez l'entraînement !"); return

        # === BOUCLE ===
        while current_target_date.date() <= self.real_tomorrow:
            print(f" Calcul pour le : {current_target_date.date()} ...")
            
            df_weather = self.get_weather_data(current_target_date)
            
            counters = df_history['counter_id'].unique()
            unique_encoded = df_history[['counter_id', 'counter_id_encoded']].drop_duplicates()
            
            day_rows = []
            
            for counter in counters:
                encoded_id = unique_encoded[unique_encoded['counter_id'] == counter]['counter_id_encoded'].values[0]
                temp_df = df_weather.copy()
                temp_df['counter_id'] = counter
                temp_df['counter_id_encoded'] = encoded_id
                
                # --- RÉCUPÉRATION DES LAGS (HYBRIDE) ---
                def get_lag_from_memory(row, hours):
                    target_time = row['ds'] - timedelta(hours=hours)
                    # Si target_time est dans le trou de données, on récupère notre propre prédiction précédente
                    # Si target_time est il y a 2 semaines, on récupère la VRAIE donnée historique
                    return memory.get((counter, target_time), 0)

                # Court terme (sera souvent une prédiction simulée dans la boucle)
                temp_df['lag_24h'] = temp_df.apply(lambda r: get_lag_from_memory(r, 24), axis=1)
                temp_df['lag_48h'] = temp_df.apply(lambda r: get_lag_from_memory(r, 48), axis=1)
                
                # Long terme (sera souvent une donnée RÉELLE fiable)
                temp_df['lag_168h'] = temp_df.apply(lambda r: get_lag_from_memory(r, 168), axis=1) # 1 sem
                temp_df['lag_336h'] = temp_df.apply(lambda r: get_lag_from_memory(r, 336), axis=1) # 2 sem
                temp_df['lag_504h'] = temp_df.apply(lambda r: get_lag_from_memory(r, 504), axis=1) # 3 sem
                
                temp_df['mean_last_4_days'] = (temp_df['lag_24h'] + temp_df['lag_48h']) / 2

                day_rows.append(temp_df)
            
            df_day = pd.concat(day_rows, ignore_index=True)
            
            # Features Temporelles & Cycliques
            df_day['hour'] = df_day['ds'].dt.hour
            df_day['day_of_week'] = df_day['ds'].dt.dayofweek
            df_day['month'] = df_day['ds'].dt.month
            df_day['is_weekend'] = df_day['day_of_week'].isin([5, 6]).astype(int)
            fr_holidays = holidays.France()
            df_day['is_holiday'] = df_day['ds'].apply(lambda x: 1 if x in fr_holidays else 0)
            
            df_day['hour_sin'] = np.sin(2 * np.pi * df_day['hour'] / 24)
            df_day['hour_cos'] = np.cos(2 * np.pi * df_day['hour'] / 24)
            df_day['month_sin'] = np.sin(2 * np.pi * df_day['month'] / 12)
            df_day['month_cos'] = np.cos(2 * np.pi * df_day['month'] / 12)
            df_day['dow_sin'] = np.sin(2 * np.pi * df_day['day_of_week'] / 7)
            df_day['dow_cos'] = np.cos(2 * np.pi * df_day['day_of_week'] / 7)

            # Alignement & Prédiction
            for col in model_cols:
                if col not in df_day.columns: df_day[col] = 0
            
            preds = model.predict(df_day[model_cols])
            preds = [max(0, int(x)) for x in preds]
            
            # Mise à jour Mémoire & BDD
            df_day['predicted_values'] = preds
            for _, row in df_day.iterrows():
                memory[(row['counter_id'], row['ds'])] = row['predicted_values']
            
            df_export = df_day[['ds', 'counter_id', 'predicted_values']].rename(columns={'ds': 'datetime'})
            try:
                fe.db.push_data(df_export, "model_data")
            except Exception as e:
                print(f" Erreur BDD : {e}")

            current_target_date += timedelta(days=1)

        print(" Terminé ! Le données futurs sont prédites.")

if __name__ == "__main__":
    p = Predictor()
    p.run_recursive_prediction()