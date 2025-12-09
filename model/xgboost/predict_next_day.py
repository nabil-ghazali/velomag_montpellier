import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import requests
from datetime import timedelta

def predict_next_day(path_data="data/processed/train_data_xgboost.csv", path_model="model/xgboost/saved/xgboost_velo.pkl"):
    print("🔮 Démarrage du moteur de prédiction...")

    # 1. CHARGEMENT
    try:
        df_history = pd.read_csv(path_data, sep=';')
        model = joblib.load(path_model)
        print("✅ Modèle et Historique chargés.")
    except Exception as e:
        print(f"❌ Erreur de chargement : {e}")
        return

    # Conversion date
    df_history['datetime'] = pd.to_datetime(df_history['datetime'])
    df_history = df_history.sort_values('datetime')

    # 2. DÉTERMINER LA DATE CIBLE (J+1 par rapport à la fin des données)
    last_date_in_data = df_history['datetime'].max()
    target_date_start = last_date_in_data + timedelta(hours=1)
    # On veut prédire les 24 prochaines heures
    target_dates = pd.date_range(start=target_date_start, periods=24, freq='h')
    
    print(f"📅 Prédiction pour la journée du : {target_dates[0].strftime('%Y-%m-%d')}")

    # 3. CONSTRUCTION DU DATAFRAME "FUTUR"
    # On doit créer une ligne pour chaque heure et chaque compteur
    unique_counters = df_history['counter_id_encoded'].unique()
    
    future_rows = []
    
    # Pour chaque heure de demain
    for dt in target_dates:
        # Pour chaque compteur
        for counter in unique_counters:
            
            # --- A. RÉCUPÉRATION DES LAGS (Le cœur du système) ---
            # Pour prédire demain 8h, j'ai besoin de la valeur d'aujourd'hui 8h (Lag 24)
            # Date de référence pour le Lag 24h
            ref_date_24h = dt - timedelta(hours=24)
            ref_date_1wk = dt - timedelta(days=7)
            
            # Recherche dans l'historique
            # On filtre sur le compteur et la date exacte
            # (En prod, on utiliserait une base de données SQL pour faire ça vite)
            hist_counter = df_history[df_history['counter_id_encoded'] == counter]
            
            val_lag_24 = hist_counter.loc[hist_counter['datetime'] == ref_date_24h, 'intensity']
            val_lag_1wk = hist_counter.loc[hist_counter['datetime'] == ref_date_1wk, 'intensity']
            
            # Sécurité si données manquantes (on prend la moyenne du compteur)
            lag_24 = val_lag_24.values[0] if len(val_lag_24) > 0 else hist_counter['intensity'].mean()
            lag_1wk = val_lag_1wk.values[0] if len(val_lag_1wk) > 0 else lag_24
            
            # Rolling mean 4 jours (approximation avec lag 24 si calcul trop lourd)
            rolling_4d = lag_24 

            # --- B. DONNÉES TEMPORELLES ---
            hour = dt.hour
            day_of_week = dt.dayofweek
            month = dt.month
            
            # --- C. MÉTÉO (SIMULATION PRÉVISION) ---
            # En prod, on appellerait l'API Open-Meteo ici.
            # Pour l'exemple, on prend la météo de la veille (méthode "naïve" souvent efficace)
            # ou des moyennes saisonnières.
            temp = 15.0 # Exemple : il fera 15 degrés
            rain = 0.0  # Pas de pluie
            wind = 10.0 # Vent moyen
            
            # --- D. FÉRIÉS ---
            # On vérifie si la date cible est dans ton fichier férié (simplifié ici à 0)
            is_holiday = 0 
            
            # Création de la ligne
            row = {
                'counter_id_encoded': counter,
                'hour': hour,
                'day_of_week': day_of_week,
                'month': month,
                'is_weekend': 1 if day_of_week >= 5 else 0,
                # Encodage Cyclique
                'hour_sin': np.sin(2 * np.pi * hour / 24),
                'hour_cos': np.cos(2 * np.pi * hour / 24),
                'month_sin': np.sin(2 * np.pi * month / 12),
                'month_cos': np.cos(2 * np.pi * month / 12),
                'dow_sin': np.sin(2 * np.pi * day_of_week / 7),
                'dow_cos': np.cos(2 * np.pi * day_of_week / 7),
                # Météo
                'temperature_2m': temp,
                'precipitation': rain,
                'wind_speed_10m': wind,
                'lat': 43.6, # À affiner selon compteur si dispo
                'lon': 3.8,
                # Lags & Events
                'lag_24h': lag_24,
                'lag_48h': lag_24, # Approximation si on n'a pas tout l'historique chargé
                'lag_1week': lag_1wk,
                'rolling_mean_4d': rolling_4d,
                'is_holiday': is_holiday,
                'is_major_event': 0 # À connecter à ton fichier event scrapé
            }
            future_rows.append(row)

    # Création du DataFrame X_future
    X_future = pd.DataFrame(future_rows)
    
    # On s'assure d'avoir les mêmes colonnes que lors de l'entraînement
    # XGBoost est très strict sur l'ordre des colonnes
    cols_when_model_built = model.get_booster().feature_names
    
    # On ajoute les colonnes manquantes avec 0 (sécurité)
    for col in cols_when_model_built:
        if col not in X_future.columns:
            X_future[col] = 0
            
    # On réordonne
    X_future = X_future[cols_when_model_built]

    # 4. PRÉDICTION
    print("🚀 Calcul des prédictions...")
    preds = model.predict(X_future)
    preds = np.clip(preds, 0, None).astype(int)
    
    # 5. RÉSULTAT
    X_future['predicted_intensity'] = preds
    X_future['date'] = target_dates[0].date()
    
    # Agrégation pour affichage propre (Total par heure sur la ville)
    print("\n🚲 PRÉVISIONS POUR DEMAIN (Somme de tous les compteurs) :")
    summary = X_future.groupby('hour')['predicted_intensity'].sum()
    print(summary)
    
    # Sauvegarde
    X_future.to_csv("output/previsions_demain.csv", index=False, sep=';')
    print("\n💾 Détail sauvegardé dans 'output/previsions_demain.csv'")

if __name__ == "__main__":
    predict_next_day()