import pandas as pd
from prophet import Prophet
import os

def train_predict_all_counters(file_path, horizon_hours=24):
    """
    Entraîne un modèle par compteur et génère un fichier de prédiction global.
    """
    print("Chargement des données...")
    df = pd.read_csv(file_path, sep=';')
    
    # Liste pour stocker les résultats de tous les compteurs
    all_forecasts = []
    
    # Liste des IDs uniques
    unique_ids = df['counter_id'].unique()
    
    print(f" Début de l'entraînement pour {len(unique_ids)} compteurs...")
    
    for i, counter_id in enumerate(unique_ids):
        print(f"[{i+1}/{len(unique_ids)}] Traitement compteur : {counter_id}")
        
        # 1. Isolation des données du compteur
        df_counter = df[df['counter_id'] == counter_id].copy()
        
        # 2. Préparation Prophet
        df_prophet = df_counter.rename(columns={'count': 'y'})
        regressors = ['temperature_2m', 'precipitation', 'wind_speed_10m', 'is_holiday', 'is_weekend']
        
        # 3. Configuration du modèle (Paramètres optimisés ou standards)
        # On utilise des valeurs standards robustes ici
        m = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=True,
            changepoint_prior_scale=0.05 # Souplesse moyenne
        )
        
        for reg in regressors:
            m.add_regressor(reg)
            
        # 4. Entrainement
        m.fit(df_prophet)
        
        # 5. Prédiction J+1
        future = m.make_future_dataframe(periods=horizon_hours, freq='h')
        
        # Gestion des régresseurs futurs (Propagation de la dernière valeur connue)
        # En prod réelle, tu ferais un merge avec les prévisions météo ici
        for reg in regressors:
            last_val = df_prophet[reg].iloc[-1]
            future[reg] = df_prophet[reg].reindex(range(len(future))).ffill().fillna(last_val)
            
        forecast = m.predict(future)
        
        # 6. Nettoyage et Stockage
        # On garde juste la date, la prédiction (yhat) et l'ID
        forecast_clean = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(horizon_hours).copy()
        forecast_clean['counter_id'] = counter_id
        
        all_forecasts.append(forecast_clean)

    # 7. Concatenation finale
    print(" Sauvegarde des résultats...")
    df_final_forecast = pd.concat(all_forecasts, ignore_index=True)
    
    # On arrondit les prédictions (pas de demi-vélo) et on met 0 si négatif
    df_final_forecast['yhat'] = df_final_forecast['yhat'].clip(lower=0).round().astype(int)
    
    os.makedirs("output/", exist_ok=True)
    output_path = "output/predictions_j_plus_1.csv"
    df_final_forecast.to_csv(output_path, index=False, sep=';')
    print(f" Terminé ! Prédictions sauvegardées dans {output_path}")
    
    return df_final_forecast

if __name__ == "__main__":
    train_predict_all_counters('data_files/train_data.csv')