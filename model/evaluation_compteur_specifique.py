import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

def evaluer_compteur_specifique(path_train_data, counter_index=0):
    # ---------------------------------------------------------
    # 1. PRÉPARATION DE df_reel (Depuis train_data.csv)
    # ---------------------------------------------------------
    print("Chargement des données réelles...")
    df_global = pd.read_csv(path_train_data, sep=';')
    
    # On sélectionne un compteur
    unique_ids = df_global['counter_id'].unique()
    if len(unique_ids) == 0:
        print("Erreur : Aucun compteur trouvé dans le fichier.")
        return
        
    counter_id = unique_ids[counter_index]
    print(f" Évaluation du compteur : {counter_id}")
    
    df_reel = df_global[df_global['counter_id'] == counter_id].copy()
    
    # Formatage pour Prophet
    df_prophet = df_reel.rename(columns={'count': 'y'})
    
    # --- CORRECTION ICI : ON FORCE LA CONVERSION EN DATE ---
    df_prophet['ds'] = pd.to_datetime(df_prophet['ds'])
    # -------------------------------------------------------

    regressors = ['temperature_2m', 'precipitation', 'wind_speed_10m', 'is_holiday', 'is_weekend']
    
    # ---------------------------------------------------------
    # 2. GÉNÉRATION DE df_pred (Sur l'historique !)
    # ---------------------------------------------------------
    print("Entraînement et prédiction sur l'historique...")
    m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    for reg in regressors:
        m.add_regressor(reg)
        
    m.fit(df_prophet)
    
    # On prédit sur l'historique pour comparer
    forecast = m.predict(df_prophet)
    
    df_pred = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()

    # ---------------------------------------------------------
    # 3. CALCUL DES MÉTRIQUES
    # ---------------------------------------------------------
    # Maintenant le merge va fonctionner car les deux 'ds' sont des dates
    df_compare = pd.merge(df_prophet[['ds', 'y']], df_pred, on='ds')
    
    # Calculs
    y_true = df_compare['y']
    y_pred = df_compare['yhat'].clip(lower=0)
    
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    
    print(f"\n RÉSULTATS POUR {counter_id}")
    print(f"   MAE  (Erreur Moyenne) : {mae:.2f}")
    print(f"   RMSE (Erreur Quadratique) : {rmse:.2f}")

    # ---------------------------------------------------------
    # 4. VISUALISATION
    # ---------------------------------------------------------
    last_date = df_compare['ds'].max()
    start_zoom = last_date - pd.Timedelta(days=15)
    
    subset = df_compare[df_compare['ds'] >= start_zoom]
    
    plt.figure(figsize=(14, 6))
    plt.plot(subset['ds'], subset['y'], label='Réel', color='black', alpha=0.6)
    plt.plot(subset['ds'], subset['yhat'], label='Prédit', color='#0072B2', linewidth=2)
    plt.fill_between(subset['ds'], subset['yhat_lower'], subset['yhat_upper'], color='#0072B2', alpha=0.2)
    
    plt.title(f"Réalité vs Modèle (15 derniers jours) - {counter_id}")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()

if __name__ == "__main__":
    evaluer_compteur_specifique('data_files/processed/features.csv', counter_index=0)