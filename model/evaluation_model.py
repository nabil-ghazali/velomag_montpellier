import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import os

def evaluer_tous_les_compteurs(path_train_data):
    # 1. Chargement global
    print("Chargement des données...")
    df_global = pd.read_csv(path_train_data, sep=';')
    
    # Création du dossier pour les graphiques
    os.makedirs("output/plots", exist_ok=True)
    
    unique_ids = df_global['counter_id'].unique()
    print(f" Démarrage de l'évaluation pour {len(unique_ids)} compteurs...\n")
    
    resultats = [] # Pour stocker les scores
    
    for i, counter_id in enumerate(unique_ids):
        print(f"[{i+1}/{len(unique_ids)}] Traitement de {counter_id}...")
        
        # --- PRÉPARATION ---
        df_reel = df_global[df_global['counter_id'] == counter_id].copy()
        df_prophet = df_reel.rename(columns={'count': 'y'})
        df_prophet['ds'] = pd.to_datetime(df_prophet['ds']) # Conversion date importante
        
        regressors = ['temperature_2m', 'precipitation', 'wind_speed_10m', 'is_holiday', 'is_weekend']
        
        # --- ENTRAÎNEMENT & PRÉDICTION (Sur l'historique) ---
        # On désactive le log (verbose=False) pour ne pas polluer la console
        m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
        for reg in regressors:
            m.add_regressor(reg)
            
        m.fit(df_prophet)
        
        forecast = m.predict(df_prophet)
        df_pred = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        
        # --- SCORES ---
        df_compare = pd.merge(df_prophet[['ds', 'y']], df_pred, on='ds')
        y_true = df_compare['y']
        y_pred = df_compare['yhat'].clip(lower=0)
        
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        
        # On calcule aussi la moyenne du trafic pour relativiser l'erreur
        trafic_moyen = y_true.mean()
        erreur_pourcentage = (mae / trafic_moyen) * 100 if trafic_moyen > 0 else 0
        
        resultats.append({
            'counter_id': counter_id,
            'MAE': round(mae, 2),
            'RMSE': round(rmse, 2),
            'Trafic_Moyen': round(trafic_moyen, 0),
            'Erreur_%': round(erreur_pourcentage, 1)
        })
        
        # --- SAUVEGARDE DU GRAPHIQUE ---
        last_date = df_compare['ds'].max()
        start_zoom = last_date - pd.Timedelta(days=15)
        subset = df_compare[df_compare['ds'] >= start_zoom]
        
        plt.figure(figsize=(12, 6))
        plt.plot(subset['ds'], subset['y'], label='Réel', color='black', alpha=0.6)
        plt.plot(subset['ds'], subset['yhat'], label='Prédit', color='#0072B2', linewidth=2)
        plt.fill_between(subset['ds'], subset['yhat_lower'], subset['yhat_upper'], color='#0072B2', alpha=0.2)
        plt.title(f"Compteur : {counter_id}\nMAE: {mae:.2f} | RMSE: {rmse:.2f}")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Sauvegarde au lieu d'afficher
        filename = counter_id.replace(':', '_').replace('/', '_') # Nettoyage nom de fichier
        plt.savefig(f"output/plots/plot_{filename}.png")
        plt.close() # Ferme la figure pour libérer la mémoire

    # --- SYNTHÈSE FINALE ---
    df_res = pd.DataFrame(resultats)
    df_res = df_res.sort_values(by='Erreur_%', ascending=True) # Les meilleurs en premier
    
    print("\n CLASSEMENT DES COMPTEURS (Du mieux prédit au moins bien) :")
    print(df_res.to_string(index=False))
    
    # Sauvegarde du tableau
    df_res.to_csv("output/performance_globale.csv", index=False, sep=';')
    print("\n Analyse terminée ! Voir 'output/performance_globale.csv' et le dossier 'output/plots/'")

if __name__ == "__main__":
    evaluer_tous_les_compteurs('data_files/train_data.csv')