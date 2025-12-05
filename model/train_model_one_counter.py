import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.plot import plot_cross_validation_metric
import itertools
import matplotlib.pyplot as plt

def train_prophet_optimized(file_path, counter_index=0):
    """
    Entraîne un modèle Prophet pour un compteur spécifique avec tuning d'hyperparamètres.
    """
    # 1. CHARGEMENT
    print("Chargement des données...")
    df = pd.read_csv(file_path, sep=';')
    
    # 2. SÉLECTION D'UN COMPTEUR
    # Prophet ne gère qu'une série à la fois. On prend le 1er compteur dispo ou celui indiqué.
    unique_ids = df['counter_id'].unique()
    selected_id = unique_ids[counter_index]
    print(f" Traitement du compteur : {selected_id}")
    
    df_counter = df[df['counter_id'] == selected_id].copy()
    
    # 3. FORMATAGE PROPHET
    # Prophet exige strictement les colonnes 'ds' et 'y'
    # Notre pipeline a déjà créé 'ds', mais la cible s'appelle 'count' -> on renomme en 'y'
    df_prophet = df_counter.rename(columns={'count': 'y'})
    
    # On garde uniquement les colonnes utiles
    regressors = ['temperature_2m', 'precipitation', 'wind_speed_10m', 'is_holiday', 'is_weekend']
    df_prophet = df_prophet[['ds', 'y'] + regressors]

    # ---------------------------------------------------------
    # ÉTAPE 4 : GRID SEARCH (Recherche des meilleurs hyperparamètres)
    # ---------------------------------------------------------
    print("🔍 Démarrage de l'optimisation des hyperparamètres...")
    
    # Grille de paramètres à tester (Tu peux en ajouter, attention au temps de calcul !)
    param_grid = {  
        'changepoint_prior_scale': [0.01, 0.1, 0.5], # Flexibilité de la tendance (Gère les ruptures)
        'seasonality_prior_scale': [1.0, 10.0],      # Force de la saisonnalité
    }

    # Génération de toutes les combinaisons
    all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]
    rmses = []  # Pour stocker les erreurs

    # Boucle sur les combinaisons
    for params in all_params:
        m = Prophet(**params, 
                    yearly_seasonality=True, # On force l'annuel car on a 1 an de données
                    weekly_seasonality=True,
                    daily_seasonality=True)
        
        # Ajout des régresseurs externes (Météo & Calendrier)
        for reg in regressors:
            m.add_regressor(reg)

        m.fit(df_prophet)

        # Cross Validation rapide (Rolling Window)
        # initial: taille historique training, period: fréquence cutoffs, horizon: prédiction
        df_cv = cross_validation(m, initial='180 days', period='30 days', horizon='7 days', parallel="processes")
        df_p = performance_metrics(df_cv, rolling_window=1)
        rmses.append(df_p['rmse'].values[0])

    # Meilleur résultat
    best_params = all_params[np.argmin(rmses)]
    print(f" Meilleurs paramètres trouvés : {best_params}")

    # ---------------------------------------------------------
    # ÉTAPE 5 : ENTRAINEMENT FINAL AVEC MEILLEURS PARAMS
    # ---------------------------------------------------------
    m_final = Prophet(**best_params,
                      yearly_seasonality=True,
                      weekly_seasonality=True,
                      daily_seasonality=True)
    
    for reg in regressors:
        m_final.add_regressor(reg)
        
    m_final.fit(df_prophet)

    # ---------------------------------------------------------
    # ÉTAPE 6 : PRÉDICTION ET VISUALISATION
    # ---------------------------------------------------------
    # Création du dataframe futur (ex: +7 jours)
    # CORRECTION 1 : Utiliser 'h' (minuscule) au lieu de 'H'
    future = m_final.make_future_dataframe(periods=24*7, freq='h')
    
    # Remplissage des régresseurs pour le futur
    for reg in regressors:
        # On remplit le futur avec la dernière valeur connue
        last_val = df_prophet[reg].iloc[-1]
        
        # CORRECTION 2 : Remplacer .fillna(method='ffill') par .ffill()
        future[reg] = df_prophet[reg].reindex(range(len(future))).ffill().fillna(last_val)

    forecast = m_final.predict(future)

    # Visualisation 1 : Composantes (Tendance, Hebdo, Annuel)
    print("Génération des graphiques...")
    fig1 = m_final.plot_components(forecast)
    plt.show()

    # Visualisation 2 : Cross Validation Metrics (Erreur en fonction de l'horizon)
    # On refait une CV sur le modèle final pour le plot
    df_cv_final = cross_validation(m_final, initial='180 days', period='30 days', horizon='7 days')
    fig2 = plot_cross_validation_metric(df_cv_final, metric='rmse')
    plt.title("Erreur RMSE selon l'horizon de prévision (heures)")
    plt.show()
    
    return m_final, forecast

# --- EXECUTION ---
if __name__ == "__main__":
    # Assure-toi que le fichier existe (celui généré à l'étape précédente)
    model, preds = train_prophet_optimized('data/train_data.csv')