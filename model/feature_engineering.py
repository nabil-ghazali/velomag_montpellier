import pandas as pd
import numpy as np
import holidays

def pipeline_feature_engineering_j_plus_1(df_input, df_meteo=None):
    """
    Prépare les données pour une prédiction à J+1 (Horizon 24h).
    Aucun lag inférieur à 24h pour éviter le Data Leakage.
    
    Args:
        df_input: DataFrame ['ds', 'counter_id', 'count']
        df_meteo: (Optionnel) DataFrame ['ds', 'temp', 'rain', ...] 
                  doit être horaire et synchronisé.
    """
    # 1. Copie de sécurité
    df = df_input.copy()
    df['ds'] = pd.to_datetime(df['ds'])
    
    # ---------------------------------------------------------
    # ÉTAPE 1 : RESAMPLING (Garantir une grille horaire parfaite)
    # ---------------------------------------------------------
    # Indispensable pour que le .shift(24) tombe exactement sur la veille
    df_list = []
    for counter in df['counter_id'].unique():
        # On isole chaque compteur
        temp = df[df['counter_id'] == counter].set_index('ds')
        # On force une ligne par heure. Interpolation pour les petits trous.
        temp = temp.resample('h').interpolate(method='linear', limit=2)
        # On remplit les gros trous restants par 0 (nuit/panne longue)
        temp['count'] = temp['count'].fillna(0)
        temp['counter_id'] = counter
        df_list.append(temp.reset_index())
        
    df = pd.concat(df_list, ignore_index=True)

    # ---------------------------------------------------------
    # ÉTAPE 2 : FUSION MÉTÉO (Si disponible)
    # ---------------------------------------------------------
    # Si tu as de la météo, c'est ici qu'on la joint. 
    # Pour J+1, on utilise les PREVISIONS météo pour le futur, 
    # et l'HISTORIQUE météo pour l'entrainement.
    if df_meteo is not None:
        df_meteo['ds'] = pd.to_datetime(df_meteo['ds'])
        df = pd.merge(df, df_meteo, on='ds', how='left')
        # Remplissage basique si météo manquante
        df.fillna({'temp': 15, 'rain': 0}, inplace=True) 

    # ---------------------------------------------------------
    # ÉTAPE 3 : FEATURES TEMPORELLES & CYCLIQUES
    # ---------------------------------------------------------
    # Variables brutes
    df['hour'] = df['ds'].dt.hour
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year # Utile si tendance long terme
    
    # Is_Weekend (0=Lundi, 5=Samedi, 6=Dimanche)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # Encodage Cyclique (Pour que 23h soit proche de 00h)
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

    # ---------------------------------------------------------
    # ÉTAPE 4 : CALENDRIER & VACANCES
    # ---------------------------------------------------------
    fr_holidays = holidays.France()
    df['is_holiday'] = df['ds'].apply(lambda x: 1 if x in fr_holidays else 0)
    
    # Optionnel : Ajouter une colonne "Jour après férié" (souvent calme)
    # df['is_post_holiday'] = df['is_holiday'].shift(24).fillna(0) 

    # ---------------------------------------------------------
    # ÉTAPE 5 : LAGS "DISTANTS" (Compatible J+1)
    # ---------------------------------------------------------
    # On trie impérativement pour que les shifts soient cohérents
    df = df.sort_values(['counter_id', 'ds'])
    
    # Lag 24h : La donnée la plus récente disponible pour demain 8h, c'est hier 8h.
    df['lag_24h'] = df.groupby('counter_id')['count'].shift(24)
    
    # Lag 48h : Avant-hier (utile pour stabiliser si hier était atypique)
    df['lag_48h'] = df.groupby('counter_id')['count'].shift(48)
    
    # Lag 1 Semaine : La référence absolue pour le vélo (Mardi vs Mardi)
    df['lag_168h'] = df.groupby('counter_id')['count'].shift(168)
    
    # Moyenne glissante sur la même heure les 4 derniers jours (lag 24, 48, 72, 96)
    # Cela capture le "niveau" récent d'activité sans fuite de données
    # Astuce : On shift d'abord de 24h, puis on fait la moyenne rolling
    grouped = df.groupby('counter_id')['count']
    df['mean_last_4_days'] = grouped.shift(24).rolling(window=4).mean()

    # ---------------------------------------------------------
    # ÉTAPE 6 : NETTOYAGE FINAL
    # ---------------------------------------------------------
    # Encodage numérique du compteur pour XGBoost
    df['counter_id_encoded'] = df['counter_id'].astype('category').cat.codes
    
    # Les lags créent des NaN au tout début de l'historique (les 7 premiers jours)
    # On les supprime car on ne peut pas entrainer dessus
    df = df.dropna()
    
    return df