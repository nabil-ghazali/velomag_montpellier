import pandas as pd
import numpy as np
import holidays

def pipeline_feature_engineering_finale(df_input):
    """
    Prépare les données FUSIONNÉES pour une prédiction à J+1.
    Suppose que l'entrée contient déjà la météo et les compteurs.
    """
    # 1. Copie de sécurité
    df = df_input.copy()
    
    # ---------------------------------------------------------
    # ÉTAPE 0 : STANDARDISATION DES NOMS
    # ---------------------------------------------------------
    # Ton merge a produit 'datetime' et 'intensity', mais la pipeline aime 'ds' et 'count'
    rename_dict = {
        'datetime': 'ds', 
        'intensity': 'count'
    }
    df = df.rename(columns=rename_dict)
    df['ds'] = pd.to_datetime(df['ds'])
    
    # ---------------------------------------------------------
    # ÉTAPE 1 : NETTOYAGE & RESAMPLING (Robustesse)
    # ---------------------------------------------------------
    # Même si le merge a fait du bon boulot, on s'assure qu'il n'y a pas de trous
    # pour que les LAGS (décalages) soient mathématiquement justes.
    
    df_list = []
    # Liste des colonnes météo à propager (Forward Fill)
    weather_cols = ['temperature_2m', 'wind_speed_10m', 'precipitation']
    
    for counter in df['counter_id'].unique():
        temp = df[df['counter_id'] == counter].set_index('ds')
        
        # On force la grille horaire
        temp = temp.resample('h').asfreq()
        
        # Remplissage intelligent :
        # 1. Le comptage vélo (count) -> 0 si manquant (hypothèse : panne ou nuit calme)
        temp['count'] = temp['count'].fillna(0)
        
        # 2. La météo -> On propage la dernière valeur connue (la météo change peu en 1h)
        # On vérifie d'abord si les colonnes existent
        existing_weather_cols = [c for c in weather_cols if c in temp.columns]
        temp[existing_weather_cols] = temp[existing_weather_cols].ffill().fillna(0)
        
        # 3. Les infos statiques (lat, lon, ID) -> On reprend la valeur du compteur
        temp['counter_id'] = counter
        # (Optionnel : remettre lat/lon si besoin)
        
        df_list.append(temp.reset_index())
        
    df = pd.concat(df_list, ignore_index=True)

    # ---------------------------------------------------------
    # ÉTAPE 2 : FEATURES TEMPORELLES & CYCLIQUES
    # ---------------------------------------------------------
    df['hour'] = df['ds'].dt.hour
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year 
    
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # Encodage Cyclique
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

    # ---------------------------------------------------------
    # ÉTAPE 3 : CALENDRIER (Jours Fériés)
    # ---------------------------------------------------------
    fr_holidays = holidays.France()
    df['is_holiday'] = df['ds'].apply(lambda x: 1 if x in fr_holidays else 0)

    # ---------------------------------------------------------
    # ÉTAPE 4 : LAGS (La Mémoire du Modèle)
    # ---------------------------------------------------------
    df = df.sort_values(['counter_id', 'ds'])
    
    # Lag 24h (Hier même heure)
    df['lag_24h'] = df.groupby('counter_id')['count'].shift(24)
    
    # Lag 48h (Avant-hier même heure)
    df['lag_48h'] = df.groupby('counter_id')['count'].shift(48)
    
    # Lag 1 semaine (Même jour semaine dernière)
    df['lag_168h'] = df.groupby('counter_id')['count'].shift(168)
    
    # Moyenne glissante sur les 4 derniers jours à la même heure
    grouped = df.groupby('counter_id')['count']
    df['mean_last_4_days'] = grouped.shift(24).rolling(window=4).mean()

    # ---------------------------------------------------------
    # ÉTAPE 5 : NETTOYAGE FINAL
    # ---------------------------------------------------------
    # Encodage ID pour XGBoost
    df['counter_id_encoded'] = df['counter_id'].astype('category').cat.codes
    
    # Suppression des NaN générés par les lags (les 7 premiers jours de l'historique)
    df = df.dropna()
    
    return df

# --- EXECUTION ---
if __name__ == "__main__":
    # 1. On charge le fichier FUSIONNÉ (attention au séparateur !)
    # Si ton merge précédent a utilisé sep=';', on le garde ici.
    print("Chargement des données fusionnées...")
    df_merged = pd.read_csv("data/merged_data.csv", sep=';')
    
    # 2. On lance la pipeline
    print("Génération des features...")
    df_features = pipeline_feature_engineering_finale(df_merged)
    
    # 3. Vérification
    print(df_features.head())
    print(f"Dimensions finales : {df_features.shape}")
    
    # 4. Sauvegarde finale pour l'entraînement
    df_features.to_csv("data/train_data.csv", index=False, sep=';')
    print("Fichier prêt pour l'entraînement : data/processede/train_data_chaima.csv")