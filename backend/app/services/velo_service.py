#velo_service: pipeline de préparation des données.
# services/velo_service.py

import pandas as pd
import numpy as np

# ---------------------------------------------------------
# 1️ Nettoyage des données
# ---------------------------------------------------------
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage des données vélo :
    - suppression des doublons
    - correction des types
    - création colonnes temporelles (hour, weekday, is_weekend)
    - remplacement des outliers (intensity > 300 par médiane du compteur)
    """
    # Supprimer doublons exacts
    df = df.drop_duplicates()
    
    # Correction des types
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['intensity'] = pd.to_numeric(df['intensity'], errors='coerce')
    
    # Colonnes temporelles
    df['hour'] = df['datetime'].dt.hour
    df['weekday'] = df['datetime'].dt.weekday
    df['is_weekend'] = df['weekday'].isin([5,6]).astype(int)
    
    # Remplacement outliers
    df['intensity'] = df.groupby('counter_id')['intensity'].transform(
        lambda x: np.where(x > 300, x.median(), x)
    )
    
    return df


# ---------------------------------------------------------
# 2️ Calcul durée collecte et catégorisation compteurs
# ---------------------------------------------------------
def categorize_counters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule la durée de collecte pour chaque compteur et crée une colonne 'category'
    """
    duration = df.groupby("counter_id")["datetime"].agg(["min", "max"])
    duration["days"] = (duration["max"] - duration["min"]).dt.days + 1

    def categorize(days):
        if days >= 365:
            return "plus de 365 jours"
        elif days >= 200:
            return "200 jours ou plus"
        elif days < 100:
            return "moins de 100 jours"
        else:
            return "autres"

    duration["category"] = duration["days"].apply(categorize)
    return duration


# ---------------------------------------------------------
# 3️ Feature engineering
# ---------------------------------------------------------
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajout de features pour analyse ou ML
    """
    # Moyenne glissante sur 3 heures
    df['rolling_3h'] = df.groupby('counter_id')['intensity'].transform(
        lambda x: x.rolling(3, min_periods=1).mean()
    )
    
    # Décalages temporels
    df['lag_1h'] = df.groupby('counter_id')['intensity'].shift(1)
    df['lag_24h'] = df.groupby('counter_id')['intensity'].shift(24)
    
    # Taux de variation
    df['pct_change_1h'] = df.groupby('counter_id')['intensity'].pct_change()
    
    return df


# ---------------------------------------------------------
# 4️ Fusion avec météo
# ---------------------------------------------------------
def merge_with_weather(df: pd.DataFrame, df_meteo: pd.DataFrame) -> pd.DataFrame:
    """
    Merge des données vélo avec les données météo sur la colonne 'datetime'
    """
    # Uniformiser timezone
    df['datetime'] = df['datetime'].dt.tz_convert(None)
    df_meteo['datetime'] = pd.to_datetime(df_meteo['datetime']).dt.tz_localize(None)
    
    df_merged = df.merge(df_meteo, on='datetime', how='left')
    return df_merged


# ---------------------------------------------------------
# 5️ Export des datasets
# ---------------------------------------------------------
def export_data(df: pd.DataFrame,
                path_clean="../data/processed/data_clean.csv",
                path_features="../data/processed/features.csv"):
    """
    Sauvegarde du dataset clean et des features
    """
    df.to_csv(path_clean, index=False)
    features = df[['counter_id', 'datetime', 'intensity', 'rolling_3h', 'lag_1h', 'lag_24h', 'pct_change_1h']]
    features.to_csv(path_features, index=False)
    print(f"Datasets exportés : {path_clean}, {path_features}")


# ---------------------------------------------------------
# 6️ Pipeline complet
# ---------------------------------------------------------
def prepare_velo_pipeline(df_velo: pd.DataFrame, df_meteo: pd.DataFrame = None) -> dict:
    """
    Pipeline complet :
    - Nettoyage
    - Catégorisation compteurs
    - Feature engineering
    - Fusion météo (optionnel)
    - Export CSV

    Retourne un dict avec :
    - df_clean
    - df_365
    - df_200
    - df_100
    """
    df_clean = clean_data(df_velo)
    
    # Catégorisation compteurs
    duration = categorize_counters(df_clean)
    
    # Filtrage compteurs fiables
    counters_365 = duration[duration['category'] == "plus de 365 jours"].index.tolist()
    counters_200 = duration[duration['category'] == "200 jours ou plus"].index.tolist()
    counters_100 = duration[duration['category'] == "moins de 100 jours"].index.tolist()
    
    df_365 = df_clean[df_clean['counter_id'].isin(counters_365)].copy()
    df_200 = df_clean[df_clean['counter_id'].isin(counters_200)].copy()
    df_100 = df_clean[df_clean['counter_id'].isin(counters_100)].copy()
    
    # Feature engineering
    df_clean = add_features(df_clean)
    df_365 = add_features(df_365)
    df_200 = add_features(df_200)
    df_100 = add_features(df_100)
    
    # Fusion météo si fourni
    if df_meteo is not None:
        df_clean = merge_with_weather(df_clean, df_meteo)
        df_365 = merge_with_weather(df_365, df_meteo)
        df_200 = merge_with_weather(df_200, df_meteo)
        df_100 = merge_with_weather(df_100, df_meteo)
    
    # Export
    export_data(df_clean)
    
    return {
        'df_clean': df_clean,
        'df_365': df_365,
        'df_200': df_200,
        'df_100': df_100,
        'duration': duration
    }


# import pandas as pd

# def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
#     df["hour"] = df["datetime"].dt.hour
#     df["weekday"] = df["datetime"].dt.weekday
#     df["is_weekend"] = df["weekday"].isin([5,6]).astype(int)
#     return df

# def compute_rolling_lags(df: pd.DataFrame) -> pd.DataFrame:
#     df["rolling_3h"] = df.groupby("counter_id")["intensity"].transform(lambda x: x.rolling(3, min_periods=1).mean())
#     df["lag_1h"] = df.groupby("counter_id")["intensity"].shift(1)
#     df["lag_24h"] = df.groupby("counter_id")["intensity"].shift(24)
#     return df

# def categorize_counters(df: pd.DataFrame) -> pd.DataFrame:
#     duration = df.groupby("counter_id")["datetime"].agg(["min", "max"])
#     duration["days"] = (duration["max"] - duration["min"]).dt.days + 1
    
#     def categorize(days):
#         if days >= 200:
#             return "plus de 200 jours"
#         elif days >= 162:
#             return "162 jours ou plus"
#         elif days < 100:
#             return "moins de 100 jours"
#         else:
#             return "autres"
    
#     duration["category"] = duration["days"].apply(categorize)
#     return duration