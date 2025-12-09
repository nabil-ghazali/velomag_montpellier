# backend/app/utils/loader.py

import pandas as pd
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------
# Chemins par défaut
# ---------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data/processed")
DATA_CLEAN_PATH = os.path.join(DATA_DIR, "data_clean.csv")
FEATURES_PATH = os.path.join(DATA_DIR, "features.csv")

# ---------------------------------------------------------
# Cache en mémoire : pour éviter de relire les CSV à chaque appel.
# ---------------------------------------------------------
_cache = {}


# ---------------------------------------------------------
# Chargement CSV
# ---------------------------------------------------------
def load_csv(path: str = DATA_CLEAN_PATH, use_cache: bool = True) -> pd.DataFrame:
    """
    Charge un CSV depuis le disque ou le cache mémoire.
    """
    global _cache
    
    if use_cache and path in _cache:
        return _cache[path].copy()
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"Le fichier n'existe pas : {path}")
    
    df = pd.read_csv(path, parse_dates=['datetime'])
    
    # Sauvegarde en cache
    _cache[path] = df.copy()
    
    return df


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def cache_clear():
    """Vide le cache mémoire"""
    global _cache
    _cache = {}


def load_clean_data(use_cache: bool = True) -> pd.DataFrame:
    """Chargement rapide du dataset clean"""
    return load_csv(DATA_CLEAN_PATH, use_cache=use_cache)


def load_features(use_cache: bool = True) -> pd.DataFrame:
    """Chargement rapide du dataset features"""
    return load_csv(FEATURES_PATH, use_cache=use_cache)


def filter_by_counter(df: pd.DataFrame, counter_id: str) -> pd.DataFrame:
    """Filtrer le DataFrame pour un compteur spécifique"""
    return df[df['counter_id'] == counter_id].copy()


def last_n_days(df: pd.DataFrame, n: int = 7) -> pd.DataFrame:
    """Retourne les données des n derniers jours"""
    max_date = df['datetime'].max()
    min_date = max_date - timedelta(days=n)
    return df[df['datetime'] >= min_date].copy()


# import pandas as pd
# from datetime import datetime, timedelta
# import os

# def load_existing_csv(path: str) -> pd.DataFrame:
#     if os.path.exists(path):
#         df = pd.read_csv(path)
#         df["datetime"] = pd.to_datetime(df["datetime"])
#         return df
#     return pd.DataFrame()

# def update_csv(df_old: pd.DataFrame, df_new: pd.DataFrame, path: str):
#     if not df_new.empty:
#         df_final = pd.concat([df_old, df_new])
#         df_final = df_final.drop_duplicates(subset=["datetime", "counter_id"])
#         df_final.to_csv(path, index=False)
#         print(f"CSV mis à jour : {len(df_new)} nouvelles lignes")
#     else:
#         print("Aucune nouvelle donnée à ajouter")