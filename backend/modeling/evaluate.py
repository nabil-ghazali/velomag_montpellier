# backend/modeling/evaluate.py
import joblib
import pandas as pd
from sklearn.metrics import mean_absolute_error, r2_score
from modeling.features import FeatureEngineering

# Charger le modèle **une seule fois** pour éviter de le recharger chaque minute
#MODEL_PATH = "backend/model/model_velo.pkl"
MODEL_PATH = "model/model_velo.pkl"

try:
    model = joblib.load(MODEL_PATH)
    print(f" Modèle XGBoost chargé depuis : {MODEL_PATH}")
except Exception as e:
    print(f" ERREUR : impossible de charger le modèle : {e}")
    model = None

# --- CHARGEMENT DU DATASET ---
def get_dataset():
    fe = FeatureEngineering()
    df = fe.create_dataset()
    if df.empty:
        raise ValueError("Dataset vide ! Vérifie la base de données.")

    # Même features que dans train_model()
    features_cols = [
        'counter_id_encoded', 'hour_sin', 'hour_cos', 
        'month_sin', 'month_cos', 'dow_sin', 'dow_cos',
        'is_weekend', 'is_holiday',
        'temperature_2m', 'wind_speed_10m', 'precipitation',
        'lag_24h', 'lag_48h', 'lag_168h', 'mean_last_4_days'
    ]
    target_col = 'count'

    X = df[features_cols]
    y = df[target_col]

    # Split temporel : 80% / 20%
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    return X_train, X_test, y_train, y_test

# --- MÉTRIQUES ---
def get_mae():
    if model is None:
        return None

    _, X_test, _, y_test = get_dataset()
    y_pred = model.predict(X_test)
    y_pred = [max(0, x) for x in y_pred]  # éviter valeurs négatives

    mae = mean_absolute_error(y_test, y_pred)
    return mae

def get_r2():
    if model is None:
        return None

    _, X_test, _, y_test = get_dataset()
    y_pred = model.predict(X_test)
    y_pred = [max(0, x) for x in y_pred]

    r2 = r2_score(y_test, y_pred)
    return r2


if __name__ == "__main__":
    print("MAE :", get_mae())
    print("R2  :", get_r2())







# import pandas as pd
# from sklearn.metrics import mean_absolute_error, r2_score
# from .features import FeatureEngineering

# # --- Chargement des données et préparation ---
# def get_dataset():
#     fe = FeatureEngineering()
#     df = fe.create_dataset()
#     if df.empty:
#         raise ValueError("Dataset vide ! Vérifie la DB.")
#     # Sélection des features et target (comme dans train.py)
#     features_cols = [
#         'counter_id_encoded', 'hour_sin', 'hour_cos', 
#         'month_sin', 'month_cos', 'dow_sin', 'dow_cos',
#         'is_weekend', 'is_holiday',
#         'temperature_2m', 'wind_speed_10m', 'precipitation',
#         'lag_24h', 'lag_48h', 'lag_168h', 'mean_last_4_days'
#     ]
#     target_col = 'count'

#     missing = [c for c in features_cols if c not in df.columns]
#     if missing:
#         raise ValueError(f"Colonnes manquantes dans le dataset : {missing}")

#     X = df[features_cols]
#     y = df[target_col]

#     # Split temporel (80% train / 20% test)
#     split_idx = int(len(df) * 0.8)
#     X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
#     y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

#     return X_train, X_test, y_train, y_test

# # --- Fonctions métriques ---
# def get_mae():
#     _, X_test, _, y_test = get_dataset()
#     # Ici on simule avec les valeurs réelles (pour metrics API)
#     y_pred = y_test  # dans ton cas tu peux utiliser le dernier modèle
#     mae = mean_absolute_error(y_test, y_pred)
#     return mae

# def get_r2():
#     _, X_test, _, y_test = get_dataset()
#     # Simulé avec les valeurs réelles
#     y_pred = y_test
#     r2 = r2_score(y_test, y_pred)
#     return r2

# # --- Test rapide ---
# if __name__ == "__main__":
#     mae = get_mae()
#     r2 = get_r2()
#     print(f"MAE : {mae}")
#     print(f"R2  : {r2}")