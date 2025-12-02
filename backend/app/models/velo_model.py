# backend/app/models/velo_model.py
#les modèles ML + toute logique de prédiction.
import pandas as pd

class Predictor:
    def __init__(self, model=None):
        self.model = model  # apres charger un modèle ML (pickle, joblib)
    
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Retourne des prédictions sur le dataset fourni
        """
        if self.model is None:
            # exemple dummy : prédiction = intensity moyenne
            df['prediction'] = df.groupby('counter_id')['intensity'].transform('mean')
        else:
            df['prediction'] = self.model.predict(df)
        return df
    
    
    
#     # backend/app/models/velo_model.py

# import pandas as pd
# import numpy as np
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.preprocessing import StandardScaler
# import joblib
# import os

# class Predictor:
#     """
#     Classe pour gérer les prédictions de trafic vélo.
#     Peut être étendue pour charger un vrai modèle ML.
#     """
#     def __init__(self, model_path: str = None):
#         """
#         Si un modèle est fourni via model_path, il sera chargé.
#         Sinon, les prédictions seront des valeurs dummy (moyenne).
#         """
#         self.model = None
#         self.scaler = None
        
#         if model_path and os.path.exists(model_path):
#             saved = joblib.load(model_path)
#             self.model = saved.get("model")
#             self.scaler = saved.get("scaler")
#             print(f"Modèle chargé depuis {model_path}")
#         else:
#             print("⚠ Aucun modèle trouvé. Les prédictions seront basiques.")
    
#     def train_dummy_model(self, df: pd.DataFrame, target: str = "intensity"):
#         """
#         Entraîne un modèle RandomForest simple sur les features existantes.
#         """
#         features = ['hour', 'weekday', 'is_weekend']
#         df_train = df.dropna(subset=features + [target])
#         X = df_train[features]
#         y = df_train[target]
        
#         self.scaler = StandardScaler()
#         X_scaled = self.scaler.fit_transform(X)
        
#         self.model = RandomForestRegressor(n_estimators=50, random_state=42)
#         self.model.fit(X_scaled, y)
#         print("Modèle dummy entraîné")
    
#     def predict(self, df: pd.DataFrame) -> pd.DataFrame:
#         """
#         Retourne un DataFrame avec la colonne 'prediction'.
#         Si aucun modèle, la prédiction = moyenne du compteur.
#         """
#         df = df.copy()
        
#         if self.model is None:
#             # prédiction moyenne par compteur
#             df['prediction'] = df.groupby('counter_id')['intensity'].transform('mean')
#         else:
#             features = ['hour', 'weekday', 'is_weekend']
#             X = df[features].fillna(0)
#             X_scaled = self.scaler.transform(X)
#             df['prediction'] = self.model.predict(X_scaled)
        
#         return df
    
#     def save_model(self, path: str):
#         """
#         Sauvegarde le modèle et le scaler
#         """
#         if self.model is None or self.scaler is None:
#             print("⚠ Aucun modèle à sauvegarder")
#             return
        
#         joblib.dump({"model": self.model, "scaler": self.scaler}, path)
#         print(f"Modèle sauvegardé dans {path}")

