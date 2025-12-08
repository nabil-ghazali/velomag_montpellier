# backend/app/models/velo_model.py
#les modèles ML + toute logique de prédiction.
from pathlib import Path
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import joblib

class VeloModelTrainer:
    """
    Entraîne les modèles ML à partir du fichier déjà préparé par :
    - fetch_velo_api.py
    - velo_service.py  (lag 24/48/168 + sin/cos + rolling + météo)

    RQ: ce fichier NE recrée pas les features.
    """

    def __init__(self, data_path="../data/processed/velo_final.csv", model_dir="../models"):
        self.data_path = Path(data_path)
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------
    # 1. Charger les données préparées
    # ----------------------------------------------------------
    # def load_data(self):
    #     df = pd.read_csv(self.data_path, parse_dates=["datetime"])
    #     df = df.sort_values(["counter_id", "datetime"]).reset_index(drop=True)
    #     return df
    def load_data(self):
        df = pd.read_csv(self.data_path, parse_dates=["datetime"])
        df = df.sort_values(["counter_id", "datetime"]).reset_index(drop=True)

        # Conversion des colonnes numériques en float
        numeric_cols = ["temperature_2m", "wind_speed_10m", "precipitation",
                        "lag_24h", "lag_48h", "lag_168h", "mean_last_4_days"]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(",", ".").astype(float)

        return df
    # ----------------------------------------------------------
    # 2. Filtrer compteurs fiables (≥ 365j)
    # ----------------------------------------------------------
    
    # def filter_reliable_counters(self, df):
    #     duration = df.groupby("counter_id")["datetime"].agg(["min", "max"])
    #     duration["days"] = (duration["max"] - duration["min"]).dt.days

    #     start_date = pd.Timestamp("2024-12-01")
    #     end_date = pd.Timestamp("2025-12-01")

    #     reliable = duration[
    #         (duration["days"] >= 365)
    #         & (duration["min"] <= start_date)
    #         & (duration["max"] >= end_date)
    #     ].index.tolist()

    #     return df[df["counter_id"].isin(reliable)].copy()

    # ----------------------------------------------------------
    # 3. Encodage compteur
    # ----------------------------------------------------------
    def encode_counter(self, df):
        le = LabelEncoder()
        df["counter_id_encoded"] = le.fit_transform(df["counter_id"])
        self.label_encoder = le
        return df

    # ----------------------------------------------------------
    # 4. Split temporel
    # ----------------------------------------------------------
    def split_data(self, df):

        # Toutes les features déjà présente au pipeline velo_service  
        features = [
            "temperature_2m", "wind_speed_10m", "precipitation",
            "hour", "day_of_week", "month", "year", "is_weekend",
            "hour_sin", "hour_cos", "dow_sin", "dow_cos",
            "month_sin", "month_cos",
            "lag_24h", "lag_48h", "lag_168h",
            "mean_last_4_days", "counter_id_encoded",
        ]

        target = "intensity"

        X = df[features]
        y = df[target]

        split = int(len(df) * 0.8)
        return X[:split], X[split:], y[:split], y[split:]

    # ----------------------------------------------------------
    # 5. Entraînement modèles
    # ----------------------------------------------------------
    def train_models(self, X_train, y_train):
        rf = RandomForestRegressor(n_estimators=200, max_depth=15, random_state=42)
        xgb = XGBRegressor(n_estimators=200, max_depth=6, learning_rate=0.1, random_state=42)
        cb = CatBoostRegressor(iterations=500, depth=6, learning_rate=0.1, verbose=0)

        rf.fit(X_train, y_train)
        xgb.fit(X_train, y_train)
        cb.fit(X_train, y_train)

        return {"RandomForest": rf, "XGBoost": xgb, "CatBoost": cb}

    # ----------------------------------------------------------
    # 6. Évaluation
    # ----------------------------------------------------------
    def evaluate(self, models, X_test, y_test):
        for name, model in models.items():
            y_pred = model.predict(X_test)
            print(f"\n--- {name} ---")
            print(f"R²   : {r2_score(y_test, y_pred):.4f}")
            print(f"MAE  : {mean_absolute_error(y_test, y_pred):.2f}")
            print(f"RMSE : {mean_squared_error(y_test, y_pred)**0.5:.2f}")

    # ----------------------------------------------------------
    # 7. Sauvegarde
    # ----------------------------------------------------------
    def save(self, models):
        for name, model in models.items():
            joblib.dump(model, self.model_dir / f"{name.lower()}_model.pkl")

        joblib.dump(self.label_encoder, self.model_dir / "labelencoder_counter.pkl")

        print("\nModèles + LabelEncoder sauvegardés ✔")

    # ----------------------------------------------------------
    # PIPELINE COMPLET
    # ----------------------------------------------------------
    def run(self):
        print("Chargement…")
        df = self.load_data()

        # print("Filtrage compteurs fiables…")
        # df = self.filter_reliable_counters(df)

        print("Encodage compteur…")
        df = self.encode_counter(df)

        print("Split…")
        X_train, X_test, y_train, y_test = self.split_data(df)

        print("Entraînement…")
        models = self.train_models(X_train, y_train)

        print("Évaluation…")
        self.evaluate(models, X_test, y_test)

        print("Sauvegarde…")
        self.save(models)

        return models

# bloc teste : 

if __name__ == "__main__":
    trainer = VeloModelTrainer(data_path="../../data/processed/velo_data.csv",
                            model_dir="../../models")
    models = trainer.run()
