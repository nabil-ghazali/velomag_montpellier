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