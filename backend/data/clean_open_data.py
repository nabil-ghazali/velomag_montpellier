import pandas as pd
import numpy as np

class VeloPipeline:
    def __init__(self, df_velo: pd.DataFrame):
        self.df_velo = df_velo.copy()
        self.duration = None
        self.df_clean = None
        self.df_365 = None
        self.df_200 = None
        self.df_100 = None

    # ---------------------------------------------------------
    # 1️ Nettoyage des données
    # ---------------------------------------------------------
    def clean_data(self):
        df = self.df_velo.drop_duplicates().copy()
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['intensity'] = pd.to_numeric(df['intensity'], errors='coerce')
        df['hour'] = df['datetime'].dt.hour
        df['weekday'] = df['datetime'].dt.weekday
        df['is_weekend'] = df['weekday'].isin([5, 6]).astype(int)

        df['intensity'] = df.groupby('counter_id')['intensity'].transform(
            lambda x: np.where(x > 300, np.nanmedian(x), x)
        )
        self.df_clean = df
        return df

    # ---------------------------------------------------------
    # 2️ Catégorisation des compteurs
    # ---------------------------------------------------------
    def categorize_counters(self):
        duration = self.df_clean.groupby("counter_id")["datetime"].agg(["min","max"])
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
        self.duration = duration
        return duration

    # ---------------------------------------------------------
    # 3️ Feature engineering
    # ---------------------------------------------------------
    def add_features(self, df: pd.DataFrame):
        df = df.copy()  # force une copie indépendante pour éviter le warning
        df['rolling_3h'] = df.groupby('counter_id')['intensity'].transform(
            lambda x: x.rolling(3, min_periods=1).mean()
        )
        df['lag_1h'] = df.groupby('counter_id')['intensity'].shift(1)
        df['lag_24h'] = df.groupby('counter_id')['intensity'].shift(24)
        df['pct_change_1h'] = df.groupby('counter_id')['intensity'].pct_change()
        return df


    # ---------------------------------------------------------
    # 6️ Pipeline complet
    # ---------------------------------------------------------
    def run_pipeline(self):
        self.clean_data()
        self.categorize_counters()

        # Filtrage compteurs par durée
        self.df_365 = self.df_clean[self.df_clean['counter_id'].isin(
            self.duration[self.duration['category'] == "plus de 365 jours"].index
        )]
        self.df_200 = self.df_clean[self.df_clean['counter_id'].isin(
            self.duration[self.duration['category'] == "200 jours ou plus"].index
        )]
        self.df_100 = self.df_clean[self.df_clean['counter_id'].isin(
            self.duration[self.duration['category'] == "moins de 100 jours"].index
        )]

        # Feature engineering
        self.df_clean = self.add_features(self.df_clean)
        self.df_365 = self.add_features(self.df_365)
        self.df_200 = self.add_features(self.df_200)
        self.df_100 = self.add_features(self.df_100)

        return {
            'df_clean': self.df_clean,
            'df_365': self.df_365,
            'df_200': self.df_200,
            'df_100': self.df_100,
            'duration': self.duration
        }