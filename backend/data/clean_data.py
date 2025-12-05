import pandas as pd
import numpy as np

class DataCleaning:

    def __init__(self):
        self.duration = None
        self.df_clean = None
        self.df_365 = None
        self.df_200 = None
        self.df_100 = None

    def clean_data_velo(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates().copy()
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
    
    def _standardize_delete_timezone(self, df):
        """
        Traite les dates Vélo : Déjà en UTC, on retire juste la timezone.
        """
        # Conversion sécurisée
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True, errors='coerce')
        # On retire la timezone pour avoir du "UTC Naive" compatible
        df['datetime'] = df['datetime'].dt.tz_localize(None)
        return df

    def _standardize_to_UTC(self, df):
        """
        Traite les dates Météo : De l'heure locale (Paris) vers UTC Naive.
        """

        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        
        # 1. On localise en Paris (Gère l'heure d'été/hiver)
        df['datetime'] = df['datetime'].dt.tz_localize(
            'Europe/Paris', 
            ambiguous='NaT', 
            nonexistent='NaT'
        )
        # 2. On convertit en UTC
        df['datetime'] = df['datetime'].dt.tz_convert('UTC')
        # 3. On retire la timezone
        df['datetime'] = df['datetime'].dt.tz_localize(None)
        return df

        