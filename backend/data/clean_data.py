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
            Traite les dates Météo : On s'assure juste que c'est au format date.
            Comme l'API envoie déjà du UTC, on a rien d'autre à faire !
            """
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
            
            # On retire le fuseau horaire si Pandas l'a ajouté automatiquement (pour avoir du "naive")
            if df['datetime'].dt.tz is not None:
                df['datetime'] = df['datetime'].dt.tz_localize(None)
                
            return df