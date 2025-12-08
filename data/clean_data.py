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

    # def _standardize_to_UTC(self, df):
    #     """
    #     Traite les dates Météo : De l'heure locale (Paris) vers UTC Naive.

    #     Le principe : On reçoit l'heure locale, on essaie de la comprendre avec Python, ça plante sur l'heure d'été, on transforme l'erreur en NaT (Not a Time) et on supprime la ligne.

    #     Avantage :
    #     Lisibilité humaine immédiate : Si vous ouvrez le fichier CSV brut, vous voyez "14:00". Vous savez que c'est 14h à Montpellier. C'est intuitif.

    #     Désavantages :
    #     Perte de données (GRAVE) : C'est le point critique. Quand on passe à l'heure d'été, l'heure "02:00" n'existe pas au cadran, mais le temps, lui, continue de s'écouler. Il y a bien eu du vent et de la pluie pendant cette heure-là.

    #     Votre code actuel supprime cette ligne. Votre modèle aura donc un "trou" dans les données météo.
    #     """

    #     df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        
    #     # 1. On localise en Paris (Gère l'heure d'été/hiver)
    #     df['datetime'] = df['datetime'].dt.tz_localize(
    #         'Europe/Paris', 
    #         ambiguous='NaT', 
    #         nonexistent='NaT'
    #     )
    #     # 2. On convertit en UTC
    #     df['datetime'] = df['datetime'].dt.tz_convert('UTC')
    #     # 3. On retire la timezone
    #     df['datetime'] = df['datetime'].dt.tz_localize(None)
    #     print(df.head())
    #     return df


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