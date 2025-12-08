import pandas as pd
import os
from DataLoader import DataLoader
import numpy as np


class ConvertData:
    """
    Classe utilitaire pour charger, harmoniser (UTC) et fusionner 
    les données Météo et Vélo.
    """
    
    def __init__(self, first_path: str, second_path: str):
        self.first_path = first_path
        self.second_path = second_path
        self.df_final = None # Stockera le résultat

    def _load_csv(self, path):
        """
        Charge un CSV de manière robuste :
        1. Détecte le séparateur (; ou ,)
        2. Renomme la colonne de temps (time, date...) en 'datetime'
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f" Fichier introuvable : {path}")
        
        # 1. Tentative de lecture intelligente (Sépareur , ou ;)
        try:
            # On essaie d'abord le séparateur ','
            df = pd.read_csv(path, sep=',')
            if len(df.columns) < 2: 
                # Si on a 1 seule colonne, c'est sûrement des point-virgules
                df = pd.read_csv(path, sep=';')
        except Exception as e:
            print(f" Erreur lecture CSV, tentative générique : {e}")
            df = pd.read_csv(path, engine='python') # Moteur plus lent mais plus souple

        # 2. Nettoyage des noms de colonnes (supprime les espaces ' time ' -> 'time')
        df.columns = df.columns.str.strip()
        
        print(f"  Colonnes trouvées dans {os.path.basename(path)} : {list(df.columns)}")

        # 3. Stratégie de Renommage pour avoir 'datetime'
        # Liste des suspects habituels
        candidats = ['time', 'date', 'Date', 'Time', 'Unnamed: 0', 'timestamp']
        
        colonne_trouvee = None
        
        # Si 'datetime' existe déjà, super !
        if 'datetime' in df.columns:
            colonne_trouvee = 'datetime'
        else:
            # Sinon, on cherche un candidat
            for col in candidats:
                if col in df.columns:
                    colonne_trouvee = col
                    break
        
        # 4. Application du renommage
        if colonne_trouvee and colonne_trouvee != 'datetime':
            print(f"  Renommage automatique : '{colonne_trouvee}' -> 'datetime'")
            df.rename(columns={colonne_trouvee: 'datetime'}, inplace=True)
        elif colonne_trouvee is None:
            # Cas désespéré : On prend la première colonne par défaut
            print(f"  Aucune colonne date identifiée. Utilisation de la 1ère colonne : '{df.columns[0]}'")
            df.rename(columns={df.columns[0]: 'datetime'}, inplace=True)

        # On supprime explicitement cette colonne
        if 'Unnamed: 0' in df.columns:
            df.drop(columns=['Unnamed: 0'], inplace=True)

        # drop=True : On jette l'ancien index à la poubelle.
        # drop=False : L'ancien index devient une colonne normale (utile si tu veux le garder).
        df.reset_index(drop=True, inplace=True)    

        # # La colonne 'datetime' quitte les colonnes pour devenir l'Index (en gras à gauche)
        # df.set_index('datetime', inplace=True)

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

    def process_merge_dfmeteo_with_dfvelo(self, start_date: str, end_date: str):
        """
        Exécute tout le pipeline : Chargement -> Standardisation -> Fusion -> Filtre date.
        
        Args:
            start_date (str): format 'YYYY-MM-DD'
            end_date (str): format 'YYYY-MM-DD'
            
        Returns:
            pd.DataFrame: Le dataframe fusionné et filtré.
        """
        print(f" [ConvertData] Démarrage du traitement...")
        
        # 1. Chargement
        df_meteo = self._load_csv(self.first_path)
        df_velo = self._load_csv(self.second_path)
        
        # 2. Standardisation Temporelle
        df_velo = self._standardize_delete_timezone(df_velo)
        df_meteo = self._standardize_to_UTC(df_meteo)
        
        # Suppression des lignes où la date n'a pas pu être convertie (NaT)
        df_velo.dropna(subset=['datetime'], inplace=True)
        df_meteo.dropna(subset=['datetime'], inplace=True)

        # 3. Fusion (Inner Join)
        print(" [ConvertData] Fusion des jeux de données...")
        df_merged = pd.merge(
            df_meteo,
            df_velo,
            on='datetime',
            how='inner',
            suffixes=('_meteo', '_velo')
        )
        
        # 4. Filtrage par Période
        print(f" [ConvertData] Filtrage : {start_date} -> {end_date}")
        
        # Conversion des bornes
        ts_start = pd.to_datetime(start_date)
        # Astuce : On ajoute presque 24h à la date de fin pour inclure toute la journée
        ts_end = pd.to_datetime(end_date) + pd.Timedelta(hours=23, minutes=59, seconds=59)
        
        mask = (df_merged['datetime'] >= ts_start) & (df_merged['datetime'] <= ts_end)
        self.df_final = df_merged.loc[mask].copy()

        print(f"[ConvertData] Terminé ! {len(self.df_final)} lignes prêtes.")
        return self.df_final
    
    def process_merge_uniform_dataframes(self, start_date: str, end_date: str):
            """
            Processus optimisé : Charge deux fichiers, NETTOIE les colonnes,
            EMPILE les données (Concat) et filtre par date.
            
            Args:
                start_date (str): 'YYYY-MM-DD'
                end_date (str): 'YYYY-MM-DD'
            """
            print(f" [ConvertData] Démarrage du traitement UNIFORME (Concaténation)...")
            
            # 1. Chargement
            df1 = self._load_csv(self.first_path)
            df2 = self._load_csv(self.second_path)
            
            # 2. Nettoyage Préalable (CRUCIAL pour éviter Unnamed: 0)
            # On nettoie df1 et df2 AVANT de les toucher
            for df in [df1, df2]:
                # Suppression des colonnes parasites
                cols_to_drop = [c for c in df.columns if 'Unnamed' in c]
                if cols_to_drop:
                    df.drop(columns=cols_to_drop, inplace=True)
                
                # Conversion Datetime
                # _load_csv a déjà renommé en 'datetime', on assure le typage
                df['datetime'] = pd.to_datetime(df['datetime'])
                df['datetime'] = df['datetime'].dt.tz_localize(None)

            print(f" DF1 (Tête) :\n{df1.head(2)}")
            print(f" DF2 (Tête) :\n{df2.head(2)}")

            # 3. CONCATÉNATION (Au lieu de Merge)
            # On empile df2 en dessous de df1.
            print(" [ConvertData] Empilement des données...")
            
            # axis=0 veut dire "Verticalement" (l'un sur l'autre)
            # ignore_index=True permet de refaire la numérotation des lignes proprement (0, 1, 2...)
            df_merged = pd.concat([df1, df2], axis=0, ignore_index=True)
            
            # On trie pour que 2025 soit bien après 2024
            df_merged.sort_values(by='datetime', inplace=True)

            # 4. Filtrage par Période
            print(f" [ConvertData] Filtrage : {start_date} -> {end_date}")
            
            ts_start = pd.to_datetime(start_date)
            ts_end = pd.to_datetime(end_date) + pd.Timedelta(hours=23, minutes=59, seconds=59)
            
            mask = (df_merged['datetime'] >= ts_start) & (df_merged['datetime'] <= ts_end)
            self.df_final = df_merged.loc[mask].copy()

            # 5. Bilan
            if self.df_final.empty:
                print(" ATTENTION : Le DataFrame résultat est VIDE.")
            else:
                print(f" [ConvertData] Terminé ! {len(self.df_final)} lignes prêtes.")
                print("  Colonnes finales :", list(self.df_final.columns))
                
            return self.df_final



    def get_holiday_boolean_mask(self, df_feries, start_date, end_date, freq='h'):
        """
        Génère une série temporelle continue sur une période donnée,
        avec une colonne binaire indiquant si l'heure tombe un jour férié.
        
        Args:
            df_feries (pd.DataFrame): Le DF contenant les jours fériés (colonnes 'datetime', 'nom_ferie')
            start_date (str): 'YYYY-MM-DD'
            end_date (str): 'YYYY-MM-DD'
            freq (str): Fréquence des données ('h' pour horaire, 'D' pour journalier)
        
        Returns:
            pd.DataFrame: Un DF indexé par le temps avec une colonne 'is_holiday' (0 ou 1) 
                        et 'nom_ferie' (le nom ou NaN).
        """
        print(f" Génération de la timeline fériée ({start_date} au {end_date})...")

        # 1. Création de la Chronologie Complète (La Toile)
        # On génère toutes les heures entre le début et la fin
        ts_start = pd.to_datetime(start_date)
        # On ajoute 23h59 à la fin pour être sûr d'inclure toute la dernière journée
        ts_end = pd.to_datetime(end_date) + pd.Timedelta(hours=23, minutes=59)
        
        full_range = pd.date_range(start=ts_start, end=ts_end, freq=freq, name='datetime')
        df_timeline = pd.DataFrame(index=full_range)
        
        # 2. Préparation des clés de jointure (Normalisation)
        # Pour comparer, on a besoin juste de la date (YYYY-MM-DD) sans l'heure
        # On crée une colonne temporaire 'date_only' dans notre timeline
        df_timeline['date_only'] = df_timeline.index.normalize()

        # On s'assure que le DF fériés est aussi au format datetime propre
        df_feries = df_feries.copy() # On travaille sur une copie pour ne pas casser l'original
        df_feries['datetime'] = pd.to_datetime(df_feries['datetime'])
        df_feries['date_only'] = df_feries['datetime'].dt.normalize()

        # 3. La Fusion (Le Mapping)
        # On colle les infos fériées sur notre timeline via la date commune
        # how='left' signifie : "Garde toutes les heures de la timeline, et mets des trous si pas de férié"
        df_merged = pd.merge(
            df_timeline,
            df_feries[['date_only', 'nom_ferie']], # On ne prend que ce qui nous intéresse
            on='date_only',
            how='left'
        )
        
        # 4. Création du Booléen (0 ou 1)
        # Si 'nom_ferie' n'est pas vide (notna), alors c'est 1, sinon 0.
        df_merged['jour_ferie'] = df_merged['nom_ferie'].notna().astype(int)
        
        # 5. Nettoyage final
        # On remet l'index temporel (perdu pendant le merge)
        df_merged.index = df_timeline.index
        
        # On supprime la colonne temporaire
        df_merged.drop(columns=['date_only'], inplace=True)
        
        # On remplit les noms manquants par "Non Férié" ou vide (optionnel)
        df_merged['nom_ferie'] = df_merged['nom_ferie'].fillna("Non Férié")

        print(f" Timeline générée : {len(df_merged)} lignes.")
        return df_merged



# 1. Initialisation de la classe avec les chemins de fichiers
converter = ConvertData(first_path='data_files/velo_data.csv', second_path='data_files/meteo_mtp.csv')

# 2. Lancement du traitement avec les dates (tu peux mettre tes widgets ici)
df_resultat = converter.process_merge_uniform_dataframes(start_date="2024-11-30", end_date="2025-12-01")

# 3. Vérification
print(df_resultat)

loader = DataLoader()
# loader.export_data(df_resultat, "data/jour_feries_2024_2025.csv")


# --- TEST AVEC TES DONNÉES ---

# Supposons que tu as déjà ton df_feries_complet (celui créé à l'étape précédente)
df_input = df_resultat 

# # # Lancement
# df_resultat_bool = converter.get_holiday_boolean_mask(
#     df_input, 
#     start_date="2024-12-24", 
#     end_date="2025-12-01",
#     freq='h' # 'h' pour avoir une ligne par heure (comme ton exemple 09:00:00)
# )
# print(df_resultat_bool.head(24)) # Affiche les 24 premières heures

# loader.export_data(df_resultat_bool, "data/jour_feries_to_bool.csv")
