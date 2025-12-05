import pandas as pd
import numpy as np
import holidays  # <--- La nouvelle star
import os

def run_feature_engineering(df_merged):
    """
    Transforme les données brutes (Vélo + Météo) en dataset riche.
    Génère les jours fériés dynamiquement via le package 'holidays'.
    
    Args:
        df_merged: DataFrame contenant [datetime, intensity, counter_id, ...]
    """
    print(" Démarrage du Feature Engineering (avec package Holidays)...")
    
    # 1. Copie de sécurité
    df = df_merged.copy()
    
    # Conversion et Tri
    if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
        df['datetime'] = pd.to_datetime(df['datetime'])
        
    df = df.sort_values(by=['counter_id', 'datetime'])

    # ---------------------------------------------------------
    # ÉTAPE 1 : GÉNÉRATION DES JOURS FÉRIÉS (Package Python)
    # ---------------------------------------------------------
    print("    Calcul des jours fériés via library 'holidays'...")
    
    # 1. On identifie les années présentes dans tes données pour optimiser
    unique_years = df['datetime'].dt.year.unique()
    
    # 2. On charge le calendrier français pour ces années
    # subdivision='FR-34' est optionnel (Hérault), mais 'France' suffit généralement
    fr_holidays = holidays.France(years=unique_years)
    
    # 3. Création de la colonne (Méthode optimisée)
    # On extrait la date (sans l'heure) et on vérifie si elle est dans la liste
    # .dt.date convertit le timestamp en objet date compatible avec la lib holidays
    df['is_holiday'] = df['datetime'].dt.date.apply(lambda x: 1 if x in fr_holidays else 0)

    # ---------------------------------------------------------
    # ÉTAPE 2 : VARIABLES TEMPORELLES
    # ---------------------------------------------------------
    df['hour'] = df['datetime'].dt.hour
    df['day_of_week'] = df['datetime'].dt.dayofweek
    df['month'] = df['datetime'].dt.month
    df['year'] = df['datetime'].dt.year
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # ---------------------------------------------------------
    # ÉTAPE 3 : ENCODAGE CYCLIQUE
    # ---------------------------------------------------------
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

    # ---------------------------------------------------------
    # ÉTAPE 4 : LAGS (J+1 Safe)
    # ---------------------------------------------------------
    # GroupBy obligatoire pour ne pas mélanger les compteurs
    grouper = df.groupby('counter_id')['intensity']
    
    df['lag_24h'] = grouper.shift(24)
    df['lag_48h'] = grouper.shift(48)
    df['lag_1week'] = grouper.shift(24 * 7)
    
    # Moyenne glissante décalée (4 jours passés)
    df['rolling_mean_4d'] = grouper.shift(24).rolling(window=4).mean()

    # ---------------------------------------------------------
    # ÉTAPE 5 : ENCODAGE COMPTEUR & NETTOYAGE
    # ---------------------------------------------------------
    df['counter_id_encoded'] = df['counter_id'].astype('category').cat.codes
    
    # Suppression des NaN (dus aux lags du début d'historique)
    initial_len = len(df)
    df = df.dropna()
    final_len = len(df)
    
    print(f" Lignes supprimées (Lags initialisation) : {initial_len - final_len}")
    print(f" Feature Engineering terminé. Dimensions finales : {df.shape}")
    
    return df

# =========================================================
# BLOC DE TEST
# =========================================================
if __name__ == "__main__":
    print("\n --- DÉBUT DU TEST UNITAIRE ---")
    
    # Chemin vers ton fichier fusionné (Vélo + Météo uniquement)
    PATH_DATA = "data/merged_data.csv" 
    # Note : On n'a plus besoin de PATH_FERIES !

    try:
        if os.path.exists(PATH_DATA):
            print(f"1. Chargement de {PATH_DATA}...")
            df_test_merged = pd.read_csv(PATH_DATA, sep=';')
            
            # 2. Exécution de la fonction (Un seul argument maintenant !)
            print("2. Exécution de la pipeline...")
            df_result = run_feature_engineering(df_test_merged)
            
            # 3. Vérifications
            print("\n --- RÉSULTATS DU TEST ---")
            
            # Vérifions Noël (si présent dans les données)
            print("Test sur le 25 Décembre :")
            noel = df_result[df_result['datetime'].astype(str).str.contains('-12-25')]
            if not noel.empty:
                print(noel[['datetime', 'is_holiday']].head(1))
                if noel['is_holiday'].iloc[0] == 1:
                    print(" SUCCÈS : Noël est bien détecté comme férié.")
                else:
                    print(" ERREUR : Noël n'est pas détecté.")
            else:
                print(" Pas de données pour Noël dans ce fichier.")

            # Sauvegarde
            out_path = "data/processed/train_data_xgboost.csv"
            df_result.to_csv(out_path, index=False, sep=';')
            print(f"\n💾 Fichier prêt pour l'entraînement : {out_path}")
            
        else:
            print(f" Fichier {PATH_DATA} introuvable pour le test.")

    except Exception as e:
        print(f"\n ERREUR PENDANT LE TEST : {e}")
        import traceback
        traceback.print_exc()