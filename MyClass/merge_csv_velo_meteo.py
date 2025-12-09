import pandas as pd

def merge_velo_meteo(path_velo, path_meteo):
    # ==============================================================================
    # 1. TRAITEMENT VÉLO (Format: 2025-03-10 06:00:00+00:00)
    # ==============================================================================
    # Lecture avec le séparateur point-virgule ';'
    df_velo = pd.read_csv(path_velo, sep=';')
    
    # Conversion en objet datetime
    # 'utc=True' force pandas à bien comprendre le '+00:00'
    df_velo['datetime'] = pd.to_datetime(df_velo['datetime'], utc=True)
    
    # --- CORRECTION DU FORMAT DE DATE ICI ---
    # On supprime l'information de fuseau horaire (+00:00) pour avoir une date "naïve"
    # Cela transforme "2025-03-10 06:00:00+00:00" en "2025-03-10 06:00:00"
    df_velo['datetime'] = df_velo['datetime'].dt.tz_localize(None)
    
    # Agrégation (Nettoyage des doublons éventuels par heure/compteur)
    df_velo_clean = df_velo.groupby(['datetime', 'counter_id']).agg({
        'intensity': 'sum',      
        'lat': 'first',          
        'lon': 'first'
    }).reset_index()

    # ==============================================================================
    # 2. TRAITEMENT MÉTÉO (Format: 2024-12-01 15:00:00)
    # ==============================================================================
    # Lecture avec séparateur ';' et décimale ','
    df_meteo = pd.read_csv(path_meteo, sep=';', decimal=',')
    
    # Conversion simple (c'est déjà du format naïf, pas de timezone)
    df_meteo['datetime'] = pd.to_datetime(df_meteo['datetime'])
    
    # ==============================================================================
    # 3. FUSION
    # ==============================================================================
    # Maintenant que les deux colonnes 'datetime' sont au format "naïf", le merge fonctionne
    df_merged = pd.merge(
        df_velo_clean, 
        df_meteo, 
        on='datetime', 
        how='left'
    )
    
    # Remplissage des données météo manquantes (propagation de la dernière valeur)
    cols_meteo = ['temperature_2m', 'wind_speed_10m', 'precipitation']
    df_merged[cols_meteo] = df_merged[cols_meteo].ffill().fillna(0)

    return df_merged

# --- EXECUTION ---
if __name__ == "__main__":
    try:
        # Remplace par tes chemins réels
        df_final = merge_velo_meteo('data/velo_data.csv', 'data/meteo_mtp.csv')
        
        print("✅ Fusion réussie !")
        print(df_final[['datetime', 'intensity', 'temperature_2m']].head())
        
        # Sauvegarde
        df_final.to_csv('data/merged_data.csv', index=False, sep=';')
        
    except Exception as e:
        print(f"❌ Erreur : {e}")