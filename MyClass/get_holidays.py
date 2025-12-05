import pandas as pd
import holidays
import requests
import io

def get_vacances_scolaires(target_zone='Zone C'):
    """
    Récupère les vacances scolaires depuis data.gouv.fr
    target_zone : 'Zone A', 'Zone B' ou 'Zone C' (ex: Paris = Zone C)
    """
    url = "https://www.data.gouv.fr/fr/datasets/r/71a4401d-91df-4234-9343-456076412571"
    
    try:
        # Téléchargement direct du CSV
        s = requests.get(url).content
        df_vac = pd.read_csv(io.StringIO(s.decode('utf-8')), sep=";")
        
        # Filtrage sur la zone et les dates
        # Le fichier contient les dates de début et de fin
        df_vac = df_vac[df_vac['zones'] == target_zone]
        
        # Conversion dates
        df_vac['start_date'] = pd.to_datetime(df_vac['start_date'])
        df_vac['end_date'] = pd.to_datetime(df_vac['end_date'])
        
        return df_vac[['start_date', 'end_date', 'nom_vacances']]
        
    except Exception as e:
        print(f"Erreur récup vacances: {e}")
        return None

def is_in_vacances(date, df_vacances):
    """Vérifie si une date tombe dans une plage de vacances"""
    if df_vacances is None:
        return 0
    # On vérifie si la date est comprise entre start et end d'une ligne
    # C'est un peu lent ligne à ligne, on optimisera si besoin, mais ok pour 1 an
    is_vac = df_vacances.apply(lambda row: row['start_date'] <= date <= row['end_date'], axis=1).any()
    return 1 if is_vac else 0

# --- INTÉGRATION DANS LA PIPELINE ---

def pipeline_feature_engineering_complete(df_input, zone_scolaire='Zone C'):
    df = df_input.copy()
    df['ds'] = pd.to_datetime(df['ds'])
    
    # 1. Jours Fériés (Via Package - Rapide & Fiable)
    fr_holidays = holidays.France()
    # Astuce : On inclut aussi l'Alsace si tes compteurs sont à Strasbourg
    # fr_holidays = holidays.France(subdiv='Alsace-Moselle') 
    df['is_holiday'] = df['ds'].apply(lambda x: 1 if x in fr_holidays else 0)

    # 2. Vacances Scolaires (Via Data Gouv)
    print("Récupération des vacances scolaires...")
    df_vacs = get_vacances_scolaires(target_zone=zone_scolaire)
    
    # Création d'un Range Index pour aller plus vite que apply
    # On crée un set de tous les jours de vacances
    vacation_days = set()
    if df_vacs is not None:
        for _, row in df_vacs.iterrows():
            dates = pd.date_range(row['start_date'], row['end_date'])
            vacation_days.update(dates.date) # On stocke juste la date (pas l'heure)

    df['is_school_holiday'] = df['ds'].apply(lambda x: 1 if x.date() in vacation_days else 0)

    # ... Le reste de tes features (Lags, Sin/Cos...) ...
    
    return df