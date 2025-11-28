import requests
import json
from datetime import datetime

# --- Configuration ---
# L'URL de base du portail API Fiware de Montpellier
BASE_URL = "https://portail-api-data.montpellier3m.fr"

# L'identifiant complet de l'entité (URN) :
ECOCOUNTER_URN = "urn:ngsi-ld:EcoCounter:X2H22104766"

# Définition de l'endpoint générique
ENDPOINT_TEMPLATE = "/ecocounter_timeseries/{ecocounterId}/attrs/intensity"

# 1. PARAMÈTRES DE REQUÊTE : Début et Fin de l'intervalle

# Date de début (ex: 1er janvier 2025)
FROM_DATE_STR = "2025-09-01T00:00:00"

# Date de fin (ex: Hier, 24 novembre 2025 à minuit, car nous sommes le 25 novembre)
# C'est une bonne pratique de s'arrêter à la veille pour les données Open Data.
TO_DATE_STR = "2025-11-22T23:59:59" 

# Clé API : À remplacer si nécessaire.
# API_KEY = "VOTRE_CLE_API_ICI"


def get_traffic_in_range(eco_urn: str, from_date: str, to_date: str):
    """
    Récupère l'historique du trafic vélo pour un compteur dans l'intervalle de temps défini
    par 'fromDate' et 'toDate'.
    """
    
    # Construction de l'URL complète
    url = BASE_URL + ENDPOINT_TEMPLATE.format(ecocounterId=eco_urn)
    
    # Paramètres de la requête GET
    params = {
        "fromDate": from_date, 
        "toDate": to_date, 
        "limit": 1000 # Limite de résultats
    }
    
    # Headers de la requête
    headers = {
        "Accept": "application/json",
        # Décommentez si besoin : "Authorization": f"Bearer {API_KEY}" 
    }
    
    try:
        print(f"Tentative de connexion à l'URL : {url}")
        print(f"Période demandée : du {from_date} au {to_date}")
        
        response = requests.get(url, headers=headers, params=params)

        response.raise_for_status() 
        
        data = response.json()
       
        print("\n Requête réussie ! Aperçu des données :")
        print(json.dumps(data))
        return data
    except requests.exceptions.HTTPError as err:
        print(f"\n Erreur HTTP ({response.status_code}): {err}")
        print("Erreur : Problème d'authentification ou ID de compteur invalide.")
    except requests.exceptions.RequestException as e:
        print(f"\n Erreur de connexion: {e}")

if __name__ == "__main__":
    # Exécution de la fonction avec la plage définie
    velo_data = get_traffic_in_range(ECOCOUNTER_URN, FROM_DATE_STR, TO_DATE_STR)
    velo_data