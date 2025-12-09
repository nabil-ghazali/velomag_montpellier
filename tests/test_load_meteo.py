import pytest
from unittest.mock import patch, Mock
import requests
#définir le chemin
# from weather import load_meteo chemin de la fonction

# --- TEST 1 : Quand tout se passe bien ---
def test_load_meteo_success():
    """
    Simule une réponse API parfaite (Status 200 + JSON valide).
    """
    
    # 1. Préparation des fausses données (ce qu'on attend de l'API)
    fake_json_response = {
        "hourly": {
            "time": ["2023-07-07T00:00"],
            "temperature_2m": [21.2]
        }
    }

    # On remplace 'requests.get' par notre doublure 'mock_get'
    with patch('requests.get') as mock_get:
        
        # On configure la doublure :
        # "Si on t'appelle, renvoie un objet qui a un status_code 200..."
        mock_get.return_value.status_code = 200
        # "...et dont la méthode .json() renvoie nos fausses données."
        mock_get.return_value.json.return_value = fake_json_response

        # 3. Action : On lance la vraie fonction
        result = load_meteo(start_date="2023-01-01", end_date="2023-01-02")

        # 4. Assertions (Vérifications)
        
        # Vérifie qu'on a bien reçu les données
        assert result == fake_json_response
        
        # Vérifie que requests.get a bien été appelé (que la fonction a tenté de joindre le web)
        mock_get.assert_called_once()
        
        # Vérifie que l'URL générée contenait bien nos dates
        # (args[0] contient l'URL passée à get)
        args, _ = mock_get.call_args
        assert "start_date=2023-01-01" in args[0]
        assert "end_date=2023-01-02" in args[0]

# --- TEST 2 : Quand l'API plante ---
def test_load_meteo_failure(capsys):
    """
    Simule une erreur API (ex: 404 Not Found).
    'capsys' est un outil pytest pour capturer les print()
    """
    
    with patch('requests.get') as mock_get:
        # Configuration du scénario catastrophe
        mock_get.return_value.status_code = 404
        
        # Action
        result = load_meteo()
        
        # Assertion 1 : La fonction doit retourner None selon ton code actuel
        assert result is None
        
        # Assertion 2 : Vérifier qu'un message d'erreur a été imprimé
        # On capture ce qui a été écrit dans la console
        captured = capsys.readouterr()
        assert "Erreur API : 404" in captured.out