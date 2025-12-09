import pandas as pd
import pytest

#definir le chemin
# from weather import process_weather_data 

def test_process_weather_data_logic():
    """
    Vérifie que la transformation JSON -> DataFrame se fait correctement :
    - L'index est bien temporel.
    - Les colonnes sont bien créées.
    - Les données correspondent.
    """
    
    # 1. SETUP : On fabrique un faux JSON d'entrée (minimaliste)
    # Pas besoin de mettre 1000 lignes, 2 suffisent pour tester la logique.
    fake_input = {
        "latitude": 43.61, # Sera ignoré par la fonction (c'est voulu)
        "hourly": {
            "time": ["2023-07-07T00:00", "2023-07-07T01:00"],
            "temperature_2m": [20.5, 21.0],
            "rain": [0.0, 2.5]
        }
    }

    # 2. ACTION : On exécute ta fonction
    df_result = process_weather_data(fake_input)

    # 3. ASSERTIONS : On vérifie le résultat sous toutes les coutures

    # A. Vérifier la structure de base
    assert isinstance(df_result, pd.DataFrame), "Le résultat doit être un DataFrame"
    assert df_result.shape == (2, 2), "On attend 2 lignes et 2 colonnes"
    
    # B. Vérifier l'index (Le point CRUCIAL)
    # On veut être sûr que c'est un DatetimeIndex, pas juste du texte
    assert isinstance(df_result.index, pd.DatetimeIndex), "L'index doit être de type Datetime"
    # On vérifie que la première date est bien celle attendue
    assert df_result.index[0] == pd.Timestamp("2023-07-07 00:00:00")

    # C. Vérifier les valeurs
    # On regarde si la température à 1h du matin est bien 21.0
    # .iloc[1] prend la 2ème ligne (index 1)
    assert df_result['temperature_2m'].iloc[1] == 21.0
    assert df_result['rain'].iloc[1] == 2.5

    # D. Vérifier qu'on n'a pas gardé les métadonnées inutiles (latitude)
    assert 'latitude' not in df_result.columns