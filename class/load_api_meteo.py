class SolarDataLoader():
    def __init__(self, csv_path: str, latitude: float, longitude: float):
        self.csv_path = csv_path
        self.latitude = latitude
        self.longitude = longitude
        self.production_df = None
        self.meteo_df = None

    def load_production(self, start_date="2022-07-07", end_date=" 2025-02-23"):
        """Charge le CSV de production solaire et filtre sur la période souhaitée"""
        self.production_df = pd.read_csv(self.csv_path, parse_dates=['date'])
        # Filtrer les dates
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        self.production_df = self.production_df[
            (self.production_df['date'] >= start_dt) & 
            (self.production_df['date'] <= end_dt)
        ]
        print(f"Production solaire chargée et filtrée : {len(self.production_df)} lignes")
        return self.production_df

    def load_meteo(self, start_date="2022-07-07", end_date="2025-02-23"):
        """Charge les données météo historiques depuis Open-Meteo ERA5
        pour la période 2022-07-07 → 2025-08-01"""
        url = (
            f"https://archive-api.open-meteo.com/v1/era5?"
            f"latitude={self.latitude}&longitude={self.longitude}"
            f"&start_date={start_date}&end_date={end_date}"
            f"&daily=temperature_2m_max,temperature_2m_min,shortwave_radiation_sum"
            f"&timezone=Europe/Paris"
        )

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Erreur API : {response.status_code}")
            return None

        data = response.json()
        if "daily" not in data:
            print("Erreur : clé 'daily' manquante dans le JSON")
            return None

        # Sauvegarde du JSON dans un fichier
        with open("meteo_montpellier.json", "w") as f:
            json.dump(data, f, indent=4)

        self.meteo_df = pd.DataFrame(data["daily"])
        self.meteo_df['time'] = pd.to_datetime(self.meteo_df['time'])

        # Filtrage pour s'assurer que les dates correspondent exactement à la période
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        self.meteo_df = self.meteo_df[
            (self.meteo_df['time'] >= start_dt) &
            (self.meteo_df['time'] <= end_dt)
        ]

        print(f"Météo solaire chargée : {len(self.meteo_df)} lignes")
        return self.meteo_df
