from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, Float, Boolean, NullPool, create_engine, text
import pandas as pd

class Database:

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, database_url: str):
        if getattr(self, "_initialized", False):
            return

        self._initialized = True

        self.metadata = MetaData()
        self.engine = create_engine(database_url, poolclass=NullPool)

        self.velo_raw = None
        self.velo_clean = None
        self.meteo_raw = None
        self.meteo_clean = None
        self.model_data = None

    def create_tables(self):

        self.counters = Table(
            "counters",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("counter_id", String, nullable=False),
            Column("lat", Float, nullable=True),
            Column("lon", Float, nullable=True),
        )

        self.velo_clean = Table(
            "velo_clean",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("counter_id", String, nullable=False),
            Column("intensity", Float, nullable=False),
            Column("lat", Float, nullable=True),
            Column("lon", Float, nullable=True),
            Column("weekday", Integer, nullable=True),
            Column("is_weekend", Boolean, nullable=True),
            Column("hour", Integer, nullable=True),
        )

        self.velo_raw = Table(
            "velo_raw",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("counter_id", String, nullable=False),
            Column("intensity", Float, nullable=False),
            Column("lat", Float, nullable=True),
            Column("lon", Float, nullable=True),
            Column("laneId", Integer, nullable=True),
            Column("vehicleType", String, nullable=True),
        )

        self.meteo_raw = Table(
            "meteo_raw",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("temperature_2m_max", Float, nullable=True),
            Column("temperature_2m_min", Float, nullable=True),
            Column("shortwave_radiation_sum", Float, nullable=True),
        )

        self.meteo_clean = Table(
            "meteo_clean",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("temperature_2m_max", Float, nullable=True),
            Column("temperature_2m_min", Float, nullable=True),
            Column("shortwave_radiation_sum", Float, nullable=True),
        )

        self.model_data = Table(
            "model_data",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("predicted_values", Float, nullable=False),
        )

        self.metadata.create_all(self.engine)

        with self.engine.begin() as conn:
            for table in [
                self.velo_raw,
                self.velo_clean,
                self.meteo_raw,
                self.meteo_clean,
                self.model_data,
            ]:
                conn.execute(
                    text(
                        f'ALTER TABLE "public"."{table.name}" ENABLE ROW LEVEL SECURITY;'
                    )
                )

    def drop_tables(self, name: str = None):
        if name is None:
            self.metadata.reflect(self.engine)
            self.metadata.drop_all(self.engine)
        else:
            table = Table(name, self.metadata, autoload_with=self.engine)
            table.drop(self.engine, checkfirst=True)

    
    def push_data(self, df: pd.DataFrame, table_name: str):
        self.metadata.reflect(self.engine)
        table = self.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"La table '{table_name}' n'existe pas sur Database")

        with self.engine.begin() as conn:
            conn.execute(
                table.insert(),
                df.to_dict(orient="records")
            )
    
    def pull_data(self, table_name: str) -> pd.DataFrame:
        self.metadata.reflect(self.engine)
        table = self.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"La table '{table_name}' n'existe pas sur Database")

        with self.engine.begin() as conn:
            result = conn.execute(table.select())
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        return df

           



    

